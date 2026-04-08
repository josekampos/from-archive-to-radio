#!/usr/bin/env python3
import os
import re
import time
from pathlib import Path
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

IA_METADATA = "https://archive.org/metadata/{identifier}"
IA_ADVANCEDSEARCH = "https://archive.org/advancedsearch.php"
IA_DOWNLOAD = "https://archive.org/download/{identifier}/{filename}"

RADIO_DIR = Path(os.getenv("RADIO_DIR", "/opt/archive-radio/radio/"))
CACHE_DIR = Path(os.getenv("CACHE_DIR", "/opt/archive-radio/cache/"))
PLAYLIST_PATH = Path(os.getenv("PLAYLIST_PATH", str(RADIO_DIR / "playlist.m3u")))
IDENTIFIERS_FILE = Path(os.getenv("IDENTIFIERS_FILE", str(RADIO_DIR / "identifiers.txt")))

ARCHIVE_USER = os.getenv("ARCHIVE_USER", "").strip()
ARCHIVE_QUERY = os.getenv("ARCHIVE_QUERY", "").strip()
ARCHIVE_ROWS = int(os.getenv("ARCHIVE_ROWS", "200"))
ARCHIVE_PAGES = int(os.getenv("ARCHIVE_PAGES", "5"))

AUDIO_EXTS = [x.strip().lower() for x in os.getenv("AUDIO_EXTS", "mp3,ogg,flac").split(",") if x.strip()]
CACHE_TARGET_COUNT = int(os.getenv("CACHE_TARGET_COUNT", "200"))
CACHE_MAX_GB = float(os.getenv("CACHE_MAX_GB", "10"))
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "60"))

# Session with automatic retries for transient server/network errors
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "archive-radio-template/1.0 (+https://archive.org)"})
_retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 502, 503, 504], allowed_methods=["GET"])
_adapter = HTTPAdapter(max_retries=_retry)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)


def log(msg: str) -> None:
    print(f"[fetcher] {msg}", flush=True)


def sanitize_title(s: str) -> str:
    s = s.strip().replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    return s[:200]


def build_query() -> str:
    if ARCHIVE_QUERY:
        return ARCHIVE_QUERY
    if not ARCHIVE_USER:
        return "identifier:__nonexistent__"
    return f'(creator:("{ARCHIVE_USER}") OR uploader:("{ARCHIVE_USER}")) AND mediatype:(audio)'


def advanced_search_identifiers(query: str) -> list[str]:
    out: list[str] = []
    for page in range(1, ARCHIVE_PAGES + 1):
        params = {
            "q": query,
            "fl[]": "identifier",
            "rows": str(ARCHIVE_ROWS),
            "page": str(page),
            "output": "json",
        }
        r = SESSION.get(IA_ADVANCEDSEARCH, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        docs = j.get("response", {}).get("docs", [])
        ids = [d.get("identifier") for d in docs if d.get("identifier")]
        out.extend(ids)
        if len(ids) < ARCHIVE_ROWS:
            break
    seen: set[str] = set()
    dedup = []
    for i in out:
        if i not in seen:
            seen.add(i)
            dedup.append(i)
    return dedup


def identifiers_from_file() -> list[str]:
    if not IDENTIFIERS_FILE.exists():
        return []
    ids = []
    for line in IDENTIFIERS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        ids.append(line)
    return ids


def pick_audio_file(files: list[dict]) -> dict | None:
    def score(f: dict) -> tuple:
        name = (f.get("name") or "").lower()
        ext = name.rsplit(".", 1)[-1] if "." in name else ""
        ext_rank = AUDIO_EXTS.index(ext) if ext in AUDIO_EXTS else 999
        src = (f.get("source") or "").lower()
        src_rank = 0 if src == "original" else 1
        bad = 1 if (name.startswith("_") or name.endswith(".torrent") or name.endswith("_meta.xml") or name.endswith("_files.xml")) else 0
        return (bad, ext_rank, src_rank, len(name))
    candidates = [f for f in files if isinstance(f, dict) and f.get("name")]
    candidates = [f for f in candidates if (f["name"].lower().rsplit(".", 1)[-1] if "." in f["name"] else "") in AUDIO_EXTS]
    if not candidates:
        return None
    candidates.sort(key=score)
    return candidates[0]


def local_target_name(identifier: str, remote_name: str) -> str:
    stem = Path(remote_name).stem.replace(" ", "_")
    ext = remote_name.rsplit(".", 1)[-1]
    return f"{identifier}__{stem}.{ext}"


def cached_tracks() -> list[Path]:
    """Return only valid downloaded audio files (excludes .part and .skip files)."""
    return [
        p for p in CACHE_DIR.iterdir()
        if p.is_file() and p.suffix.lstrip(".").lower() in AUDIO_EXTS
    ]


def handled_identifiers() -> set[str]:
    """
    Return the set of identifier prefixes already handled (downloaded or permanently skipped).
    Local filenames are {identifier}__{stem}.ext or {identifier}__{stem}.skip,
    so we extract the part before __ as the identifier.
    """
    prefixes: set[str] = set()
    for p in CACHE_DIR.iterdir():
        if not p.is_file():
            continue
        stem = p.stem
        prefix = stem.split("__", 1)[0] if "__" in stem else stem
        prefixes.add(prefix)
    return prefixes


def download_file(identifier: str, remote_name: str, target_path: Path) -> None:
    url = IA_DOWNLOAD.format(identifier=identifier, filename=quote(remote_name))
    part = target_path.with_suffix(target_path.suffix + ".part")
    headers = {}
    if part.exists():
        headers["Range"] = f"bytes={part.stat().st_size}-"

    with SESSION.get(url, headers=headers, stream=True, timeout=120) as r:
        if r.status_code not in (200, 206):
            raise RuntimeError(f"download failed {r.status_code} for {url}")
        mode = "ab" if r.status_code == 206 else "wb"
        with open(part, mode) as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    part.replace(target_path)


def clean_partial(target: Path) -> None:
    part = target.with_suffix(target.suffix + ".part")
    if part.exists():
        try:
            part.unlink()
        except Exception:
            pass


def enforce_cache_limits() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    files = cached_tracks()
    # Count cap (soft)
    if len(files) > CACHE_TARGET_COUNT:
        files.sort(key=lambda p: p.stat().st_mtime)
        for p in files[: max(0, len(files) - CACHE_TARGET_COUNT)]:
            try:
                p.unlink()
            except Exception:
                pass

    # Size cap (hard)
    files = cached_tracks()
    total = sum(p.stat().st_size for p in files)
    max_bytes = int(CACHE_MAX_GB * (1024 ** 3))
    if total > max_bytes:
        files.sort(key=lambda p: p.stat().st_mtime)
        for p in files:
            try:
                total -= p.stat().st_size
                p.unlink()
            except Exception:
                pass
            if total <= max_bytes:
                break


def write_playlist() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tracks = sorted(cached_tracks(), key=lambda p: p.name.lower())

    lines = []
    for p in tracks:
        stem = p.stem.split("__", 1)[1] if "__" in p.stem else p.stem
        title = sanitize_title(stem.replace("_", " "))
        lines.append(f'annotate:title="{title}":{p.as_posix()}')

    tmp = PLAYLIST_PATH.with_suffix(".m3u.tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8", newline="\n")
    tmp.replace(PLAYLIST_PATH)


def main() -> None:
    RADIO_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    PLAYLIST_PATH.touch(exist_ok=True)
    IDENTIFIERS_FILE.touch(exist_ok=True)

    query = build_query()
    log(f"query: {query}")

    while True:
        try:
            ids = identifiers_from_file()
            if ids:
                log(f"using identifiers from file ({len(ids)})")
            else:
                ids = advanced_search_identifiers(query)
                log(f"fetched identifiers from advancedsearch ({len(ids)})")

            # Build once per cycle - avoids repeated directory scans inside the loop
            known = handled_identifiers()
            track_count = len(cached_tracks())
            log(f"cycle start: {track_count} tracks cached, {len(known)} identifiers already handled")

            downloaded = 0
            skipped = 0
            for identifier in ids:
                if track_count >= CACHE_TARGET_COUNT:
                    log(f"reached target of {CACHE_TARGET_COUNT} tracks, stopping downloads")
                    break

                # Skip metadata fetch entirely if already downloaded or permanently failed
                if identifier in known:
                    continue

                # Fetch metadata
                try:
                    r = SESSION.get(IA_METADATA.format(identifier=identifier), timeout=30)
                except Exception as e:
                    log(f"metadata fetch error for {identifier} (transient, will retry): {e}")
                    continue  # transient - retry next cycle, no skip marker

                if r.status_code != 200:
                    log(f"metadata {r.status_code} for {identifier}, skipping permanently")
                    known.add(identifier)
                    skipped += 1
                    continue

                meta = r.json()
                f = pick_audio_file(meta.get("files", []))
                if not f:
                    log(f"no audio file found for {identifier}, skipping")
                    known.add(identifier)
                    skipped += 1
                    continue

                remote_name = f["name"]
                local_name = local_target_name(identifier, remote_name)
                target = CACHE_DIR / local_name
                skip_marker = target.with_suffix(".skip")

                if skip_marker.exists() or (target.exists() and target.stat().st_size > 0):
                    known.add(identifier)
                    continue

                log(f"downloading {identifier}/{remote_name} -> {local_name}")
                try:
                    download_file(identifier, remote_name, target)
                    track_count += 1
                    downloaded += 1
                    known.add(identifier)
                except RuntimeError as e:
                    # Permanent HTTP error (e.g. 500, 404) - mark as skip
                    log(f"download failed (permanent): {e}")
                    skip_marker.touch()
                    known.add(identifier)
                    clean_partial(target)
                except Exception as e:
                    # Transient error (timeout, connection reset) - will retry next cycle
                    log(f"download failed (transient, will retry): {e}")
                    clean_partial(target)

            enforce_cache_limits()
            write_playlist()
            log(f"cycle done: {track_count} tracks, {downloaded} downloaded, {skipped} skipped")

        except Exception as e:
            log(f"cycle error: {e}")

        time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
