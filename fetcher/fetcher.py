#!/usr/bin/env python3
import os
import re
import json
import time
import math
import shutil
from pathlib import Path
from urllib.parse import quote

import requests

IA_ADVANCEDSEARCH = "https://archive.org/advancedsearch.php"
IA_METADATA = "https://archive.org/metadata/{identifier}"
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

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "archive-radio-template/1.0 (+https://archive.org)"})


def log(msg: str) -> None:
    print(f"[fetcher] {msg}", flush=True)


def sanitize_title(s: str) -> str:
    s = s.strip().replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    return s[:200]


def build_query() -> str:
    # If user provided a full query, use it.
    if ARCHIVE_QUERY:
        return ARCHIVE_QUERY

    # Otherwise generate a robust query from a handle.
    # "creator" is a common field; "uploader" may exist on some items, so we try both.
    if not ARCHIVE_USER:
        # Safe default: no-op query will return nothing; user must set .env
        return "identifier:__nonexistent__"

    # Restrict to audio items; you can edit this in .env for other media.
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
    # De-dup preserving order
    seen = set()
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
    # Prefer original/derivative mp3 etc, avoid helper files.
    def score(f: dict) -> tuple:
        name = (f.get("name") or "").lower()
        ext = name.rsplit(".", 1)[-1] if "." in name else ""
        ext_rank = AUDIO_EXTS.index(ext) if ext in AUDIO_EXTS else 999
        src = (f.get("source") or "").lower()
        src_rank = 0 if src == "original" else 1
        bad = 1 if (name.startswith("_") or name.endswith(".torrent") or name.endswith("_meta.xml") or name.endswith("_files.xml")) else 0
        return (bad, ext_rank, src_rank, len(name))
    candidates = [f for f in files if isinstance(f, dict) and f.get("name")]
    # Filter to audio extensions only
    candidates = [f for f in candidates if (f["name"].lower().rsplit(".", 1)[-1] if "." in f["name"] else "") in AUDIO_EXTS]
    if not candidates:
        return None
    candidates.sort(key=score)
    return candidates[0]


def local_target_name(identifier: str, remote_name: str) -> str:
    stem = Path(remote_name).stem.replace(" ", "_")
    ext = remote_name.rsplit(".", 1)[-1]
    return f"{identifier}__{stem}.{ext}"


def download_file(identifier: str, remote_name: str, target_path: Path) -> None:
    url = IA_DOWNLOAD.format(identifier=identifier, filename=quote(remote_name))
    part = target_path.with_suffix(target_path.suffix + ".part")
    headers = {}
    if part.exists():
        headers["Range"] = f"bytes={part.stat().st_size}-"

    with SESSION.get(url, headers=headers, stream=True, timeout=60) as r:
        if r.status_code not in (200, 206):
            raise RuntimeError(f"download failed {r.status_code} for {url}")
        mode = "ab" if r.status_code == 206 else "wb"
        with open(part, mode) as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    part.replace(target_path)


def enforce_cache_limits() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    files = [p for p in CACHE_DIR.iterdir() if p.is_file() and not p.name.endswith(".part")]
    # Count cap (soft)
    if len(files) > CACHE_TARGET_COUNT:
        files.sort(key=lambda p: p.stat().st_mtime)  # oldest first
        for p in files[: max(0, len(files) - CACHE_TARGET_COUNT)]:
            try:
                p.unlink()
            except Exception:
                pass

    # Size cap (hard)
    files = [p for p in CACHE_DIR.iterdir() if p.is_file() and not p.name.endswith(".part")]
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
    tracks = [p for p in CACHE_DIR.iterdir() if p.is_file() and not p.name.endswith(".part")]
    tracks.sort(key=lambda p: p.name.lower())

    lines = []
    for p in tracks:
        title = sanitize_title(p.stem)  # identifier__stem
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

            # Download until we have at least CACHE_TARGET_COUNT tracks (or run out)
            existing = {p.name for p in CACHE_DIR.iterdir() if p.is_file()}
            downloaded = 0
            for identifier in ids:
                # Stop if we have enough
                have = [p for p in CACHE_DIR.iterdir() if p.is_file() and not p.name.endswith(".part")]
                if len(have) >= CACHE_TARGET_COUNT:
                    break

                meta_url = IA_METADATA.format(identifier=identifier)
                r = SESSION.get(meta_url, timeout=30)
                if r.status_code != 200:
                    continue
                meta = r.json()
                f = pick_audio_file(meta.get("files", []))
                if not f:
                    continue

                remote_name = f["name"]
                local_name = local_target_name(identifier, remote_name)
                target = CACHE_DIR / local_name

                if target.exists() and target.stat().st_size > 0:
                    continue

                log(f"downloading {identifier}/{remote_name} -> {local_name}")
                try:
                    download_file(identifier, remote_name, target)
                    downloaded += 1
                except Exception as e:
                    log(f"download failed: {e}")
                    # Clean partial
                    part = target.with_suffix(target.suffix + ".part")
                    if part.exists():
                        try:
                            part.unlink()
                        except Exception:
                            pass
                    continue

            enforce_cache_limits()
            write_playlist()
            log(f"playlist updated. downloaded this cycle: {downloaded}")

        except Exception as e:
            log(f"cycle error: {e}")

        time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

