USER='desterronics'
QUERY="uploader:(${USER}) AND mediatype:(audio)"
ROWS=200
PAGE=1

: > identifiers.txt

while true; do
  JSON="$(curl -sG 'https://archive.org/advancedsearch.php' \
    --data-urlencode "q=${QUERY}" \
    --data-urlencode 'fl[]=identifier' \
    --data-urlencode "rows=${ROWS}" \
    --data-urlencode "page=${PAGE}" \
    --data-urlencode 'output=json')"

  COUNT="$(echo "$JSON" | jq -r '.response.docs | length')"
  if [ "$COUNT" -eq 0 ]; then
    break
  fi

  echo "$JSON" | jq -r '.response.docs[].identifier' >> identifiers.txt

  NUMFOUND="$(echo "$JSON" | jq -r '.response.numFound')"
  SHOWN=$((PAGE * ROWS))
  if [ "$SHOWN" -ge "$NUMFOUND" ]; then
    break
  fi

  PAGE=$((PAGE + 1))
done

echo "Wrote $(wc -l < identifiers.txt) identifiers to identifiers.txt"

