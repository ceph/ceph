 TOKEN=""
  for i in $(seq 1 3); do
    if [ -z "$TOKEN" ]; then
      RESP=$(aws --endpoint-url http://127.0.0.1:9080 s3api list-objects-v2 --bucket perf-bucket-7 --max-keys 1000)
    else
      RESP=$(aws --endpoint-url http://127.0.0.1:9080 s3api list-objects-v2 --bucket perf-bucket-7 --max-keys 1000 --continuation-token "$TOKEN")
    fi
    echo "=== Page $i ==="
    echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'KeyCount={d.get(\"KeyCount\")} IsTruncated={d.get(\"IsTruncated\")} 
  First={d[\"Contents\"][0][\"Key\"]} Last={d[\"Contents\"][-1][\"Key\"]}')"
    TOKEN=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('NextContinuationToken',''))")
    [ -z "$TOKEN" ] && break
  done
