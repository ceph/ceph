# salt_api_test.sh
# Salt API test script
set -e
TMPFILE=$(mktemp)
curl --silent http://$(hostname):8000/ | tee $TMPFILE # show curl output in log
test -s $TMPFILE
jq . $TMPFILE >/dev/null
echo -en "\\n" # this is just for log readability
rm $TMPFILE
echo "Salt API test passed"
