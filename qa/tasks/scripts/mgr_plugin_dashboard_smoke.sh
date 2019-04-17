# mgr_plugin_dashboard_smoke.sh
#
# smoke test the MGR dashboard module
#
# args: None

set -ex
URL=$(ceph mgr services 2>/dev/null | jq .dashboard | sed -e 's/"//g')
curl --insecure --silent $URL 2>&1 > dashboard.html
test -s dashboard.html
file dashboard.html | grep "HTML document"
# verify SUSE branding (in title and suse_favicon)
grep -i "suse" dashboard.html
echo "OK" >/dev/null
