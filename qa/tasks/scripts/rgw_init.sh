# rgw_init.sh
# Set up RGW
set -ex
USERSYML=/srv/salt/ceph/rgw/users/users.d/rgw.yml
cat <<EOF > $USERSYML
- { uid: "demo", name: "Demo", email: "demo@demo.nil" }
- { uid: "demo1", name: "Demo1", email: "demo1@demo.nil" }
EOF
cat $USERSYML
