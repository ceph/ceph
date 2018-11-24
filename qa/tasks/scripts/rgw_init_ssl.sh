# rgw_init_ssl.sh
# Set up RGW-over-SSL
set -ex
CERTDIR=/srv/salt/ceph/rgw/cert
mkdir -p $CERTDIR
pushd $CERTDIR
openssl req -x509 \
        -nodes \
        -days 1095 \
        -newkey rsa:4096 \
        -keyout rgw.key \
        -out rgw.crt \
        -subj "/C=DE"
cat rgw.key > rgw.pem && cat rgw.crt >> rgw.pem
popd
GLOBALYML=/srv/pillar/ceph/stack/global.yml
cat <<EOF >> $GLOBALYML
rgw_init: default-ssl
EOF
cat $GLOBALYML
cp /srv/salt/ceph/configuration/files/rgw-ssl.conf \
    /srv/salt/ceph/configuration/files/ceph.conf.d/rgw.conf
