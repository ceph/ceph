restful plugin
==============

RESTful plugin offers the REST API access to the status of the cluster. RESTful
plugin enables you to secure the API endpoints via SSL. If you don't have a
security certificate and key already, you need to create them first::

  openssl req -new -nodes -x509 \
    -subj "/O=IT/CN=ceph-mgr-restful" \
    -days 3650 -keyout $PKEY -out $CERT -extensions v3_ca

where ``$PKEY`` and ``$CERT`` are the paths to the private key and the
certificate. And then you need to import the keystore to the cluster using the
configuration key facility, so RESTful plugin can read them at startup::

  ceph config-key put mgr/restful/$name/crt -i $CERT
  ceph config-key put mgr/restful/$name/key -i $PKEY

Also, like other web applications, RESTful plugin is bound to the a hostname and
a port::

  ceph config-key put mgr/restful/$name/server_addr $IP
  ceph config-key put mgr/restful/$name/server_port $PORT

If not specified, the plugin uses ``127.0.0.1:8003`` by default.
