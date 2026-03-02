FQDN=${KAFKA_CERT_HOSTNAME:-localhost}
IP_SAN=${KAFKA_CERT_IP:-}
KEYFILE=server.keystore.jks
TRUSTFILE=server.truststore.jks
CAFILE=y-ca.crt
CAKEYFILE=y-ca.key
REQFILE=$FQDN.req
CERTFILE=$FQDN.crt
MYPW=mypassword
VALIDITY=36500

rm -f $KEYFILE
rm -f $TRUSTFILE
rm -f $CAFILE
rm -f $REQFILE
rm -f $CERTFILE

SAN_STRING="DNS:$FQDN"
if [ -n "$IP_SAN" ]; then
  SAN_STRING="$SAN_STRING,IP:$IP_SAN"
fi

echo "########## create the request in key store '$KEYFILE' with SAN=$SAN_STRING"
keytool -keystore $KEYFILE -alias localhost \
  -dname "CN=$FQDN, OU=Michigan Engineering, O=Red Hat Inc, \
  L=Ann Arbor, ST=Michigan, C=US" \
  -storepass $MYPW -keypass $MYPW \
  -validity $VALIDITY -genkey -keyalg RSA \
  -ext SAN=$SAN_STRING

echo "########## create the CA '$CAFILE'"
openssl req -new -nodes -x509 -keyout $CAKEYFILE -out $CAFILE \
  -days $VALIDITY -subj \
  '/C=US/ST=Michigan/L=Ann Arbor/O=Red Hat Inc/OU=Michigan Engineering/CN=yuval-1'

echo "########## store the CA in trust store '$TRUSTFILE'"
keytool -keystore $TRUSTFILE -storepass $MYPW -alias CARoot \
  -noprompt -importcert -file $CAFILE

echo "########## create a request '$REQFILE' for signing in key store '$KEYFILE'"
keytool -storepass $MYPW -keystore $KEYFILE \
  -alias localhost -certreq -file $REQFILE

echo "########## sign and create certificate '$CERTFILE' with SAN=$SAN_STRING"
EXTFILE=$(mktemp)
cat > $EXTFILE << EOF
subjectAltName=$SAN_STRING
EOF

openssl x509 -req -CA $CAFILE -CAkey $CAKEYFILE -CAcreateserial \
  -days $VALIDITY \
  -in $REQFILE -out $CERTFILE -extfile $EXTFILE

rm -f $EXTFILE

echo "########## store CA '$CAFILE' in key store '$KEYFILE'"
keytool -storepass $MYPW -keystore $KEYFILE -alias CARoot \
  -noprompt -importcert -file $CAFILE

echo "########## store certificate '$CERTFILE' in key store '$KEYFILE'"
keytool -storepass $MYPW -keystore $KEYFILE -alias localhost \
  -import -file $CERTFILE

