=====================================
 Integrating with OpenStack Keystone
=====================================

It is possible to integrate the Ceph Object Gateway with Keystone, the OpenStack
identity service. This sets up the gateway to accept Keystone as the users
authority. A user that Keystone authorizes to access the gateway will also be
automatically created on the Ceph Object Gateway (if didn't exist beforehand). A
token that Keystone validates will be considered as valid by the gateway.

The following configuration options are available for Keystone integration::

	[client.radosgw.gateway]
	rgw keystone url = {keystone server url:keystone server admin port}
	rgw keystone admin token = {keystone admin token}
	rgw keystone accepted roles = {accepted user roles}
	rgw keystone token cache size = {number of tokens to cache}
	rgw keystone revocation interval = {number of seconds before checking revoked tickets}
	rgw s3 auth use keystone = true
	nss db path = {path to nss db}

A Ceph Object Gateway user is mapped into a Keystone ``tenant``. A Keystone user
has different roles assigned to it on possibly more than a single tenant. When
the Ceph Object Gateway gets the ticket, it looks at the tenant, and the user
roles that are assigned to that ticket, and accepts/rejects the request
according to the ``rgw keystone accepted roles`` configurable.

Keystone itself needs to be configured to point to the Ceph Object Gateway as an
object-storage endpoint::

	keystone service-create --name swift --type object-store
	keystone endpoint-create --service-id <id> --publicurl http://radosgw.example.com/swift/v1 \
		--internalurl http://radosgw.example.com/swift/v1 --adminurl http://radosgw.example.com/swift/v1


The keystone URL is the Keystone admin RESTful API URL. The admin token is the
token that is configured internally in Keystone for admin requests.

The Ceph Object Gateway will query Keystone periodically for a list of revoked
tokens. These requests are encoded and signed. Also, Keystone may be configured
to provide self-signed tokens, which are also encoded and signed. The gateway
needs to be able to decode and verify these signed messages, and the process
requires that the gateway be set up appropriately. Currently, the Ceph Object
Gateway will only be able to perform the procedure if it was compiled with
``--with-nss``. Configuring the Ceph Object Gateway to work with Keystone also
requires converting the OpenSSL certificates that Keystone uses for creating the
requests to the nss db format, for example::

	mkdir /var/ceph/nss

	openssl x509 -in /etc/keystone/ssl/certs/ca.pem -pubkey | \
		certutil -d /var/ceph/nss -A -n ca -t "TCu,Cu,Tuw"
	openssl x509 -in /etc/keystone/ssl/certs/signing_cert.pem -pubkey | \
		certutil -A -d /var/ceph/nss -n signing_cert -t "P,P,P"
