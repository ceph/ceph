# PowerDNS RADOS Gateway backend

A backend for PowerDNS to direct RADOS Gateway bucket traffic to the correct regions.

For example, two regions exist, US and EU.

    EU: o.myobjects.eu
    US: o.myobjects.us

A global domain o.myobjects.com exists.

Bucket 'foo' exists in the region EU and 'bar' in US.

    foo.o.myobjects.com will return a CNAME to foo.o.myobjects.eu
    bar.o.myobjects.com will return a CNAME to foo.o.myobjects.us

The HTTP Remote Backend from PowerDNS is used in this case: http://doc.powerdns.com/html/remotebackend.html

PowerDNS must be compiled with Remote HTTP backend support enabled, this is not default.

For more information visit the [Blueprint](http://wiki.ceph.com/Planning/Blueprints/Firefly/PowerDNS_backend_for_RGW)

# Configuration

## PowerDNS
    launch=remote
    remote-connection-string=http:url=http://localhost:6780/dns

## PowerDNS backend
Usage for this backend is showed by invoking with --help. See rgw-pdns.conf.in for a configuration example

The ACCESS and SECRET key pair requires the caps "metadata=read"

# Testing

$ curl -X GET http://localhost:6780/dns/lookup/foo.o.myobjects.com/ANY

Should return something like:

    {
     "result": [
      {
       "content": "foo.o.myobjects.eu",
       "qtype": "CNAME",
       "qname": "foo.o.myobjects.com",
       "ttl": 60
      }
     ]
    }

## WSGI
You can run this backend directly behind an Apache server with mod_wsgi

    WSGIScriptAlias / /var/www/pdns-backend-rgw.py

Placing that in your virtualhost should be sufficient.

Afterwards point PowerDNS to localhost on port 80:

    launch=remote
    remote-connection-string=http:url=http://localhost/dns
