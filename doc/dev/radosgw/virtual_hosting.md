# radosgw virtual hosting

allow the different rest apis to use their own domain names. route requests between them based on Host header

## motivation

radosgw currently serves all rest apis from the same namespace. some are hosted at the root, with others under paths like `admin`, `auth`, `swift` etc. most apis are enabled by default, so parts of the root s3 namespace are unavailable because those paths get routed to other apis

s3 shares the root path with several other apis (iam, s3control, sts, sns) and relies on tricks to disambiguate the requests and route them correctly. these tricks are based on assumptions that may not be reliable, especially as the apis evolve

to avoid these conflicts in path-based routing, we can first route requests by hostname

this _could_ be accomplished by deploying separate radosgw processes for each api and configuring dns accordingly. however,
* radosgw processes are heavy-weight, having several background threads and a main pool of 512 threads
* the overall load would not be well-balanced over radosgws, with some apis expecting very little traffic
* the metadata cache broadcasts invalidation messages, so will limit scaling to many radosgws

radosgw can continue serving all of these apis from the same frontend socket by instead routing the requests according to their http `Host` header. this [virtual hosting](https://en.wikipedia.org/wiki/Virtual_hosting) technique is common in web servers

## existing request routing

config options:

```
rgw_enable_apis = s3, s3control, s3website, swift, swift_auth, admin, sts, iam, notifications
rgw_dns_name =
rgw_dns_s3website_name =
```

request flow:

1. `process_request()` calls `get_manager()` on the root `RGWRESTMgr` to route the given request path to its final `RGWRESTMgr`
2. `RGWRESTMgr::get_handler()` creates the request `RGWHandler` for that api
3. `RGWHandler::get_op()` creates the `RGWOp` based on request method

during startup, `rgw::AppMain::cond_init_apis()` builds a tree of `RGWRESTMgr`s to match nested request paths

`RGWRESTMgr::register_resource(string resource, RGWRESTMgr* mgr)` is used to register child managers at relative paths for static routing

each api listed in `rgw_enable_apis` registers their own `RGWRESTMgr` at the configured path. ex `RGWRESTMgr_SWIFT` at `rgw_swift_url_prefix`, `RGWRESTMgr_Admin` at `rgw_admin_entry`

s3 paths can have arbitrary bucket names, so require dynamic routing. `RGWRESTMgr_S3` is registered as the default manager, selected when no registered static paths match. `RGWRESTMgr_S3::get_handler()` interpets the path itself when producing the request handler

other apis hosted at the root path (iam, s3control, s3website, sns, sts) must be routed internally by `RGWRESTMgr_S3`

the `Host` header is matched against `rgw_dns_name`/`rgw_dns_s3website_name` to:
* disambiguate between s3 and s3website requests, and
* transform s3 virtual-host style requests into path-style requests

## virtual host request routing

new config options:

```
rgw_dns_admin_name =
rgw_dns_s3control_name =
rgw_dns_sns_name =
rgw_dns_sts_name =
rgw_dns_swift_name =
rgw_dns_swift_auth_name =
```

each option may specify a dns domain name. each unique domain name maps to a virtual host

multiple apis can be configured on the same virtual host (ex `rgw_dns_swift_name` = `rgw_dns_swift_auth_name`)

virtual hosts can be nested? routing prefers subdomains (ex `swift.ceph.example.com` < `auth.swift.ceph.example.com`)

each virtual host gets its own root `RGWRESTMgr` for path-based routing

on startup, apis from `rgw_enable_apis` register to the virtual host corresponding to `rgw_dns_{api}_name`. when empty, register to the default virtual host

the default virtual host is selected when no configured dns names match the Host header. backward compatible with existing configuration

s3 vhost-style requests depend on wildcard matching (ex `bucketname.s3.ceph.example.com` matches `s3.ceph.example.com`), but also support [CNAME records](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#VirtualHostingCustomURLs) where the Host header only contains the bucket name. to support this, radosgw must map all non-matching dns names to the s3 api - meaning that s3 must be in the default virtual host

multisite expects a single zone/zonegroup endpoint to serve both s3 and admin apis, so `rgw_dns_admin_name` should not be overridden in multisite configurations

for apis currently routed by `RGWRESTMgr_S3`:
* create `RGWRESTMgr` classes for each and register with virtual host when configured
* `RGWRESTMgr_S3(bool enable_s3website, bool enable_sts, bool enable_iam, bool enable_pubsub)` only enables nesting when `rgw_dns_{api}_name` is empty

## ssl

support separate ssl certificates for each virtual host

the client sends this host name as the [server name](https://en.wikipedia.org/wiki/Server_Name_Indication) in the tls handshake. we can hook into the handshake with [SSL_CTX_set_tlsext_servername_callback()](https://docs.openssl.org/master/man3/SSL_CTX_set_tlsext_servername_callback/) to select a different ssl context for each virtual host

as above, should all virtual hosts match their subdomains too, or just s3/s3website/s3control?

### configuration

extend `ssl_certificate`/`ssl_private_key` in `rgw_frontends` to optionally specify a server name:
```
  ssl_certificate:s3.ceph.example.com=/path/to/s3.crt ssl_private_key:s3.ceph.example.com=/path/to/s3.key
  ssl_certificate:iam.ceph.example.com=/path/to/iam.crt ssl_private_key:iam.ceph.example.com=/path/to/iam.key
  ssl_certificate=/path/to/rgw.crt ssl_private_key=/path/to/rgw.key
```

* `ssl_certificate`/`ssl_private_key` values without a server name apply to the 'default' ssl context. backward compatible with existing config
* a given server name must specify both `ssl_certificate` and `ssl_private_key` and cannot be duplicated for either
* if there is no default ssl context, reject handshakes that don't match a virtual host

## testing

### s3-tests

s3tests.conf needs separate "host" options for each service

various `get_client()` helpers can look up the right host config by `service_name`

### teuthology

test default config and split config

`qa/tasks/dnsmasq.py` can set up dns names, already used in `rgw/hadoop-s3a` and `rgw/website` suites

TODO: wildcard dns and ssl certficates for s3 vhost-style

## cephadm/rook

automate dns configuration and certificate generation
