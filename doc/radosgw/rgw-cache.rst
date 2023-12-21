==========================
RGW Data caching and CDN
==========================

.. versionadded:: Octopus

.. contents::

This feature adds to RGW the ability to securely cache objects and offload the workload from the cluster, using Nginx.
After an object is accessed the first time it will be stored in the Nginx cache directory.
When data is already cached, it need not be fetched from RGW. A permission check will be made against RGW to ensure the requesting user has access.
This feature is based on some Nginx modules, ngx_http_auth_request_module, https://github.com/kaltura/nginx-aws-auth-module, Openresty for Lua capabilities.

Currently, this feature will cache only AWSv4 requests (only s3 requests), caching-in the output of the 1st GET request
and caching-out on subsequent GET requests, passing thru transparently PUT,POST,HEAD,DELETE and COPY requests.


The feature introduces 2 new APIs: Auth and Cache.

    NOTE: The `D3N RGW Data Cache`_ is an alternative data caching mechanism implemented natively in the Rados Gateway.

New APIs
-------------------------

There are 2 new APIs for this feature:

Auth API - The cache uses this to validate that a user can access the cached data

Cache API - Adds the ability to override securely Range header, that way Nginx can use it is own smart cache on top of S3:
https://www.nginx.com/blog/smart-efficient-byte-range-caching-nginx/
Using this API gives the ability to read ahead objects when clients asking a specific range from the object.
On subsequent accesses to the cached object, Nginx will satisfy requests for already-cached ranges from the cache. Uncached ranges will be read from RGW (and cached).

Auth API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This API Validates a specific authenticated access being made to the cache, using RGW's knowledge of the client credentials and stored access policy.
Returns success if the encapsulated request would be granted.

Cache API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This API is meant to allow changing signed Range headers using a privileged user, cache user.

Creating cache user

::

$ radosgw-admin user create --uid=<uid for cache user> --display-name="cache user" --caps="amz-cache=read"

This user can send to the RGW the Cache API header ``X-Amz-Cache``, this header contains the headers from the original request(before changing the Range header).
It means that ``X-Amz-Cache`` built from several headers.
The headers that are building the ``X-Amz-Cache`` header are separated by char with ASCII code 177 and the header name and value are separated by char ASCII code 178.
The RGW will check that the cache user is an authorized user and if it is a cache user,
if yes it will use the ``X-Amz-Cache`` to revalidate that the user has permissions, using the headers from the X-Amz-Cache.
During this flow, the RGW will override the Range header.


Using Nginx with RGW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Download the source of Openresty:

::

$ wget https://openresty.org/download/openresty-1.15.8.3.tar.gz

git clone the AWS auth Nginx module:

::

$ git clone https://github.com/kaltura/nginx-aws-auth-module

untar the openresty package:

::

$ tar xvzf openresty-1.15.8.3.tar.gz
$ cd openresty-1.15.8.3

Compile openresty, Make sure that you have pcre lib and openssl lib:

::

$ sudo yum install pcre-devel openssl-devel gcc curl zlib-devel nginx
$ ./configure --add-module=<the nginx-aws-auth-module dir> --with-http_auth_request_module --with-http_slice_module --conf-path=/etc/nginx/nginx.conf
$ gmake -j $(nproc)
$ sudo gmake install
$ sudo ln -sf /usr/local/openresty/bin/openresty /usr/bin/nginx

Put in-place your Nginx configuration files and edit them according to your environment:

All Nginx conf files are under:
https://github.com/ceph/ceph/tree/main/examples/rgw/rgw-cache

`nginx.conf` should go to `/etc/nginx/nginx.conf`

`nginx-lua-file.lua` should go to `/etc/nginx/nginx-lua-file.lua`

`nginx-default.conf` should go to `/etc/nginx/conf.d/nginx-default.conf`

The parameters that are most likely to require adjustment according to the environment are located in the file `nginx-default.conf`

Modify the example values of *proxy_cache_path* and *max_size* at:

::

 proxy_cache_path /data/cache levels=2:2:2 keys_zone=mycache:999m max_size=20G inactive=1d use_temp_path=off;


And modify the example *server* values to point to the RGWs URIs:

::

 server rgw1:8000 max_fails=2 fail_timeout=5s;
 server rgw2:8000 max_fails=2 fail_timeout=5s;
 server rgw3:8000 max_fails=2 fail_timeout=5s;

| It is important to substitute the *access key* and *secret key* located in the `nginx.conf` with those belong to the user with the `amz-cache` caps
| for example, create the `cache` user as following:

::

 radosgw-admin user create --uid=cacheuser --display-name="cache user" --caps="amz-cache=read" --access-key <access> --secret <secret>

It is possible to use Nginx slicing which is a better method for streaming purposes.

For using slice you should use `nginx-slicing.conf` and not `nginx-default.conf`

Further information about Nginx slicing:

https://docs.nginx.com/nginx/admin-guide/content-cache/content-caching/#byte-range-caching


If you do not want to use the prefetch caching, It is possible to replace `nginx-default.conf` with `nginx-noprefetch.conf`
Using `noprefetch` means that if the client is sending range request of 0-4095 and then 0-4096 Nginx will cache those requests separately, So it will need to fetch those requests twice.


Run Nginx(openresty):

::

$ sudo systemctl restart nginx

Appendix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**A note about performance:** In certain instances like development environment, disabling the authentication by commenting the following line in `nginx-default.conf`:

::

 #auth_request /authentication;

may (depending on the hardware) increases the performance significantly as it forgoes the auth API calls to radosgw.


.. _D3N RGW Data Cache: ../d3n_datacache/
