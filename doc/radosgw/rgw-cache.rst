========================
RGW Data Caching and CDN
========================

.. versionadded:: Octopus

.. contents::

This feature adds to RGW the ability to securely cache objects and offload the workload from the cluster, using Nginx.
After an object is accessed the first time it will be stored in the Nginx cache directory.
When data is already cached, it need not be fetched from RGW. A permission check will be made against RGW to ensure the requesting user has access.
This feature is based on the Nginx modules ``ngx_http_auth_request_module`` and `nginx-aws-auth-module <https://github.com/kaltura/nginx-aws-auth-module>`_, and OpenResty for Lua capabilities.

Currently this feature will cache only AWSv4 requests (only S3 requests), caching-in the output of the first GET request
and caching-out on subsequent GET requests, passing through transparently PUT,POST,HEAD,DELETE and COPY requests.


The feature introduces 2 new APIs: Auth and Cache.

.. note:: The `D3N RGW Data Cache`_ is an alternative data caching mechanism implemented natively in the RADOS Gateway.

New APIs
--------

There are 2 new APIs for this feature:

- **Auth API:** The cache uses this to validate that a user can access the cached data.
- **Cache API:** Adds the ability to override securely ``Range`` header so that Nginx can use its own `smart cache <https://www.nginx.com/blog/smart-efficient-byte-range-caching-nginx/>`_ on top of S3.
  Using this API gives the ability to read ahead objects when client is asking a specific range from the object.
  On subsequent accesses to the cached object, Nginx will satisfy requests for already-cached ranges from the cache. Uncached ranges will be read from RGW (and cached).

Auth API
~~~~~~~~

This API validates a specific authenticated access being made to the cache, using RGW's knowledge of the client credentials and stored access policy.
Returns success if the encapsulated request would be granted.

Cache API
~~~~~~~~~

This API is meant to allow changing signed ``Range`` headers using a privileged cache user.

Creating the cache user:

.. prompt:: bash #

   radosgw-admin user create --uid=<uid for cache user> --display-name="cache user" --caps="amz-cache=read"

This user can send to the RGW the Cache API header ``X-Amz-Cache``. This header contains the headers from the original request (before changing the ``Range`` header):

- Original headers are separated from each other by a character with ASCII code 177 decimal.
- Each original header and its value are separated by a character with ASCII code 178 decimal.

The RGW will check that the user is an authorized user and that the value is a cache user.
If both checks succeed it will use the ``X-Amz-Cache`` to revalidate that the user has permissions, using the original headers stored in the ``X-Amz-Cache`` header.
During this flow the RGW will override the ``Range`` header.


Using Nginx with RGW
--------------------

Download the source of OpenResty:

.. prompt:: bash $

   wget https://openresty.org/download/openresty-1.15.8.3.tar.gz

Use git to clone the Nginx AWS authentication module:

.. prompt:: bash $

   git clone https://github.com/kaltura/nginx-aws-auth-module

Untar the OpenResty package:

.. prompt:: bash $

   tar xvzf openresty-1.15.8.3.tar.gz
   cd openresty-1.15.8.3

Compile OpenResty, make sure that you have ``pcre`` library and ``openssl`` library:

.. prompt:: bash $

   sudo yum install pcre-devel openssl-devel gcc curl zlib-devel nginx
   ./configure --add-module=<the nginx-aws-auth-module dir> --with-http_auth_request_module --with-http_slice_module --conf-path=/etc/nginx/nginx.conf
   gmake -j $(nproc)
   sudo gmake install
   sudo ln -sf /usr/local/openresty/bin/openresty /usr/bin/nginx

Put in-place your Nginx configuration files and edit them according to your environment:

Example Nginx configuration files are available at
https://github.com/ceph/ceph/tree/main/examples/rgw/rgw-cache

- ``nginx.conf`` should go to ``/etc/nginx/nginx.conf``.
- ``nginx-lua-file.lua`` should go to ``/etc/nginx/nginx-lua-file.lua``.
- ``nginx-default.conf`` should go to ``/etc/nginx/conf.d/nginx-default.conf``.

The parameters that are most likely to require adjustment according to the environment are located in the file ``nginx-default.conf``.

Modify the example values of ``proxy_cache_path`` and ``max_size`` at:

::

 proxy_cache_path /data/cache levels=2:2:2 keys_zone=mycache:999m max_size=20G inactive=1d use_temp_path=off;


And modify the example ``server`` values to point to the RGWs URIs:

::

 server rgw1:8000 max_fails=2 fail_timeout=5s;
 server rgw2:8000 max_fails=2 fail_timeout=5s;
 server rgw3:8000 max_fails=2 fail_timeout=5s;

| It is important to substitute the *access key* and *secret key* located in the ``nginx.conf`` file with those belonging to the user with the ``amz-cache`` caps.
| For example, create the cache user as follows:

.. prompt:: bash #

   radosgw-admin user create --uid=cacheuser --display-name="cache user" --caps="amz-cache=read" --access-key <access> --secret <secret>

It is possible to use `Nginx slicing <https://docs.nginx.com/nginx/admin-guide/content-cache/content-caching/#byte-range-caching>`_ which is suitable for streaming purposes.

To enable slicing you should use ``nginx-slicing.conf`` instead of ``nginx-default.conf``.


If you do not want to use prefetch caching, it is possible to replace ``nginx-default.conf`` with ``nginx-noprefetch.conf``.
If prefetch caching is disabled Nginx will cache each range request separately and possible overlap in the range requests will be fetched more than once. For example, if a client is sending a range request of 0-4095 and then 0-4096 both requests are fetched completely from RGW.


Run Nginx (OpenResty):

.. prompt:: bash #

   systemctl restart nginx

Appendix
--------

**A note about performance:** In certain instances such as a development environment, disabling authentication may (depending on the hardware) increase performance significantly as it forgoes auth API calls to RADOS Gateway.
This can be done by commenting the following line in ``nginx-default.conf``:

::

 #auth_request /authentication;


.. _D3N RGW Data Cache: ../d3n_datacache/
