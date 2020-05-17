========================== 
RGW Data caching and CDN
==========================

.. versionadded:: Octopus

.. contents::

This feature adds to RGW the ability to securely cache objects and offload the workload from the cluster, using Nginx.
After an object is accessed the first time it will be stored in Nginx directory.
When data is already cached, it need not be fetched from RGW. A permission check will be made against RGW to ensure the requesting user has access.
This feature is based on some Nginx modules, ngx_http_auth_request_module, https://github.com/kaltura/nginx-aws-auth-module, Openresty for lua capabilities.     
Currently this feature only works for GET requests and it will cache only AWSv4 requests (only s3 requests).
The feature introduces 2 new APIs: Auth and Cache.

New APIs
-------------------------

There are 2 new apis for this feature:

Auth API - The cache uses this to validate that an user can access the cached data

Cache API - Adds the ability to override securely Range header, that way Nginx can use it is own smart cache on top of S3:
https://www.nginx.com/blog/smart-efficient-byte-range-caching-nginx/
Using this API gives the ability to read ahead objects when clients asking a specific range from the object. 
On subsequent accesses to the cached object, Nginx will satisfy requests for already-cached ranges from cache. Uncached ranges will be read from RGW (and cached).

Auth API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                                
This APIValidates a specific authenticated access being made to the cache, using RGW's knowledge of the client credentials and stored access policy. 
Returns success if the encapsulated request would be granted.

Cache API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This API meant to allow changing signed Range headers using a privileged user, cache user.

Creating cache user

::

$ radosgw-admin user create --uid=<uid for cache user> --display-name="cache user" --caps="amz-cache=read"

This user can send to the RGW the Cache API header ``X-Amz-Cache``, this header contains the headers from the original request(before changing the Range header).
It means that ``X-Amz-Cache`` built from several headers.
The headers that are building the ``X-Amz-Cache`` header are separated by char with ascii code 177 and the header name and value are separated by char ascii code 178.
The RGW will check that the cache user is an authorized user and if it is a cache user, 
if yes it will use the ``X-Amz-Cache`` to revalidate that the user have permissions, using the headers from the X-Amz-Cache.
During this flow the RGW will override the Range header.


Using Nginx with RGW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Download the source of Openresty:

::

$ wget https://openresty.org/download/openresty-1.15.8.3.tar.gz

git clone the aws auth nginx module:

::

$ git clone https://github.com/kaltura/nginx-aws-auth-module

untar the openresty package:

::

$ tar xvzf openresty-1.15.8.3.tar.gz
$ cd openresty-1.15.8.3

Compile openresty, Make sure that you have pcre lib and openssl lib:

::

$ sudo yum install pcre-devel openssl-devel gcc curl zlib-devel nginx
$ ./configure --add-module=<the nginx-aws-auth-module dir> --with-http_auth_request_module --with-http_slice_module
$ gmake -j $(nproc)
$ sudo gmake install
$ sudo ln -sf /usr/local/openresty/bin/openresty /usr/bin/nginx

Put in-place your nginx configuration files and edit them according to your environment:

nginx.conf should go to /etc/nginx/nginx.conf
nginx-lua-file.lua should go to /etc/nginx/nginx-lua-file.lua
nginx-default.conf should go to /etc/nginx/conf.d/nginx-default.conf

It is possible to use nginx slicing which is a better method for streaming purposes.

For using slice you should use nginx-slicing.conf and not nginx-default.conf

Further information about nginx slicing:

https://docs.nginx.com/nginx/admin-guide/content-cache/content-caching/#byte-range-caching


If you do not want to use the prefetch caching, It is possible to replace nginx-default.conf with nginx-noprefetch.conf
Using noprefetch means that if the client is sending range request of 0-4095 and then 0-4096 Nginx will cache those requests separately, So it will need to fetch those requests twice.


Run nginx(openresty):
::

$ nginx -c /etc/nginx/nginx.conf
