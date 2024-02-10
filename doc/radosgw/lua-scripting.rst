=============
Lua Scripting
=============

.. versionadded:: Pacific

.. contents::

This feature allows users to assign execution context to Lua scripts. The supported contexts are:

 - ``preRequest`` which will execute a script before each operation is performed
 - ``postRequest`` which will execute after each operation is performed

A request (pre or post) or data (get or put) context script may be constrained to operations belonging to a specific tenant's users.
The request context script can also access fields in the request and modify certain fields, as well as the `Global RGW Table`_.
The data context script can access the content of the object as well as the request fields and the `Global RGW Table`_. 
All Lua language features can be used in all contexts.
An execution of a script in a context can use up to 500K byte of memory. This include all libraries used by Lua, but not the memory which is managed by the RGW itself, and may be accessed from Lua.
To change this default value, use the ``rgw_lua_max_memory_per_state`` configuration parameter. Note that the basic overhead of Lua with its standard libraries is ~32K bytes. To disable the limit, use zero or a negative number.

By default, all Lua standard libraries are available in the script, however, in order to allow for other Lua modules to be used in the script, we support adding packages to an allowlist:

  - Adding a Lua package to the allowlist, or removing a packge from it does not install or remove it. For the changes to take affect a "reload" command should be called.
  - In addition all packages in the allowlist are being re-installed using the luarocks package manager on radosgw restart.
  - To add a package that contains C source code that needs to be compiled, use the ``--allow-compilation`` flag. In this case a C compiler needs to be available on the host
  - Lua packages are installed in, and used from, a directory local to the radosgw. Meaning that Lua packages in the allowlist are separated from any Lua packages available on the host.
    By default, this directory would be ``/tmp/luarocks/<entity name>``. Its prefix part (``/tmp/luarocks/``) could be set to a different location via the ``rgw_luarocks_location`` configuration parameter. 
    Note that this parameter should not be set to one of the default locations where luarocks install packages (e.g. ``$HOME/.luarocks``, ``/usr/lib64/lua``, ``/usr/share/lua``).
	

.. toctree::
   :maxdepth: 1


Script Management via CLI
-------------------------

To upload a script:
   

::
   
   # radosgw-admin script put --infile={lua-file-path} --context={preRequest|postRequest} [--tenant={tenant-name}]


* When uploading a script into a cluster deployed with cephadm, use the following command:

::

  # cephadm shell radosgw-admin script put --infile=/rootfs/{lua-file-path} --context={prerequest|postrequest} [--tenant={tenant-name}]


To print the content of the script to standard output:

::
   
   # radosgw-admin script get --context={preRequest|postRequest} [--tenant={tenant-name}]


To remove the script:

::
   
   # radosgw-admin script rm --context={preRequest|postRequest} [--tenant={tenant-name}]


Package Management via CLI
--------------------------

To add a package to the allowlist:

::

  # radosgw-admin script-package add --package={package name} [--allow-compilation]


To add a specific version of a package to the allowlist:

::

  # radosgw-admin script-package add --package='{package name} {package version}' [--allow-compilation]


* When adding a different version of a package which already exists in the list, the newly
  added version will override the existing one.

* When adding a package without a version specified, the latest version of the package
  will be added.


To remove a package from the allowlist:

::

  # radosgw-admin script-package rm --package={package name}


To remove a specific version of a package from the allowlist:

::

  # radosgw-admin script-package rm --package='{package name} {package version}'


* When removing a package without a version specified, any existing versions of the
  package will be removed.


To print the list of packages in the allowlist:

::

  # radosgw-admin script-package list


To apply changes from the allowlist to all RGWs:

::

  # radosgw-admin script-package reload


Context Free Functions
----------------------
Debug Log
~~~~~~~~~
The ``RGWDebugLog()`` function accepts a string and prints it to the debug log with priority 20.
Each log message is prefixed ``Lua INFO:``. This function has no return value.

Request Fields
-----------------

.. warning:: This feature is experimental. Fields may be removed or renamed in the future.

.. note::

    - Although Lua is a case-sensitive language, field names provided by the radosgw are case-insensitive. Function names remain case-sensitive.
    - Fields marked "optional" can have a nil value.
    - Fields marked as "iterable" can be used by the pairs() function and with the # length operator.
    - All table fields can be used with the bracket operator ``[]``.
    - ``time`` fields are strings with the following format: ``%Y-%m-%d %H:%M:%S``.


+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| Field                                              | Type     | Description                                                  | Iterable | Writeable | Optional |
+====================================================+==========+==============================================================+==========+===========+==========+
| ``Request.RGWOp``                                  | string   | radosgw operation                                            | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.DecodedURI``                             | string   | decoded URI                                                  | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.ContentLength``                          | integer  | size of the request                                          | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.GenericAttributes``                      | table    | string to string generic attributes map                      | yes      | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Response``                               | table    | response to the request                                      | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Response.HTTPStatusCode``                | integer  | HTTP status code                                             | no       | yes       | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Response.HTTPStatus``                    | string   | HTTP status text                                             | no       | yes       | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Response.RGWCode``                       | integer  | radosgw error code                                           | no       | yes       | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Response.Message``                       | string   | response message                                             | no       | yes       | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.SwiftAccountName``                       | string   | swift account name                                           | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket``                                 | table    | info on the bucket                                           | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.Tenant``                          | string   | tenant of the bucket                                         | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.Name``                            | string   | bucket name (writeable only in ``prerequest`` context)       | no       | yes       | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.Marker``                          | string   | bucket marker (initial id)                                   | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.Id``                              | string   | bucket id                                                    | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.ZoneGroupId``                     | string   | zone group of the bucket                                     | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.CreationTime``                    | time     | creation time of the bucket                                  | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.MTime``                           | time     | modification time of the bucket                              | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.Quota``                           | table    | bucket quota                                                 | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.Quota.MaxSize``                   | integer  | bucket quota max size                                        | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.Quota.MaxObjects``                | integer  | bucket quota max number of objects                           | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Reques.Bucket.Quota.Enabled``                    | boolean  | bucket quota is enabled                                      | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.Quota.Rounded``                   | boolean  | bucket quota is rounded to 4K                                | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.PlacementRule``                   | table    | bucket placement rule                                        | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.PlacementRule.Name``              | string   | bucket placement rule name                                   | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.PlacementRule.StorageClass``      | string   | bucket placement rule storage class                          | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.User``                            | table    | bucket owner                                                 | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.User.Tenant``                     | string   | bucket owner tenant                                          | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Bucket.User.Id``                         | string   | bucket owner id                                              | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Object``                                 | table    | info on the object                                           | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Object.Name``                            | string   | object name                                                  | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Object.Instance``                        | string   | object version                                               | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Object.Id``                              | string   | object id                                                    | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Object.Size``                            | integer  | object size                                                  | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Object.MTime``                           | time     | object mtime                                                 | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.CopyFrom``                               | table    | information on copy operation                                | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.CopyFrom.Tenant``                        | string   | tenant of the object copied from                             | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.CopyFrom.Bucket``                        | string   | bucket of the object copied from                             | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.CopyFrom.Object``                        | table    | object copied from. See: ``Request.Object``                  | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.ObjectOwner``                            | table    | object owner                                                 | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.ObjectOwner.DisplayName``                | string   | object owner display name                                    | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.ObjectOwner.User``                       | table    | object user. See: ``Request.Bucket.User``                    | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.ZoneGroup.Name``                         | string   | name of zone group                                           | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.ZoneGroup.Endpoint``                     | string   | endpoint of zone group                                       | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl``                                | table    | user ACL                                                     | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl.Owner``                          | table    | user ACL owner. See: ``Request.ObjectOwner``                 | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl.Grants``                         | table    | user ACL map of string to grant                              | yes      | no        | no       |
|                                                    |          | note: grants without an Id are not presented when iterated   |          |           |          |
|                                                    |          | and only one of them can be accessed via brackets            |          |           |          |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl.Grants["<name>"]``               | table    | user ACL grant                                               | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl.Grants["<name>"].Type``          | integer  | user ACL grant type                                          | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl.Grants["<name>"].User``          | table    | user ACL grant user                                          | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl.Grants["<name>"].User.Tenant``   | table    | user ACL grant user tenant                                   | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl.Grants["<name>"].User.Id``       | table    | user ACL grant user id                                       | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl.Grants["<name>"].GroupType``     | integer  | user ACL grant group type                                    | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserAcl.Grants["<name>"].Referer``       | string   | user ACL grant referer                                       | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.BucketAcl``                              | table    | bucket ACL. See: ``Request.UserAcl``                         | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.ObjectAcl``                              | table    | object ACL. See: ``Request.UserAcl``                         | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Environment``                            | table    | string to string environment map                             | yes      | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Policy``                                 | table    | policy                                                       | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Policy.Text``                            | string   | policy text                                                  | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Policy.Id``                              | string   | policy Id                                                    | no       | no        | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Policy.Statements``                      | table    | list of string statements                                    | yes      | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserPolicies``                           | table    | list of user policies                                        | yes      | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.UserPolicies[<index>]``                  | table    | user policy. See: ``Request.Policy``                         | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.RGWId``                                  | string   | radosgw host id: ``<host>-<zone>-<zonegroup>``               | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP``                                   | table    | HTTP header                                                  | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP.Parameters``                        | table    | string to string parameter map                               | yes      | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP.Resources``                         | table    | string to string resource map                                | yes      | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP.Metadata``                          | table    | string to string metadata map                                | yes      | yes       | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP.StorageClass``                      | string   | storage class                                                | no       | yes       | yes      |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP.Host``                              | string   | host name                                                    | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP.Method``                            | string   | HTTP method                                                  | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP.URI``                               | string   | URI                                                          | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP.QueryString``                       | string   | HTTP query string                                            | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.HTTP.Domain``                            | string   | domain name                                                  | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Time``                                   | time     | request time                                                 | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Dialect``                                | string   | "S3" or "Swift"                                              | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Id``                                     | string   | request Id                                                   | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.TransactionId``                          | string   | transaction Id                                               | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Tags``                                   | table    | object tags map                                              | yes      | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.User``                                   | table    | user that triggered the request                              | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.User.Tenant``                            | string   | triggering user tenant                                       | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.User.Id``                                | string   | triggering user id                                           | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Trace``                                  | table    | info on trace                                                | no       | no        | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``Request.Trace.Enable``                           | boolean  | tracing is enabled                                           | no       | yes       | no       |
+----------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+

Request Functions
--------------------
Operations Log
~~~~~~~~~~~~~~
The ``Request.Log()`` function prints the requests into the operations log. This function has no parameters. It returns 0 for success and an error code if it fails.

Tracing
~~~~~~~
Tracing functions can be used only in the ``postrequest`` context.

- ``Request.Trace.SetAttribute(<key>, <value>)`` - sets the attribute for the request's trace.
  The function takes two arguments: the first is the ``key``, which should be a string, and the second is the ``value``, which can either be a string or a number (integer or double).
  You may then locate specific traces by using this attribute.

- ``Request.Trace.AddEvent(<name>, <attributes>)`` - adds an event to the first span of the request's trace
  An event is defined by event name, event time, and zero or more event attributes.
  The function accepts one or two arguments: A string containing the event ``name`` should be the first argument, followed by the event ``attributes``, which is optional for events without attributes.
  An event's attributes must be a table of strings.

Background Context
--------------------
The ``background`` context may be used for purposes that include analytics, monitoring, caching data for other context executions.
- Background script execution default interval is 5 seconds.

Data Context
--------------------
Both ``getdata`` and ``putdata`` contexts have the following fields:
- ``Data`` which is read-only and iterable (byte by byte). In case that an object is uploaded or retrieved in multiple chunks, the ``Data`` field will hold data of one chunk at a time.
- ``Offset`` which is holding the offset of the chunk within the entire object.
- The ``Request`` fields and the background ``RGW`` table are also available in these contexts.

Global RGW Table
--------------------
The ``RGW`` Lua table is accessible from all contexts and saves data written to it
during execution so that it may be read and used later during other executions, from the same context of a different one.
- Each RGW instance has its own private and ephemeral ``RGW`` Lua table that is lost when the daemon restarts. Note that ``background`` context scripts will run on every instance.
- The maximum number of entries in the table is 100,000. Each entry has a string key a value with a combined length of no more than 1KB.
A Lua script will abort with an error if the number of entries or entry size exceeds these limits.
- The ``RGW`` Lua table uses string indices and can store values of type: string, integer, double and boolean

Increment/Decrement Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Since entries in the ``RGW`` table could be accessed from multiple places at the same time we need a way
to atomically increment and decrement numeric values in it. For that the following functions should be used:
- ``RGW.increment(<key>, [value])`` would increment the value of ``key`` by ``value`` if value is provided or by 1 if not
- ``RGW.decrement(<key>, [value])`` would decrement the value of ``key`` by ``value`` if value is provided or by 1 if not
- if the value of ``key`` is not numeric, the execution of the script would fail
- if we try to increment or decrement by non-numeric values, the execution of the script would fail


Lua Code Samples
----------------
- Print information on source and destination objects in case of copy:

.. code-block:: lua
 
  function print_object(object)
    RGWDebugLog("  Name: " .. object.Name)
    RGWDebugLog("  Instance: " .. object.Instance)
    RGWDebugLog("  Id: " .. object.Id)
    RGWDebugLog("  Size: " .. object.Size)
    RGWDebugLog("  MTime: " .. object.MTime)
  end

  if Request.CopyFrom and Request.Object and Request.CopyFrom.Object then
    RGWDebugLog("copy from object:")
    print_object(Request.CopyFrom.Object)
    RGWDebugLog("to object:")
    print_object(Request.Object)
  end

- Print ACLs via a "generic function":

.. code-block:: lua

  function print_owner(owner)
    RGWDebugLog("Owner:")
    RGWDebugLog("  Display Name: " .. owner.DisplayName)
    RGWDebugLog("  Id: " .. owner.User.Id)
    RGWDebugLog("  Tenant: " .. owner.User.Tenant)
  end

  function print_acl(acl_type)
    index = acl_type .. "ACL"
    acl = Request[index]
    if acl then
      RGWDebugLog(acl_type .. "ACL Owner")
      print_owner(acl.Owner)
      RGWDebugLog("  there are " .. #acl.Grants .. " grant for owner")
      for k,v in pairs(acl.Grants) do
        RGWDebugLog("    Grant Key: " .. k)
        RGWDebugLog("    Grant Type: " .. v.Type)
        RGWDebugLog("    Grant Group Type: " .. v.GroupType)
        RGWDebugLog("    Grant Referer: " .. v.Referer)
        RGWDebugLog("    Grant User Tenant: " .. v.User.Tenant)
        RGWDebugLog("    Grant User Id: " .. v.User.Id)
      end
    else
      RGWDebugLog("no " .. acl_type .. " ACL in request: " .. Request.Id)
    end 
  end

  print_acl("User")
  print_acl("Bucket")
  print_acl("Object")

- Use of operations log only in case of errors:

.. code-block:: lua
  
  if Request.Response.HTTPStatusCode ~= 200 then
    RGWDebugLog("request is bad, use ops log")
    rc = Request.Log()
    RGWDebugLog("ops log return code: " .. rc)
  end

- Set values into the error message:

.. code-block:: lua

  if Request.Response.HTTPStatusCode == 500 then
    Request.Response.Message = "<Message> something bad happened :-( </Message>"
  end

- Add metadata to objects that was not originally sent by the client:

In the ``prerequest`` context we should add:

.. code-block:: lua

  if Request.RGWOp == 'put_obj' then
    Request.HTTP.Metadata["x-amz-meta-mydata"] = "my value"
  end

In the ``postrequest`` context we look at the metadata:

.. code-block:: lua

  RGWDebugLog("number of metadata entries is: " .. #Request.HTTP.Metadata)
  for k, v in pairs(Request.HTTP.Metadata) do
    RGWDebugLog("key=" .. k .. ", " .. "value=" .. v)
  end
 
- Use modules to create Unix socket based, JSON encoded, "access log":

First we should add the following packages to the allowlist:

::

  # radosgw-admin script-package add --package=lua-cjson --allow-compilation
  # radosgw-admin script-package add --package=luasocket --allow-compilation


Then, run a server to listen on the Unix socket. For example, use "netcat":

::

  # rm -f /tmp/socket       
  # nc -vklU /tmp/socket

And last, do a restart for the radosgw and upload the following script to the ``postrequest`` context:

.. code-block:: lua

  if Request.RGWOp == "get_obj" then
    local json = require("cjson")
    local socket = require("socket")
    local unix = require("socket.unix")
    local s = assert(unix())
    E = {}

    msg = {bucket = (Request.Bucket or (Request.CopyFrom or E).Bucket).Name,
      time = Request.Time,
      operation = Request.RGWOp,
      http_status = Request.Response.HTTPStatusCode,
      error_code = Request.Response.HTTPStatus,
      object_size = Request.Object.Size,
      trans_id = Request.TransactionId}

    assert(s:connect("/tmp/socket"))
    assert(s:send(json.encode(msg).."\n"))
    assert(s:close())
  end


- Trace only requests of specific bucket

Tracing is disabled by default, so we should enable tracing for this specific bucket

.. code-block:: lua

  if Request.Bucket.Name == "my-bucket" then
      Request.Trace.Enable = true
  end


If `tracing is enabled <https://docs.ceph.com/en/latest/jaegertracing/#how-to-enable-tracing-in-ceph/>`_ on the RGW, the value of Request.Trace.Enable is true, so we should disable tracing for all other requests that do not match the bucket name.
In the ``prerequest`` context:

.. code-block:: lua

  if Request.Bucket.Name ~= "my-bucket" then
      Request.Trace.Enable = false
  end

Note that changing ``Request.Trace.Enable`` does not change the tracer's state, but disables or enables the tracing for the request only.


- Add Information for requests traces

in ``postrequest`` context, we can add attributes and events to the request's trace.

.. code-block:: lua

  Request.Trace.AddEvent("lua script execution started")

  Request.Trace.SetAttribute("HTTPStatusCode", Request.Response.HTTPStatusCode)

  event_attrs = {}
  for k,v in pairs(Request.GenericAttributes) do
    event_attrs[k] = v
  end

  Request.Trace.AddEvent("second event", event_attrs)

- The entropy value of an object could be used to detect whether the object is encrypted. 
  The following script calculates the entropy and size of uploaded objects and print to debug log

in the ``putdata`` context, add the following script

.. code-block:: lua

	function object_entropy()
		local byte_hist = {}
		local byte_hist_size = 256 
		for i = 1,byte_hist_size do
			byte_hist[i] = 0 
		end 
		local total = 0 

		for i, c in pairs(Data)  do
			local byte = c:byte() + 1 
			byte_hist[byte] = byte_hist[byte] + 1 
			total = total + 1 
		end 

		entropy = 0 

		for _, count in ipairs(byte_hist) do
			if count ~= 0 then
				local p = 1.0 * count / total
				entropy = entropy - (p * math.log(p)/math.log(byte_hist_size))
			end 
		end 

		return entropy
	end

	local full_name = Request.Bucket.Name.."\\"..Request.Object.Name
	RGWDebugLog("entropy of chunk of: " .. full_name .. " at offset:" .. tostring(Offset)  ..  " is: " .. tostring(object_entropy()))
	RGWDebugLog("payload size of chunk of: " .. full_name .. " is: " .. #Data)

