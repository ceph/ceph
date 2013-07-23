Porting To New Platforms
========================

Running Ceph on new platforms requires porting platform-specific functionality
to the target platform.

Get Process Name
----------------

Lookup the name of the running process.

Supported:

On Linux since 2.6.11 ``prctl(2)`` can be used with the ``PR_GET_NAME`` flag.

Platform References:

- Darwin: http://stackoverflow.com/questions/3018054/retrieve-names-of-running-processes
- Darwin: http://stackoverflow.com/questions/12273546/get-name-from-pid
