# /etc/security/limits.d/ceph
#
#<domain>        <type>  <item>  <value>
#

# We want a very large value for nofile for the ceph user as the ceph
# clients and daemons consume lots and lots of file descriptors.

ceph	  -	      nofile  4194304
