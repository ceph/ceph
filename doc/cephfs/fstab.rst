==========================================
 Mount Ceph FS in your File Systems Table
==========================================

If you mount Ceph FS in your file systems table, the Ceph file system will mount 
automatically on startup. To mount Ceph FS in your file systems table, add the 
following to ``/etc/fstab``::

	{ipaddress}:{port}:/ {mount}/{mountpoint} {filesystem-name}	[name=username,secret=secretkey|secretfile=/path/to/secretfile],[{mount.options}]

For example:: 

	10.10.10.10:6789:/     /mnt/ceph    ceph    name=admin,secretfile=/etc/ceph/secret.key,noauto,rw,noexec,nodev,noatime,nodiratime    0       2
	
.. important:: The ``name`` and ``secret`` or ``secretfile`` options are 
   mandatory when you have Ceph authentication running. See `Authentication`_
   for details.
   
   .. _Authentication: ../../rados/operations/authentication/