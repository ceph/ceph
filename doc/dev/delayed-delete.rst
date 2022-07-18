=========================
 CephFS delayed deletion
=========================

The deletion of a file does not immediately remove its data. Each of the file's
underlying objects must be removed independently. If these objects were removed 
immediately, the client would have to send ``size_of_file / stripe_size *
replication_count`` messages. This would consume significant bandwith and would
slow the client unacceptably. If snapshots exist, their existence can prevent
the deletion of objects associated with them.

In these cases, such files are (1) marked as deleted on the MDS and (2) deleted
lazily.
