=========================
 CephFS delayed deletion
=========================

When you delete a file, the data is not immediately removed. Each
object in the file needs to be removed independently, and sending
``size_of_file / stripe_size * replication_count`` messages would slow
the client down too much, and use a too much of the clients
bandwidth. Additionally, snapshots may mean some objects should not be
deleted.

Instead, the file is marked as deleted on the MDS, and deleted lazily.
