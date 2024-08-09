==================
 LibradosPP (C++)
==================

.. note:: The librados C++ API is not guaranteed to be API+ABI stable
   between major releases. All applications using the librados C++ API must
   be recompiled and relinked against a specific Ceph release.


// Assisted by WCA@IBM// Latest GenAI contribution: ibm/granite-20b-code-instruct-v2

Purpose
=======
The ObjectReadOperation class is a utility class that provides methods for reading data from an object in Ceph. It is used as a parameter to the read() method of the ObjectReadOperation class.

Usage
=====
To use the ObjectReadOperation class, you need to create an instance of it and call its various methods to specify the operations you want to perform on the object. Once you have created the ObjectReadOperation instance, you can pass it as a parameter to the read() method of the ObjectWriteOperation class.

For example, to read the contents of an object into a bufferlist object::

 // Create an ObjectReadOperation instance
 ObjectReadOperation op;
 // Read the contents of the object into a bufferlist
 int r = ioctx.read("object_name", op, &bl, sizeof(bl), 0);

Available Methods
=================
This method retrieves information about the object, including its size and modification time. The information is stored in the provided pointers. ::

 void stat(uint64_t *psize, time_t *pmtime, int *prval);
This method retrieves information about the object, including its size and modification time. The information is stored in the provided pointers. ::

 void stat2(uint64_t *psize, struct timespec *pts, int *prval);
This method retrieves the value of an extended attribute for the object. The name of the attribute and the value are stored in the provided bufferlist objects. ::

 void getxattr(const char *name, bufferlist *pbl, int *prval);

This method retrieves all the extended attributes for the object and stores them in a map. The names and values of the attributes are stored in the provided bufferlist objects. ::

 void getxattrs(std::map<std::string, bufferlist> *pattrs, int *prval);

This method reads data from the object and stores it in a bufferlist object. The off parameter specifies the offset from which to start reading, and the len parameter specifies the number of bytes to read. ::

 void read(size_t off, uint64_t len, bufferlist *pbl, int *prval);

This method calculates a checksum for the data in the object. The type parameter specifies the type of checksum to calculate, and the init_value_bl parameter specifies the initial value for the checksum calculation. The off, len, chunk_size, and pbl parameters specify the range of data to calculate the checksum for and store the result in, respectively. ::

 void checksum(rados_checksum_type_t type, const bufferlist &init_value_bl, uint64_t off, size_t len, size_t chunk_size, bufferlist *pbl, int *prval);

This method reads data from the object and stores it in a bufferlist object. The off parameter specifies the offset from which to start reading, and the len parameter specifies the number of bytes to read. The m parameter is a map that maps the offset of each chunk read to its length. ::

 void sparse_read(uint64_t off, uint64_t len, std::map<uint64_t,uint64_t> *m, bufferlist *data_bl, int *prval, uint64_t truncate_size = 0, uint32_t truncate_seq = 0);

This method retrieves key-value pairs from the object's OMAP. The start_after parameter specifies the key to start listing from, and the max_return parameter specifies the maximum number of key-value pairs to retrieve. The retrieved key-value pairs are stored in the provided map. ::

 void omap_get_vals(  const std::string &start_after,  uint64_t max_return,  std::map<std::string, bufferlist> *out_vals,  int *prval) __attribute__ ((deprecated));  // use v2

This method retrieves key-value pairs from the object's OMAP. The start_after parameter specifies the key to start listing from, and the max_return parameter specifies the maximum number of key-value pairs to retrieve. The retrieved key-value pairs are stored in the provided map. ::

 void omap_get_vals2(  const std::string &start_after,  uint64_t max_return,  std::map<std::string, bufferlist> *out_vals,  bool *pmore,  int *prval);

This method retrieves key-value pairs from the object's OMAP. The start_after parameter specifies the key to start listing from, the filter_prefix parameter specifies a prefix to filter by, and the max_return parameter specifies the maximum number of key-value pairs to retrieve. The retrieved key-value pairs are stored in the provided map. ::

 void omap_get_vals(  const std::string &start_after,  const std::string &filter_prefix,  uint64_t max_return,  std::map<std::string, bufferlist> *out_vals,  int *prval) __attribute__ ((deprecated));  // use v2

This method retrieves key-value pairs from the object's OMAP. The start_after parameter specifies the key to start listing from, the filter_prefix parameter specifies a prefix to filter by, and the max_return parameter specifies the maximum number of key-value pairs to retrieve. The retrieved key-value pairs are stored in the provided map. ::

 void omap_get_vals2(  const std::string &start_after,  const std::string &filter_prefix,  uint64_t max_return,  std::map<std::string, bufferlist> *out_vals,  bool *pmore,  int *prval);

This method retrieves keys from the object's OMAP. The start_after parameter specifies the key to start listing from, and the max_return parameter specifies the maximum number of keys to retrieve. The retrieved keys are stored in the provided set. ::

 void omap_get_keys(const std::string &start_after, uint64_t max_return, std::set<std::string> *out_keys, int *prval) __attribute__ ((deprecated)); // use v2

This method retrieves keys from the object's OMAP. The start_after parameter specifies the key to start listing from, and the max_return parameter specifies the maximum number of keys to retrieve. The retrieved keys are stored in the provided set. ::

 void omap_get_keys2(const std::string &start_after, uint64_t max_return, std::set<std::string> *out_keys, bool *pmore, int *prval);

This method retrieves the header from the object's OMAP. The header contains metadata about the object's OMAP. ::

 void omap_get_header(bufferlist *header, int *prval);

This method retrieves key-value pairs for the specified keys from the object's OMAP. The keys parameter specifies the keys to retrieve, and the retrieved key-value pairs are stored in the provided map. ::

 void omap_get_vals_by_keys(const std::set<std::string> &keys, std::map<std::string, bufferlist> *map, int *prval);

This method retrieves a list of watchers for the object. The retrieved watchers are stored in the provided list. ::

 void list_watchers(std::list<obj_watch_t> *out_watchers, int *prval);

This method retrieves a list of snapshot clones associated with a logical object. The retrieved snapshot clones are stored in the provided snap_set_t object. ::

 void list_snaps(snap_set_t *out_snaps, int *prval);

This method queries the dirty state of an object. The isdirty parameter is a pointer to a boolean that indicates whether the object is dirty or not. ::

 void is_dirty(bool *isdirty, int *prval);

This method flushes a cache tier object to its backing tier. This method blocks racing updates. ::

 void cache_flush();

This method flushes a cache tier object to its backing tier. This method returns immediately if another thread is already flushing the object. ::

 void cache_try_flush();

This method evicts a clean cache tier object. ::

 void cache_evict();

This method sets a chunk pointing a part of the source object at the target object. ::

 void set_chunk(uint64_t src_offset, uint64_t src_length, const IoCtx& tgt_ioctx, std::string tgt_oid, uint64_t tgt_offset, int flag = 0);

This method flushes a manifest tier object to its backing tier, performing deduplication. This method blocks racing updates. ::

 void tier_flush();

This method evicts a manifest tier object to its backing tier. This method blocks racing updates.

 void tier_evict();
