/* OSBDB.h -- ObjectStore on Berkeley DB. -*- c++ -*-
   Copyright (C) 2007 Casey Marshall <csm@soe.ucsc.edu>

Ceph - scalable distributed file system

This is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License version 2.1, as published by the Free Software 
Foundation.  See file COPYING. */


#include <db_cxx.h>
#include "osd/ObjectStore.h"

// Redefine this to use a different BDB access type. DB_BTREE is
// probably the only other one that makes sense.
#ifndef OSBDB_DB_TYPE
#define OSBDB_DB_TYPE DB_HASH
#endif // OSBDB_DB_TYPE

/*
 * Maximum length of an attribute name.
 */
#define OSBDB_MAX_ATTR_LEN 256

#define OSBDB_THIS_VERSION 1

#define OSBDB_SUPERBLOCK_KEY ((void *) "s")

/*
 * The "superblock" of the BDB object store. We store one of these in
 * the DB, to store version and other information. We don't record
 * anything special here, just the version number the database was
 * written with.
 *
 * In principle, this structure is variable-length, depending on the
 * software version writing the superblock.
 */
struct stored_superblock
{
  uint32_t version;
};

/**
 * A stored object. These are mapped by the raw 128-bit object ID, and
 * include the length of the object.
 */
struct stored_object
{
  uint32_t length;
  char data[0];    // Actually variable-length
};

/*
 * Key referencing the list of attribute names for an object. This is
 * simply the object's ID, with an additional character 'a' appended.
 */
struct attrs_id
{
  attrs_id() : pad('a') { }

  object_t oid;
  const char pad;
};

/*
 * Encapsulation of a single attribute name.
 */
struct attr_name
{
  char name[OSBDB_MAX_ATTR_LEN];
};

inline bool operator<(const attr_name n1, const attr_name n2)
{
  return (strncmp (n1.name, n2.name, OSBDB_MAX_ATTR_LEN) < 0);
}

inline bool operator>(const attr_name n1, const attr_name n2)
{
  return (strncmp (n1.name, n2.name, OSBDB_MAX_ATTR_LEN) > 0);
}

inline bool operator==(const attr_name n1, const attr_name n2)
{
  return (strncmp (n1.name, n2.name, OSBDB_MAX_ATTR_LEN) == 0);
}

inline bool operator!=(const attr_name n1, const attr_name n2)
{
  return !(n1 == n2);
}

inline bool operator>=(const attr_name n1, const attr_name n2)
{
  return !(n1 < n2);
}

inline bool operator<=(const attr_name n1, const attr_name n2)
{
  return !(n1 > n2);
}

/*
 * A list of an object or collection's attribute names.
 */
struct stored_attrs
{
  uint32_t count;
  attr_name names[0];   // actually variable-length
};

/*
 * An object attribute key. An object attribute is mapped simply by
 * the object ID appended with the attribute name. Attribute names
 * may not be empty, and must be less than 256 characters, in this
 * implementation.
 */
struct attr_id
{
  object_t oid;
  attr_name name;
};

/*
 * A collection attribute key. Similar to 
 */
struct coll_attr_id
{
  coll_t cid;
  attr_name name;
};

/*
 * This is the key we store the master collections list under.
 */
#define COLLECTIONS_KEY ((void *) "c")

/*
 * The master list of collections. There should be one of these per
 * OSD. The sole reason for this structure is to have the ability
 * to enumerate all collections stored on this OSD.
 */
struct stored_colls
{
  // The number of collections.
  uint32_t count;

  // The collection identifiers. This is a sorted list of coll_t
  // values.
  coll_t colls[0]; // actually variable-length
};

/*
 * A stored collection (a bag of object IDs). These are referenced by
 * the bare collection identifier type, a coll_t (thus, a 32-bit
 * integer). Internally this is stored as a sorted list of object IDs.
 *
 * Note, this structure places all collection items in a single
 * record; this may be a memory burden for large collections.
 */
struct stored_coll
{
  // The size of this collection.
  uint32_t count;

  // The object IDs in this collection. This is a sorted list of all
  // object ID's in this collection.
  object_t objects[0]; // actually variable-length
};

/*
 * The object store interface for Berkeley DB.
 */
class OSBDB : public ObjectStore
{
 private:
  //  DbEnv env;
  Db db;
  string device;

 public:

  OSBDB(const char *dev) : db (NULL, 0), device (dev)
  {
    if (!g_conf.bdbstore_btree)
      {
        if (g_conf.bdbstore_pagesize > 0)
          db.set_pagesize (g_conf.bdbstore_pagesize);
        if (g_conf.bdbstore_ffactor > 0 && g_conf.bdbstore_nelem > 0)
          {
            db.set_h_ffactor (g_conf.bdbstore_ffactor);
            db.set_h_nelem (g_conf.bdbstore_nelem);
          }
      }
    if (db.open (NULL, dev, "OSD", (g_conf.bdbstore_btree ? DB_BTREE : DB_HASH),
                 DB_CREATE, 0) != 0)
      {
        std::cerr << "failed to open database" << std::endl;
        ::abort();
      }
  }

  ~OSBDB()
  {
    db.close (0);
  }

  int mount();
  int umount();
  int mkfs();

  int statfs(struct statfs *buf);

  int pick_object_revision_lt(object_t& oid);

  bool exists(object_t oid);
  int stat(object_t oid, struct stat *st);

  int remove(object_t oid, Context *onsafe=0);

  int truncate(object_t oid, off_t size, Context *onsafe=0);

  int read(object_t oid, off_t offset, size_t len,
           bufferlist& bl);
  int write(object_t oid, off_t offset, size_t len,
            bufferlist& bl, Context *onsafe);

  int setattr(object_t oid, const char *name,
              const void *value, size_t size, Context *onsafe=0);
  int setattrs(object_t oid, map<string,bufferptr>& aset,
               Context *onsafe=0);
  int clone(object_t oid, object_t noid);

  int listattr(object_t oid, char *attrs, size_t size);
  
  // Collections.
  
  int list_collections(list<coll_t>& ls);
  int create_collection(coll_t c, Context *onsafe=0);
  int destroy_collection(coll_t c, Context *onsafe=0);
  bool collection_exists(coll_t c);
  int collection_stat(coll_t c, struct stat *st);
  int collection_add(coll_t c, object_t o, Context *onsafe=0);
  int collection_remove(coll_t c, object_t o, Context *onsafe=0);
  int collection_list(coll_t c, list<object_t>& o);

  int collection_setattr(coll_t cid, const char *name,
                         const void *value, size_t size,
                         Context *onsafe=0);
  int collection_rmattr(coll_t cid, const char *name,
                        Context *onsafe=0);
  int collection_getattr(coll_t cid, const char *name,
                         void *value, size_t size);
  int collection_listattr(coll_t cid, char *attrs, size_t size);

  void sync(Context *onsync);
  void sync();

private:
  int _setattr(object_t oid, const char *name, const void *value,
               size_t size, Context *onsync);
};
