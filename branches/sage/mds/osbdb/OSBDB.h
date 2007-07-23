// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/* OSBDB.h -- ObjectStore on Berkeley DB. -*- c++ -*-
   Copyright (C) 2007 Casey Marshall <csm@soe.ucsc.edu>

Ceph - scalable distributed file system

This is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License version 2.1, as published by the Free Software 
Foundation.  See file COPYING. */


#include <db_cxx.h>
#include "osd/ObjectStore.h"

#define OSBDB_MAGIC 0x05BDB

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

inline ostream& operator<<(ostream& out, const stored_superblock sb)
{
  out << "osbdb.super(" << sb.version << ")" << endl;
  return out;
}

/**
 * An object identifier; we define this so we can have a POD object to
 * work with.
 */
struct oid_t // POD
{
  char id[16];
};

inline void mkoid (oid_t& id, object_t& oid)
{
  // XXX byte order?
  memcpy (id.id, &oid, sizeof (oid_t));
}

inline ostream& operator<<(ostream& out, const oid_t id)
{
  for (int i = 0; i < 16; i++)
    {
      out.fill('0');
      out << setw(2) << hex << (id.id[i] & 0xFF);
      if ((i & 3) == 3)
        out << ':';
    }
  out.unsetf(ios::right);
  out << dec;
  return out;
}

/**
 * An "inode" key. We map a 'stored_object' struct to this key for
 * every object.
 */
struct object_inode_key // POD
{
  oid_t oid;
  char tag;
};

/**
 * "Constructor" for an object_inode_key.
 */
inline object_inode_key new_object_inode_key (object_t& oid)
{
  object_inode_key key;
  memset(&key, 0, sizeof (object_inode_key));
  mkoid (key.oid, oid);
  key.tag = 'i';
  return key;
}

/*
 * We use this, instead of sizeof(), to try and guarantee that we
 * don't include the structure padding, if any.
 *
 * This *should* return 17: sizeof (oid_t) == 16; sizeof (char) == 1.
 */
inline size_t sizeof_object_inode_key()
{
  return offsetof(object_inode_key, tag) + sizeof (char);
}

             // Frank Poole: Unfortunately, that sounds a little
             //              like famous last words.
             //   -- 2001: A Space Odyssey

inline ostream& operator<<(ostream& out, const object_inode_key o)
{
  out << o.tag << "/" << o.oid;
  return out;
}

/**
 * A stored object. This is essentially the "inode" of the object,
 * containing things like the object's length. The object itself is
 * stored as-is, mapped by the 128-bit object ID.
 */
struct stored_object
{
  uint32_t length;
};

inline ostream& operator<<(ostream& out, const stored_object s)
{
  out << "inode(l:" << s.length << ")";
  return out;
}

/*
 * Key referencing the list of attribute names for an object. This is
 * simply the object's ID, with an additional character 'a' appended.
 */
struct attrs_id // POD
{
  oid_t oid;
  char tag;
};

/*
 * "Construtor" for attrs_id.
 */
inline struct attrs_id new_attrs_id (object_t& oid)
{
  attrs_id aid;
  memset (&aid, 0, sizeof (attrs_id));
  mkoid(aid.oid, oid);
  aid.tag = 'a';
  return aid;
}

/*
 * See explanation for sizeof_object_inode_id.
 */
inline size_t sizeof_attrs_id()
{
  return offsetof(struct attrs_id, tag) + sizeof (char);
}

inline ostream& operator<<(ostream& out, const attrs_id id)
{
  out << id.tag << "/" << id.oid;
  return out;
}

/*
 * Encapsulation of a single attribute name.
 */
struct attr_name // POD
{
  char name[OSBDB_MAX_ATTR_LEN];
};

inline ostream& operator<<(ostream& out, const attr_name n)
{
  out << n.name;
  return out;
}

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
  std::cerr << n1.name << " == " << n2.name << "?" << endl;
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

inline ostream& operator<<(ostream& out, const stored_attrs *sa)
{
  out << sa->count << " [ ";
  for (unsigned i = 0; i < sa->count; i++)
    out << sa->names[i] << (i == sa->count - 1 ? " " : ", ");
  out << "]";
  return out;
}

/*
 * An object attribute key. An object attribute is mapped simply by
 * the object ID appended with the attribute name. Attribute names
 * may not be empty, and must be less than 256 characters, in this
 * implementation.
 */
struct attr_id // POD
{
  oid_t oid;
  attr_name name;
};

inline attr_id new_attr_id (object_t& oid, const char *name)
{
  attr_id aid;
  memset(&aid, 0, sizeof (attr_id));
  mkoid (aid.oid, oid);
  strncpy (aid.name.name, name, OSBDB_MAX_ATTR_LEN);
  return aid;
}

inline ostream& operator<<(ostream &out, const attr_id id)
{
  out << id.oid << ":" << id.name;
  return out;
}

/*
 * A key for a collection attributes list.
 */
struct coll_attrs_id // POD
{
  coll_t cid;
  char tag;
};

inline coll_attrs_id new_coll_attrs_id (coll_t cid)
{
  coll_attrs_id catts;
  memset(&catts, 0, sizeof (coll_attrs_id));
  catts.cid = cid;
  catts.tag = 'C';
  return catts;
}

inline size_t sizeof_coll_attrs_id()
{
  return offsetof(coll_attrs_id, tag) + sizeof (char);
}

inline ostream& operator<<(ostream& out, coll_attrs_id id)
{
  out << id.tag << "/" << id.cid;
  return out;
}

/*
 * A collection attribute key. Similar to 
 */
struct coll_attr_id // POD
{
  coll_t cid;
  attr_name name;
};

inline coll_attr_id new_coll_attr_id (coll_t cid, const char *name)
{
  coll_attr_id catt;
  memset(&catt, 0, sizeof (coll_attr_id));
  catt.cid = cid;
  strncpy (catt.name.name, name, OSBDB_MAX_ATTR_LEN);
  return catt;
}

inline ostream& operator<<(ostream& out, coll_attr_id id)
{
  out << id.cid << ":" << id.name;
  return out;
}

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

inline ostream& operator<<(ostream& out, stored_colls *c)
{
  out << c->count << " [ ";
  for (unsigned i = 0; i < c->count; i++)
    {
      out << hex << c->colls[i];
      if (i < c->count - 1)
        out << ", ";
    }
  out << " ]" << dec;
  return out;
}

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

inline ostream& operator<<(ostream& out, stored_coll *c)
{
  out << c->count << " [ ";
  for (unsigned i = 0; i < c->count; i++)
    {
      out << c->objects[i];
      if (i < c->count - 1)
        out << ", ";
    }
  out << " ]";
  return out;
}

class OSBDBException : public std::exception
{
  const char *msg;

public:
  OSBDBException(const char *msg) : msg(msg) { }
  const char *what() { return msg; }
};

/*
 * The object store interface for Berkeley DB.
 */
class OSBDB : public ObjectStore
{
 private:
  Mutex lock;
  DbEnv *env;
  Db *db;
  string device;
  string env_dir;
  bool mounted;
  bool opened;
  bool transactional;

 public:

  OSBDB(const char *dev) throw(OSBDBException)
    : lock(true), env(0), db (0), device (dev), mounted(false), opened(false),
      transactional(g_conf.bdbstore_transactional)
  {
  }

  ~OSBDB()
  {
    if (mounted)
      {
        umount();
      }
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
  int getattr(object_t oid, const char *name,
              void *value, size_t size);
  int getattrs(object_t oid, map<string,bufferptr>& aset);
  int rmattr(object_t oid, const char *name,
             Context *onsafe=0);
  int listattr(object_t oid, char *attrs, size_t size);

  int clone(object_t oid, object_t noid);
  
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
  int opendb (DBTYPE type=DB_UNKNOWN, int flags=0, bool new_env=false);

  int _setattr(object_t oid, const char *name, const void *value,
               size_t size, Context *onsync, DbTxn *txn);
  int _getattr(object_t oid, const char *name, void *value, size_t size);
  DbEnv *getenv();
};
