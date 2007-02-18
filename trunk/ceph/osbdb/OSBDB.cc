/* OSBDB.cc -- ObjectStore on top of Berkeley DB.
   Copyright (C) 2007 Casey Marshall <csm@soe.ucsc.edu>

Ceph - scalable distributed file system

This is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License version 2.1, as published by the Free Software 
Foundation.  See file COPYING. */


#include <cerrno>
#include "OSBDB.h"

using namespace std;

#undef dout
#define dout(x) if (x <= g_conf.debug_bdbstore) cout << "bdbstore(" << device << ")."
#undef derr
#define derr(x) if (x <= g_conf.debug_bdbstore) cerr << "bdbstore(" << device << ")."

 // Utilities.

// Starting off with my own bsearch; mail reader to follow...

// Perform a binary search on a sorted array, returning the insertion
// point for key, or key if it is exactly found. In other words, this
// will return a pointer to the element that will come after key if
// key were to be inserted into the sorted array.
//
// Requires that T have < and > operators defined.
template<typename T>
uint32_t binary_search (T *array, size_t size, T key)
{
  int c = size >> 1;
  int i = c;

  while (c != 0)
    {
      c = c >> 1;
      if (array[i] < key)
        {
          i = i + c;
        }
      else if (array[i] > key)
        {
          i = i - c;
        }
      else
        return i;
    }

  if (array[i] < key)
    i++;
  return i;
} 

 // Management.

int OSBDB::mount()
{
  // XXX Do we want anything else in the superblock?

  Dbt key (OSBDB_SUPERBLOCK_KEY, 1);
  stored_superblock super;
  Dbt value (&super, sizeof (super));
  value.set_dlen (sizeof (super));
  value.set_ulen (sizeof (super));
  value.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
  if (db.get (NULL, &key, &value, 0) != 0)
    return -EINVAL; // XXX how to say "badly formed fs?"
  if (super.version != OSBDB_THIS_VERSION)
    return -EINVAL;

  DBTYPE t;
  db.get_type (&t);

  if (t == DB_BTREE)
    {
      u_int32_t minkey;
      db.get_bt_minkey (&minkey);
      dout(1) << "mounted version " << OSBDB_THIS_VERSION << "; Btree; "
              << "min keys per page: " << minkey << endl;
    }
  else
    {
      u_int32_t ffactor;
      u_int32_t nelem;
      db.get_h_ffactor (&ffactor);
      db.get_h_nelem (&nelem);
      dout(1) << "mounted version " << OSBDB_THIS_VERSION << "; Hash; "
              << "fill factor: " << ffactor
              << " table size: " << nelem << endl;
    }

  return 0;
}

int OSBDB::umount()
{
  // Anything?
  return 0;
}

int OSBDB::mkfs()
{
  uint32_t c;
  int ret = db.truncate (NULL, &c, 0);
  if (ret != 0)
    return -EIO; // ???

  Dbt key (OSBDB_SUPERBLOCK_KEY, 1);
  struct stored_superblock sb;
  sb.version = OSBDB_THIS_VERSION;
  Dbt value (&sb, sizeof (sb));

  if (db.put (NULL, &key, &value, 0) != 0)
    return -EIO; // ???

  return 0;
}

 // Objects.

int OSBDB::pick_object_revision_lt(object_t& oid)
{
  return -1; // FIXME
}

bool OSBDB::exists(object_t oid)
{
  struct stat st;
  return (stat (oid, &st) == 0);
}

int OSBDB::statfs (struct statfs *st)
{
  return 0;
}

int OSBDB::stat(object_t oid, struct stat *st)
{
  stored_object obj;
  Dbt key (&oid, sizeof (object_t));
  Dbt value (&obj, sizeof (obj));
  value.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
  value.set_ulen (sizeof (obj));

  if (db.get (NULL, &key, &value, 0) != 0)
    return -ENOENT;

  st->st_size = obj.length;
  return 0;
}

int OSBDB::remove(object_t oid, Context *onsafe)
{
  Dbt key (&oid, sizeof (object_t));
  db.del (NULL, &key, 0);

  // FIXME remove attributes for this object.

  return 0;
}

int OSBDB::truncate(object_t oid, off_t size, Context *onsafe)
{
  if (size > 0xFFFFFFFF)
    return -ENOSPC;

  stored_object obj;
  Dbt key (&oid, sizeof (oid));
  Dbt value (&obj, sizeof (obj));
  value.set_dlen (sizeof (obj));
  value.set_ulen (sizeof (obj));
  value.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);

  if (db.get (NULL, &key, &value, 0) != 0)
    {
      return -ENOENT;
    }

  if (obj.length < size)
    {
      char b[] = { '\0' };
      Dbt newVal (b, 1);
      newVal.set_doff ((size_t) size + offsetof (stored_object, data) - 1);
      newVal.set_dlen (1);
      newVal.set_ulen (1);
      newVal.set_flags (DB_DBT_PARTIAL);
      if (db.put (NULL, &key, &newVal, 0) != 0)
        {
          return -EIO;
        }

      obj.length = size;
      value.set_doff (0);
      value.set_dlen (sizeof (off_t));
      value.set_ulen (sizeof (off_t));
      value.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
      if (db.put (NULL, &key, &value, 0) != 0)
        {
          return -EIO;
        }
    }
  else if (obj.length > size)
    {
      if (size == 0)
        {
          obj.length = 0;
          Dbt tval (&obj, sizeof (obj));
          tval.set_ulen (sizeof (obj));
          tval.set_flags (DB_DBT_USERMEM);
          if (db.put (NULL, &key, &tval, 0) != 0)
            {
              return -EIO;
            }
        }
      else if (size < 2 * 1024 * 1024) // XXX
        {
          size_t newSz = sizeof (stored_object) + (size_t) size;
          auto_ptr<stored_object> obj2 ((stored_object *) malloc (newSz));
          stored_object *objp = obj2.get();
          Dbt tval (objp, newSz);
          tval.set_doff (0);
          tval.set_dlen (newSz);
          tval.set_ulen (newSz);
          tval.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
          if (db.get (NULL, &key, &tval, 0) != 0)
            {
              return -EIO;
            }

          objp->length = size;
          tval.set_ulen (newSz);
          tval.set_size (newSz);
          tval.set_flags (DB_DBT_USERMEM);
          if (db.put (NULL, &key, &tval, 0) != 0)
            {
              return -EIO;
            }
        }
      else
        {
          // XXX FIXME
          std::cerr << "WARNING: punting on truncating file to size "
                    << size << std::endl;
          obj.length = size;
          value.set_doff (0);
          value.set_dlen (sizeof (off_t));
          value.set_ulen (sizeof (off_t));
          value.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
          db.put (NULL, &key, &value, 0);
        }
    }

  return 0;
}

int OSBDB::read(object_t oid, off_t offset, size_t len, bufferlist& bl)
{
  stored_object obj;
  Dbt key (&oid, sizeof (oid));
  Dbt value (&obj, sizeof (obj));

  value.set_ulen (sizeof (obj));
  value.set_dlen (sizeof (obj));
  value.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
  
  if (db.get (NULL, &key, &value, 0) != 0)
    {
      return -ENOENT;
    }

  if (offset >= obj.length)
    return 0;

  char *buf = bl.c_str();
  Dbt dval (buf, len);
  dval.set_doff (offsetof (stored_object, data) + (size_t) offset);
  dval.set_dlen (len);
  dval.set_ulen (len);
  dval.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
  if (db.get (NULL, &key, &dval, 0) != 0)
    {
      return -EIO;
    }

  return dval.get_size();
}

int OSBDB::write(object_t oid, off_t offset, size_t len,
                 bufferlist& bl, Context *onsafe)
{
  stored_object obj;
  Dbt key (&oid, sizeof (oid));
  Dbt value (&obj, sizeof (obj));

  if (db.get (NULL, &key, &value, 0) != 0)
    {
      obj.length = 0;
    }

  if (offset + len > obj.length)
    {
      obj.length = (size_t) offset + len;
      value.set_dlen (offsetof (stored_object, data));
      value.set_ulen (offsetof (stored_object, data));
      value.set_flags (DB_DBT_PARTIAL);
      if (db.put (NULL, &key, &value, 0) != 0)
        {
          return -EIO;
        }
    }

  char *buf = bl.c_str();
  Dbt dval (buf, len);
  dval.set_doff (offsetof (stored_object, data) + (size_t) offset);
  dval.set_dlen (len);
  dval.set_ulen (len);
  dval.set_flags (DB_DBT_PARTIAL);
  if (db.put (NULL, &key, &dval, 0) != 0)
    {
      return -EIO;
    }

  return len;
}

int OSBDB::clone(object_t oid, object_t noid)
{
  struct stored_object obj;

  if (exists (noid))
    return -EEXIST;

  Dbt key (&oid, sizeof (oid));
  Dbt value (&obj, sizeof (obj));
  Dbt newKey (&noid, sizeof (noid));

  value.set_ulen (sizeof (obj));
  value.set_dlen (sizeof (obj));
  if (db.get (NULL, &key, &value, 0) != 0)
    return -ENOENT;

  db.put (NULL, &newKey, &value, 0);

  auto_ptr<char> buf (new char[4096]);
  for (size_t o = 0; o < obj.length; o += 4096)
    {
      Dbt block (buf.get(), 4096);
      block.set_ulen (4096);
      block.set_doff (offsetof (stored_object, data) + o);
      block.set_dlen (4096);
      block.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
      if (db.get (NULL, &key, &block, 0) != 0)
        {
          return -EIO;
        }

      block.set_flags (DB_DBT_PARTIAL);
      if (db.put (NULL, &newKey, &block, 0) != 0)
        {
          return -EIO;
        }
    }

  return 0;
}

 // Collections

int OSBDB::list_collections(list<coll_t>& ls)
{
  Dbt key (COLLECTIONS_KEY, 1);
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  if (db.get (NULL, &key, &value, 0) != 0)
    return 0; // no collections.

  auto_ptr<stored_colls> sc ((stored_colls *) value.get_data());
  stored_colls *scp = sc.get();
  for (uint32_t i = 0; i < sc->count; i++)
    ls.push_back (scp->colls[i]);

  return scp->count;
}

int OSBDB::create_collection(coll_t c, Context *onsafe)
{
  Dbt key (COLLECTIONS_KEY, 1);
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  stored_colls *scp = NULL;
  size_t sz = 0;
  bool created = false;
  if (db.get (NULL, &key, &value, 0) != 0)
    {
      sz = sizeof (stored_colls) + sizeof (coll_t);
      scp = (stored_colls *) malloc (sz);
      scp->count = 0;
      created = true;
    }
  else
    {
      scp = (stored_colls *) value.get_data();
      sz = value.get_size();
    }

  auto_ptr<stored_colls> sc (scp);
  int ins = 0;
  if (scp->count > 0)
    ins = binary_search<coll_t> (scp->colls, scp->count, c);
  if (scp->colls[ins] == c)
    return -EEXIST;

  // Make room for a new collection ID.
  if (!created)
  {
    sz += sizeof (coll_t);
    stored_colls *scp2 = (stored_colls *) realloc (scp, sz);
    sc.reset (scp2);
    scp = scp2;
  }

  memmove (&scp->colls[ins], &scp->colls[ins + 1],
           (scp->count - ins) * sizeof (coll_t));
  scp->colls[ins] = c;

  // Put the modified collection list back.
  {
    Dbt value2 (scp, sz);
    if (db.put (NULL, &key, &value, 0) != 0)
      {
        return -EIO;
      }
  }

  // Create the new collection.
  {
    stored_coll new_coll;
    new_coll.count = 0;
    Dbt coll_key (&c, sizeof (coll_t));
    Dbt coll_value (&new_coll, sizeof (stored_coll));
    if (db.put (NULL, &coll_key, &coll_value, 0) != 0)
      {
        return -EIO;
      }
  }

  return 0;
}

int OSBDB::destroy_collection(coll_t c, Context *onsafe)
{
  Dbt key (COLLECTIONS_KEY, 1);
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  if (db.get (NULL, &key, &value, 0) != 0)
    {
      return -ENOENT; // XXX
    }

  stored_colls *scp = (stored_colls *) value.get_data();
  auto_ptr<stored_colls> valueBuf (scp);
  if (scp->count == 0)
    {
      return -ENOENT;
    }
  uint32_t ins = binary_search<coll_t> (scp->colls, scp->count, c);
  if (scp->colls[ins] != c)
    {
      return -ENOENT;
    }

  // Move the rest of the list down in memory, if needed.
  if (ins < scp->count - 1)
    {
      size_t n = scp->count - ins - 1;
      memmove (&scp->colls[ins], &scp->colls[ins + 1], n);
    }

  // Modify the record size to be one less.
  Dbt nvalue (scp, value.get_size() - sizeof (coll_t));
  nvalue.set_flags (DB_DBT_USERMEM);
  if (db.put (NULL, &key, &nvalue, 0) != 0)
    {
      return -EIO;
    }

  // Delete the collection.
  Dbt collKey (&c, sizeof (coll_t));
  if (db.del (NULL, &collKey, 0) != 0)
    {
      return -EIO;
    }

  return 0;
}

bool OSBDB::collection_exists(coll_t c)
{
  Dbt key (COLLECTIONS_KEY, 1);
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  if (db.get (NULL, &key, &value, 0) != 0)
    return false;

  stored_colls *scp = (stored_colls *) value.get_data();
  auto_ptr<stored_colls> sc (scp);
  if (scp->count == 0)
    return false;
  uint32_t ins = binary_search<coll_t> (scp->colls, scp->count, c);

  return (scp->colls[ins] == c);
}

int OSBDB::collection_stat(coll_t c, struct stat *st)
{
  return -ENOSYS;
}

int OSBDB::collection_add(coll_t c, object_t o, Context *onsafe)
{
  Dbt key (&c, sizeof (coll_t));
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  if (db.get (NULL, &key, &value, 0) != 0)
    {
      return -ENOENT;
    }

  size_t sz = value.get_size();
  stored_coll *scp = (stored_coll *) value.get_data();
  auto_ptr<stored_coll> sc (scp);

  // Find the insertion point for the new object ID.
  uint32_t ins = 0;
  if (scp->count > 0)
    {
      ins = binary_search<object_t> (scp->objects, scp->count, o);
      // Already there?
      if (scp->objects[ins] == o)
        {
          return -EEXIST;
        }
    }

  // Make room for the new value, and add it.
  sz += sizeof (object_t);
  scp = (stored_coll *) realloc (scp, sz);
  sc.reset (scp);
  if (ins < scp->count)
    {
      size_t n = (scp->count - ins - 1) * sizeof (object_t);
      memmove (&scp->objects[ins + 1], &scp->objects[ins], n);
    }
  scp->count++;
  scp->objects[ins] = o;

  Dbt nvalue (scp, sz);
  if (db.put (NULL, &key, &nvalue, 0) != 0)
    {
      return -EIO;
    }

  return 0;
}

int OSBDB::collection_remove(coll_t c, object_t o, Context *onsafe)
{
  Dbt key (&c, sizeof (coll_t));
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  if (db.get (NULL, &key, &value, 0) != 0)
    {
      return -ENOENT;
    }

  stored_coll *scp = (stored_coll *) value.get_data();
  auto_ptr<stored_coll> sc (scp);

  if (scp->count == 0)
    {
      return -ENOENT;
    }
  uint32_t ins = binary_search<object_t> (scp->objects, scp->count, o);
  if (scp->objects[ins] != o)
    {
      return -ENOENT;
    }

  if (ins < scp->count - 1)
    {
      size_t n = (scp->count - ins - 1) * sizeof (object_t);
      memmove (&scp->objects[ins], &scp->objects[ins + 1], n);
    }
  scp->count--;

  Dbt nval (scp, value.get_size() - sizeof (object_t));
  if (db.put (NULL, &key, &nval, 0) != 0)
    {
      return -EIO;
    }

  return 0;
}

int OSBDB::collection_list(coll_t c, list<object_t>& o)
{
  Dbt key (&c, sizeof (coll_t));
  Dbt value;
  if (db.get (NULL, &key, &value, 0) != 0)
    return -ENOENT;

  stored_coll *scp = (stored_coll *) value.get_data();
  auto_ptr<stored_coll> sc (scp);
  for (uint32_t i = 0; i < scp->count; i++)
    o.push_back (scp->objects[i]);

  return 0;
}

 // Attributes

int OSBDB::_setattr(object_t oid, const char *name,
                    const void *value, size_t size, Context *onsafe)
{
  if (strlen (name) >= OSBDB_MAX_ATTR_LEN)
    return -ENAMETOOLONG;

  // Add name to attribute list, if needed.
  attrs_id aids;
  aids.oid = oid;
  Dbt attrs_key (&aids, sizeof (aids));
  Dbt attrs_val;
  attrs_val.set_flags (DB_DBT_MALLOC);
  stored_attrs *sap = NULL;
  size_t sz = 0;
  if (db.get (NULL, &attrs_key, &attrs_val, 0) != 0)
    {
      sz = sizeof (stored_attrs);
      sap = new stored_attrs;
      sap->count = 0;
    }
  else
    {
      sz = attrs_val.get_size();
      sap = (stored_attrs *) attrs_val.get_data();
    }
  auto_ptr<stored_attrs> sa (sap);

  attr_name _name;
  strncpy (_name.name, name, OSBDB_MAX_ATTR_LEN);

  int ins = 0;
  if (sap->count > 0)
    ins = binary_search<attr_name> (sap->names, sap->count, _name);
  if (sap->count == 0 || sap->names[ins] != _name)
    {
      sz += sizeof (attr_name);
      sap = (stored_attrs *) realloc (sap, sz);
      sa.reset (sap);
      size_t n = (sap->count - ins - 1) * sizeof (attr_name);
      if (n > 0)
        {
          memmove (&sap->names[ins+1], &sap->names[ins], n);
        }
      sap->names[ins] = _name;
      sap->count++;

      Dbt newAttrs_val (sap, sz);
      newAttrs_val.set_ulen (sz);
      newAttrs_val.set_flags (DB_DBT_USERMEM);
      if (db.put (NULL, &attrs_key, &newAttrs_val, 0) != 0)
        return -EIO;
    }

  // Add the attribute.
  attr_id aid;
  aid.oid = oid;
  strncpy (aid.name.name, name, OSBDB_MAX_ATTR_LEN);
  Dbt attr_key (&aid, sizeof (aid));
  Dbt attr_val ((void *) value, size);
  if (db.put (NULL, &attr_key, &attr_val, 0) != 0)
    return -EIO;

  return 0;
}

int OSBDB::setattr(object_t oid, const char *name,
                   const void *value, size_t size,
                   Context *onsafe)
{
  int ret = _setattr (oid, name, value, size, onsafe);
  return ret;
}

int OSBDB::setattrs(object_t oid, map<string,bufferptr>& aset,
                    Context *onsafe)
{
  map<string,bufferptr>::iterator it;
  for (it = aset.begin(); it != aset.end(); it++)
    {
      string name = it->first;
      bufferptr value = it->second;
      int ret = _setattr (oid, name.c_str(), value.c_str(),
                          value.length(), onsafe);
      if (ret != 0)
        {
          return ret;
        }
    }
  return 0;
}

int OSBDB::listattr(object_t oid, char *attrs, size_t size)
{
  attrs_id aids;
  aids.oid = oid;
  Dbt key (&aids, sizeof (aids));
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  if (!db.get (NULL, &key, &value, 0) != 0)
    return -ENOENT;

  stored_attrs *attrsp = (stored_attrs *) value.get_data();
  auto_ptr<stored_attrs> _attrs (attrsp);
  size_t s = 0;
  char *p = attrs;
  for (unsigned i = 0; i < attrsp->count && s < size; i++)
    {
      int n = MIN (OSBDB_MAX_ATTR_LEN,
                   MIN (strlen (attrsp->names[i].name), size - s));
      strncpy (p, attrsp->names[i].name, n);
      p = p + n;
    }
  return 0;
}

 // Collection attributes.

int OSBDB::collection_setattr(coll_t cid, const char *name,
                              const void *value, size_t size,
                              Context *onsafe)
{
  return -ENOSYS;
}

int OSBDB::collection_rmattr(coll_t cid, const char *name,
                             Context *onsafe)
{
  return -ENOSYS;
}

int OSBDB::collection_getattr(coll_t cid, const char *name,
                              void *value, size_t size)
{
  return -ENOSYS;
}

int OSBDB::collection_listattr(coll_t cid, char *attrs, size_t size)
{
  return -ENOSYS;
}

 // Sync.

void OSBDB::sync (Context *onsync)
{
  sync();
  // huh?
}

void OSBDB::sync()
{
  db.sync(0);
}
