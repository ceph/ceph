// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/* OSBDB.cc -- ObjectStore on top of Berkeley DB.
   Copyright (C) 2007 Casey Marshall <csm@soe.ucsc.edu>

Ceph - scalable distributed file system

This is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License version 2.1, as published by the Free Software 
Foundation.  See file COPYING. */


#include <map>
#include <string>
#include <cerrno>
#include "OSBDB.h"
#include "common/Timer.h"

using namespace std;

#define dout_prefix *_dout << "bdbstore(" << device << ")@" << __LINE__ << "."
#define derr(x) dout(x)

#define CLEANUP(onsafe) do { \
    dout(6) << "DELETE " << hex << onsafe << dec << dendl; \
    delete onsafe; \
  } while (0)
#define COMMIT(onsafe) do { \
    dout(6) << "COMMIT " << hex << onsafe << dec << dendl; \
    sync(onsafe); \
  } while (0)

 // Have a lock, already.

class scoped_lock
{
private:
  Mutex *m;
public:
  scoped_lock(Mutex *m) : m(m) { m->Lock(); }
  ~scoped_lock() { m->Unlock(); }
};

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
  int low = 0;
  int high = size;
  int p = (low + high) / 2;

  while (low < high - 1)
    {
      if (array[p] > key)
        {
          high = p;
        }
      else if (array[p] < key)
        {
          low = p;
        }
      else
        return p;

      p = (low + high) / 2;
    }

  if (array[p] < key)
    p++;
  else if (array[p] > key && p > 0)
    p--;
  return p;
} 

 // Management.

DbEnv *OSBDB::getenv ()
{
  DbEnv *envp = new DbEnv (DB_CXX_NO_EXCEPTIONS);
  if (g_conf.debug > 1 || g_conf.debug_bdbstore > 1)
    envp->set_error_stream (&std::cerr);
  if (g_conf.debug > 2 || g_conf.debug_bdbstore > 2)
    envp->set_message_stream (&std::cout);
  envp->set_flags (DB_LOG_INMEMORY, 1);
  //env->set_flags (DB_DIRECT_DB, 1);
  int env_flags = (DB_CREATE
                   | DB_THREAD
                   //| DB_INIT_LOCK
                   | DB_INIT_MPOOL
                   //| DB_INIT_TXN
                   //| DB_INIT_LOG
                   | DB_PRIVATE);
  if (envp->open (NULL, env_flags, 0) != 0)
    {
      std::cerr << "failed to open environment " << std::endl;
      assert(0);
    }
  return envp;
}

int OSBDB::opendb(DBTYPE type, int flags, bool new_env)
{
  env = getenv();
  db = new Db(env, 0);
  db->set_error_stream (&std::cerr);
  db->set_message_stream (&std::cout);
  db->set_flags (0);
  if (!g_conf.bdbstore_btree)
    {
      if (g_conf.bdbstore_pagesize > 0)
        db->set_pagesize (g_conf.bdbstore_pagesize);
      if (g_conf.bdbstore_ffactor > 0 && g_conf.bdbstore_nelem > 0)
        {
          db->set_h_ffactor (g_conf.bdbstore_ffactor);
          db->set_h_nelem (g_conf.bdbstore_nelem);
        }
    }
  if (g_conf.bdbstore_cachesize > 0)
    {
      db->set_cachesize (0, g_conf.bdbstore_cachesize, 0);
    }

  flags = flags | DB_THREAD;
  if (transactional)
    flags = flags | DB_AUTO_COMMIT;

  int ret;
  if ((ret = db->open (NULL, device.c_str(), NULL, type, flags, 0)) != 0)
    {
      derr(1) << "failed to open database: " << device << ": "
              << db_strerror(ret) << std::dendl;
      return -EINVAL;
    }
  opened = true;
  return 0;
}

int OSBDB::mount()
{
  dout(2) << "mount " << device << dendl;

  if (mounted)
    {
      dout(4) << "..already mounted" << dendl;
      return 0;
    }

  if (!opened)
    {
      int ret;
      if ((ret = opendb ()) != 0)
        {
          dout(4) << "..returns " << ret << dendl;
          return ret;
        }
    }

  // XXX Do we want anything else in the superblock?

  Dbt key (OSBDB_SUPERBLOCK_KEY, 1);
  stored_superblock super;
  Dbt value (&super, sizeof (super));
  value.set_dlen (sizeof (super));
  value.set_ulen (sizeof (super));
  value.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);

  if (db->get (NULL, &key, &value, 0) != 0)
    {
      dout(4) << "..get superblock fails" << dendl;
      return -EINVAL; // XXX how to say "badly formed fs?"
    }

  dout(3) << ".mount " << super << dendl;

  if (super.version != OSBDB_THIS_VERSION)
    {
      dout(4) << "version mismatch (" << super.version << ")" << dendl;
      return -EINVAL;
    }

  DBTYPE t;
  db->get_type (&t);

  if (t == DB_BTREE)
    {
      u_int32_t minkey;
      u_int32_t flags;
      db->get_bt_minkey (&minkey);
      db->get_flags (&flags);
      dout(1) << "mounted version " << OSBDB_THIS_VERSION << "; Btree; "
              << "min keys per page: " << minkey << "; flags: "
              << hex << flags << dec << dendl;
      cout << dec;
    }
  else
    {
      u_int32_t ffactor;
      u_int32_t nelem;
      u_int32_t flags;
      db->get_h_ffactor (&ffactor);
      db->get_h_nelem (&nelem);
      db->get_flags (&flags);
      dout(1) << "mounted version " << OSBDB_THIS_VERSION << "; Hash; "
              << "fill factor: " << ffactor
              << " table size: " << nelem << "; flags: "
              << hex << flags << dec << dendl;
      cout << dec;
    }

  mounted = true;
  dout(4) << "..mounted" << dendl;
  return 0;
}

int OSBDB::umount()
{
  if (!mounted)
    return -EINVAL;

  dout(2) << "umount" << dendl;

  int ret;
  if (opened)
    {
      if (transactional)
        {
          env->log_flush (NULL);
          if ((ret = env->lsn_reset (device.c_str(), 0)) != 0)
            {
              derr(1) << "lsn_reset: " << db_strerror (ret) << dendl;
            }
        }

      db->sync (0);

      if ((ret = db->close (0)) != 0)
        {
          derr(1) << "close: " << db_strerror(ret) << dendl;
          return -EINVAL;
        }
      delete db;
      db = NULL;

      if (env)
        {
          env->close (0);
          delete env;
          env = NULL;
        }
    }
  mounted = false;
  opened = false;
  dout(4) << "..unmounted" << dendl;
  return 0;
}

int OSBDB::mkfs()
{
  if (mounted)
    return -EINVAL;

  dout(2) << "mkfs" << dendl;

  string d = env_dir;
  d += device;
  unlink (d.c_str());

  int ret;
  if ((ret = opendb((g_conf.bdbstore_btree ? DB_BTREE : DB_HASH),
                    DB_CREATE, true)) != 0)
    {
      derr(1) << "failed to open database: " << device << ": "
              << db_strerror(ret) << std::dendl;
      return -EINVAL;
    }
  opened = true;
  dout(3) << "..opened " << device << dendl;

  uint32_t c;
  ret = db->truncate (NULL, &c, 0);
  if (ret != 0)
    {
      derr(1) << "db truncate failed: " << db_strerror (ret) << dendl;
      return -EIO; // ???
    }

  Dbt key (OSBDB_SUPERBLOCK_KEY, 1);
  struct stored_superblock sb;
  sb.version = OSBDB_THIS_VERSION;
  Dbt value (&sb, sizeof (sb));

  dout(3) << "..writing superblock" << dendl;
  if ((ret = db->put (NULL, &key, &value, 0)) != 0)
    {
      derr(1) << "failed to write superblock: " << db_strerror (ret)
              << dendl;
      return -EIO;
    }
  dout(3) << "..wrote superblock" << dendl;
  dout(4) << "..mkfs done" << dendl;
  return 0;
}

 // Objects.

int OSBDB::pick_object_revision_lt(object_t& oid)
{
  // Not really needed.
  dout(0) << "pick_object_revision_lt " << oid << dendl;
  return -ENOSYS;
}

bool OSBDB::exists(object_t oid)
{
  dout(2) << "exists " << oid << dendl;
  struct stat st;
  bool ret = (stat (oid, &st) == 0);
  dout(4) << "..returns " << ret << dendl;
  return ret;
}

int OSBDB::statfs (struct statfs *st)
{
  // Hacky?
  if (::statfs (device.c_str(), st) != 0)
    {
      int ret = -errno;
      derr(1) << "statfs returns " << ret << dendl;
      return ret;
    }
  st->f_type = OSBDB_MAGIC;
  dout(4) << "..statfs OK" << dendl;
  return 0;
}

int OSBDB::stat(object_t oid, struct stat *st)
{
  if (!mounted)
    {
      dout(4) << "not mounted!" << dendl;
      return -EINVAL;
    }

  dout(2) << "stat " << oid << dendl;

  object_inode_key ikey = new_object_inode_key(oid);
  stored_object obj;
  Dbt key (&ikey, sizeof_object_inode_key());
  Dbt value (&obj, sizeof (obj));
  value.set_flags (DB_DBT_USERMEM);
  value.set_ulen (sizeof (obj));

  dout(3) << "  lookup " << ikey << dendl;
  int ret;
  if ((ret = db->get (NULL, &key, &value, 0)) != 0)
    {
      derr(1) << " get returned " << ret << dendl;
      return -ENOENT;
    }

  st->st_size = obj.length;
  dout(3) << "stat length:" << obj.length << dendl;
  dout(4) << "..stat OK" << dendl;
  return 0;
}

int OSBDB::remove(object_t oid, Context *onsafe)
{
  if (!mounted)
    {
      derr(1) << "not mounted!" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  dout(6) << "Context " << hex << onsafe << dec << dendl;
  scoped_lock __lock(&lock);
  dout(2) << "remove " << oid << dendl;

  DbTxn *txn = NULL;
  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  oid_t id;
  mkoid(id, oid);
  Dbt key (&id, sizeof (oid_t));
  int ret;
  if ((ret = db->del (txn, &key, 0)) != 0)
    {
      derr(1) << ".del returned error: " << db_strerror (ret) << dendl;
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EIO;
    }

  object_inode_key _ikey = new_object_inode_key (oid);
  Dbt ikey (&_ikey, sizeof_object_inode_key());
  if ((ret = db->del (txn, &ikey, 0)) != 0)
    {
      derr(1) << ".del returned error: " << db_strerror (ret) << dendl;
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EIO;
    }

  attrs_id aids = new_attrs_id (oid);
  Dbt askey (&aids, sizeof_attrs_id());
  Dbt asval;
  asval.set_flags (DB_DBT_MALLOC);
  if (db->get (txn, &askey, &asval, 0) == 0)
    {
      // We have attributes; remove them.
      stored_attrs *sap = (stored_attrs *) asval.get_data();
      auto_ptr<stored_attrs> sa (sap);
      for (unsigned i = 0; i < sap->count; i++)
        {
          attr_id aid = new_attr_id (oid, sap->names[i].name);
          Dbt akey (&aid, sizeof (aid));
          if ((ret = db->del (txn, &akey, 0)) != 0)
            {
              derr(1) << ".del returns error: " << db_strerror (ret) << dendl;
              if (txn != NULL)
                txn->abort();
              if (onsafe != NULL)
                CLEANUP(onsafe);
              return -EIO;
            }
        }
      if ((ret = db->del (txn, &askey, 0)) != 0)
        {
          derr(1) << ".del returns error: " << db_strerror (ret) << dendl;
          if (txn != NULL)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          return -EIO;
        }
    }

  // XXX check del return value

  if (txn != NULL)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);
  dout(4) << "..remove OK" << dendl;
  return 0;
}

int OSBDB::truncate(object_t oid, off_t size, Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;

  if (!mounted)
    {
      derr(1) << "not mounted!" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  scoped_lock __lock(&lock);
  dout(2) << "truncate " << size << dendl;

  if (size > 0xFFFFFFFF)
    {
      derr(1) << "object size too big!" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -ENOSPC;
    }

  DbTxn *txn = NULL;

  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  object_inode_key ikey = new_object_inode_key (oid);
  stored_object obj;
  Dbt key (&ikey, sizeof_object_inode_key());
  Dbt value (&obj, sizeof (obj));
  value.set_dlen (sizeof (obj));
  value.set_ulen (sizeof (obj));
  value.set_flags (DB_DBT_USERMEM);

  if (db->get (txn, &key, &value, 0) != 0)
    {
      if (txn)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      dout(4) << "..returns -ENOENT" << dendl;
      return -ENOENT;
    }

  if (obj.length < size)
    {
      oid_t id;
      mkoid (id, oid);
      Dbt okey (&id, sizeof (oid_t));
      char b[] = { '\0' };
      Dbt newVal (b, 1);
      newVal.set_doff ((size_t) size);
      newVal.set_dlen (1);
      newVal.set_ulen (1);
      newVal.set_flags (DB_DBT_PARTIAL);
      if (db->put (txn, &okey, &newVal, 0) != 0)
        {
          if (txn)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          derr(1) << ".updating object failed" << dendl;
          return -EIO;
        }

      obj.length = size;
      value.set_ulen (sizeof (obj));
      if (db->put (txn, &key, &value, 0) != 0)
        {
          if (txn)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          derr(1) << ".updating object info failed" << dendl;
          return -EIO;
        }
    }
  else if (obj.length > size)
    {
      obj.length = size;
      Dbt tval (&obj, sizeof (obj));
      tval.set_ulen (sizeof (obj));
      tval.set_flags (DB_DBT_USERMEM);
      if (db->put (txn, &key, &tval, 0) != 0)
        {
          if (txn)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          derr(1) << ".updating object info failed" << dendl;
          return -EIO;
        }
      if (size == 0)
        {
          char x[1];
          oid_t id;
          mkoid (id, oid);
          Dbt okey (&id, sizeof (oid_t));
          Dbt oval (&x, 0);
          if (db->put (txn, &okey, &oval, 0) != 0)
            {
              if (txn)
                txn->abort();
              if (onsafe != NULL)
                CLEANUP(onsafe);
              derr(1) << ".updating object failed" << dendl;
              return -EIO;
            }
        }
      else
        {
          oid_t id;
          mkoid (id, oid);
          Dbt okey (&id, sizeof (oid_t));
          Dbt oval;
          oval.set_flags (DB_DBT_MALLOC);
          if (db->get (txn, &okey, &oval, 0) != 0)
            {
              if (txn)
                txn->abort();
              if (onsafe != NULL)
                CLEANUP(onsafe);
              derr(1) << ".getting old object failed" << dendl;
              return -EIO;
            }
          auto_ptr<char> ovalPtr ((char *) oval.get_data());
          oval.set_size ((size_t) size);
          oval.set_ulen ((size_t) size);
          if (db->put (txn, &okey, &oval, 0) != 0)
            {
              if (txn)
                txn->abort();
              if (onsafe != NULL)
                CLEANUP(onsafe);
              derr(1) << ".putting new object failed" << dendl;
              return -EIO;
            }
        }
    }

  if (txn)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);

  dout(4) << "..truncate OK" << dendl;
  return 0;
}

int OSBDB::read(object_t oid, off_t offset, size_t len, bufferlist& bl)
{
  if (!mounted)
    {
      derr(1) << "not mounted!" << dendl;
      return -EINVAL;
    }

  dout(2) << "read " << oid << " " << offset << " "
          << len << dendl;

  if (bl.length() < len)
    {
      int remain = len - bl.length();
      bufferptr ptr (remain);
      bl.push_back(ptr);
    }

  DbTxn *txn = NULL;
  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  object_inode_key _ikey = new_object_inode_key (oid);
  stored_object obj;
  Dbt ikey (&_ikey, sizeof_object_inode_key());
  Dbt ival (&obj, sizeof (obj));
  ival.set_flags (DB_DBT_USERMEM);
  ival.set_ulen (sizeof(obj));

  dout(3) << "..get " << _ikey << dendl;
  int ret;
  if ((ret = db->get (txn, &ikey, &ival, 0)) != 0)
    {
      if (txn)
        txn->abort();
      derr(1) << "get returned " << db_strerror (ret) << dendl;
      return -ENOENT;
    }

  dout(3) << "..object has size " << obj.length << dendl;

  if (offset == 0 && len >= obj.length)
    {
      len = obj.length;
      dout(3) << "..doing full read of " << len << dendl;
      oid_t id;
      mkoid (id, oid);
      Dbt key (&id, sizeof (oid_t));
      Dbt value (bl.c_str(), len);
      value.set_ulen (len);
      value.set_flags (DB_DBT_USERMEM);
      dout(3) << "..getting " << oid << dendl;
      if ((ret = db->get (txn, &key, &value, 0)) != 0)
        {
          derr(1) << ".get returned " << db_strerror (ret) << dendl;
          if (txn)
            txn->abort();
          return -EIO;
        }
    }
  else
    {
      if (offset > obj.length)
        {
          dout(2) << "..offset out of range" << dendl;
          return 0;
        }
      if (offset + len > obj.length)
        len = obj.length - (size_t) offset;
      dout(3) << "..doing partial read of " << len << dendl;
      oid_t id;
      mkoid (id, oid);
      Dbt key (&id, sizeof (oid));
      Dbt value;
      char *data = bl.c_str();
      dout(3) << ".bufferlist c_str returned " << ((void*) data) << dendl;
      value.set_data (data);
      value.set_doff ((size_t) offset);
      value.set_dlen (len);
      value.set_ulen (len);
      value.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
      dout(3) << "..getting " << oid << dendl;
      if ((ret = db->get (txn, &key, &value, 0)) != 0)
        {
          derr(1) << ".get returned " << db_strerror (ret) << dendl;
          if (txn)
            txn->abort();
          return -EIO;
        }
    }

  if (txn)
    txn->commit (0);
  dout(4) << "..read OK, returning " << len << dendl;
  return len;
}

int OSBDB::write(object_t oid, off_t offset, size_t len,
                 bufferlist& bl, Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      derr(1) << "not mounted!" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  scoped_lock __lock(&lock);
  dout(2) << "write " << oid << " " << offset << " "
          << len << dendl;

  if (offset > 0xFFFFFFFFL || offset + len > 0xFFFFFFFFL)
    {
      derr(1) << "object too big" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -ENOSPC;
    }

  DbTxn *txn = NULL;
  if (transactional)
    env->txn_begin (txn, &txn, 0);

  object_inode_key _ikey = new_object_inode_key (oid);
  stored_object obj;
  Dbt ikey (&_ikey, sizeof_object_inode_key());
  Dbt ival (&obj, sizeof (obj));
  ival.set_ulen (sizeof (obj));
  ival.set_flags (DB_DBT_USERMEM);

  int ret;
  dout(3) << "..getting " << _ikey << dendl;
  if (db->get (txn, &ikey, &ival, 0) != 0)
    {
      dout(3) << "..writing new object" << dendl;

      // New object.
      obj.length = (size_t) offset + len;
      dout(3) << "..mapping " << _ikey << " => "
              << obj << dendl;
      if ((ret = db->put (txn, &ikey, &ival, 0)) != 0)
        {
          derr(1) << "..put returned " << db_strerror (ret) << dendl;
          if (txn)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          return -EIO;
        }

      oid_t id;
      mkoid (id, oid);
      Dbt key (&id, sizeof (oid_t));
      Dbt value (bl.c_str(), len);
      if (offset == 0) // whole object
        {
          value.set_flags (DB_DBT_USERMEM);
          value.set_ulen (len);
        }
      else
        {
          value.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);
          value.set_ulen (len);
          value.set_doff ((size_t) offset);
          value.set_dlen (len);
        }
      dout(3) << "..mapping " << oid << " => ("
              << obj.length << " bytes)" << dendl;
      if ((ret = db->put (txn, &key, &value, 0)) != 0)
        {
          derr(1) << "..put returned " << db_strerror (ret) << dendl;
          if (txn)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          return -EIO;
        }

      if (txn != NULL)
        txn->commit (0);
      if (onsafe != NULL)
        COMMIT(onsafe);

      dout(4) << "..write OK, returning " << len << dendl;
      return len;
    }

  if (offset == 0 && len >= obj.length)
    {
      if (len != obj.length)
        {
          obj.length = len;
          if ((ret = db->put (txn, &ikey, &ival, 0)) != 0)
            {
              derr(1) << "  put returned " << db_strerror (ret) << dendl;
              if (txn)
                txn->abort();
              if (onsafe != NULL)
                CLEANUP(onsafe);
              return -EIO;
            }
        }
      oid_t id;
      mkoid(id, oid);
      Dbt key (&id, sizeof (oid_t));
      Dbt value (bl.c_str(), len);
      if (db->put (txn, &key, &value, 0) != 0)
        {
          if (txn)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          derr(1) << "..writing object failed!" << dendl;
          return -EIO;
        }
    }
  else
    {
      if (offset + len > obj.length)
        {
          obj.length = (size_t) offset + len;
          if (db->put (txn, &ikey, &ival, 0) != 0)
            {
              if (txn)
                txn->abort();
              if (onsafe != NULL)
                CLEANUP(onsafe);
              derr(1) << "..writing object info failed!" << dendl;
              return -EIO;
            }
        }
      oid_t id;
      mkoid(id, oid);
      Dbt key (&id, sizeof (oid_t));
      Dbt value (bl.c_str(), len);
      value.set_doff ((size_t) offset);
      value.set_dlen (len);
      value.set_ulen (len);
      value.set_flags (DB_DBT_PARTIAL);
      if (db->put (txn, &key, &value, 0) != 0)
        {
          if (txn)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          derr(1) << "..writing object failed!" << dendl;
          return -EIO;
        }
    }

  if (txn != NULL)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);

  dout(4) << "..write OK, returning " << len << dendl;
  return len;
}

int OSBDB::clone(object_t oid, object_t noid)
{
  if (!mounted)
    {
      derr(1) << "not mounted!" << dendl;
      return -EINVAL;
    }

  dout(2) << "clone " << oid << ", " << noid << dendl;

  if (exists (noid))
    {
      dout(4) << "..target exists; returning -EEXIST" << dendl;
      return -EEXIST;
    }

  DbTxn *txn = NULL;
  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  object_inode_key _ikey = new_object_inode_key (oid);
  object_inode_key _nikey = new_object_inode_key (noid);
  stored_object obj;
  Dbt ikey (&_ikey, sizeof_object_inode_key());
  Dbt ival (&obj, sizeof (obj));
  Dbt nikey (&_nikey, sizeof_object_inode_key());
  ival.set_ulen (sizeof (obj));
  ival.set_flags (DB_DBT_USERMEM);

  oid_t id, nid;
  mkoid(id, oid);
  mkoid(nid, noid);
  Dbt key (&id, sizeof (oid_t));
  Dbt nkey (&oid, sizeof (oid_t));
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  if (db->get (txn, &ikey, &ival, 0) != 0)
    {
      if (txn)
        txn->abort();
      derr(1) << "..getting object info failed!" << dendl;
      return -ENOENT;
    }
  if (db->get (txn, &key, &value, 0) != 0)
    {
      if (txn)
        txn->abort();
      derr(1) << "..getting original object failed" << dendl;
      return -ENOENT;
    }
  auto_ptr<char> valueptr ((char *) value.get_data());

  if (db->put (txn, &nikey, &ival, 0) != 0)
    {
      if (txn)
        txn->abort();
      derr(1) << "..putting object info failed" << dendl;
      return -EIO;
    }
  if (db->put (txn, &nkey, &value, 0) != 0)
    {
      if (txn)
        txn->abort();
      derr(1) << "..putting new object failed" << dendl;
      return -EIO;
    }

  if (txn)
    txn->commit (0);

  dout(4) << "..clone OK" << dendl;
  return 0;
}

 // Collections

int OSBDB::list_collections(list<coll_t>& ls)
{
  if (!mounted)
    {
      derr(1) << "not mounted!" << dendl;
      return -EINVAL;
    }

  dout(2) << "list_collections" << dendl;

  Dbt key (COLLECTIONS_KEY, 1);
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  if (db->get (NULL, &key, &value, 0) != 0)
    {
      dout(4) << "..no collections" << dendl;
      return 0; // no collections.
    }

  auto_ptr<stored_colls> sc ((stored_colls *) value.get_data());
  stored_colls *scp = sc.get();
  for (uint32_t i = 0; i < sc->count; i++)
    ls.push_back (scp->colls[i]);

  dout(4) << "..list_collections returns " << scp->count << dendl;
  return scp->count;
}

int OSBDB::create_collection(coll_t c, Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      derr(1) << "not mounted" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  scoped_lock __lock(&lock);
  dout(2) << "create_collection " << hex << c << dec << dendl;

  Dbt key (COLLECTIONS_KEY, 1);
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  DbTxn *txn = NULL;
  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  stored_colls *scp = NULL;
  size_t sz = 0;
  bool created = false;
  if (db->get (txn, &key, &value, 0) != 0)
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
  if (ins < scp->count && scp->colls[ins] == c)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".collection " << c << " already exists " << dendl;
      return -EEXIST;
    }

  dout(3) << "..insertion point: " << ins << dendl;

  // Make room for a new collection ID.
  if (!created)
  {
    sz += sizeof (coll_t);
    dout(3) << "..increase size to " << sz << dendl;
    stored_colls *scp2 = (stored_colls *) realloc (scp, sz);
    sc.release ();
    sc.reset (scp2);
    scp = scp2;
  }

  int n = (scp->count - ins) * sizeof (coll_t);
  if (n > 0)
    {
      dout(3) << "..moving " << n << " bytes up" << dendl;
      memmove (&scp->colls[ins + 1], &scp->colls[ins], n);
    }
  scp->count++;
  scp->colls[ins] = c;

  dout(3) << "..collections: " << scp << dendl;

  // Put the modified collection list back.
  {
    Dbt value2 (scp, sz);
    if (db->put (txn, &key, &value2, 0) != 0)
      {
        if (txn != NULL)
          txn->abort();
        if (onsafe != NULL)
          CLEANUP(onsafe);
        derr(1) << ".writing new collections list failed" << dendl;
        return -EIO;
      }
  }

  // Create the new collection.
  {
    stored_coll new_coll;
    new_coll.count = 0;
    Dbt coll_key (&c, sizeof (coll_t));
    Dbt coll_value (&new_coll, sizeof (stored_coll));
    if (db->put (txn, &coll_key, &coll_value, 0) != 0)
      {
        if (txn != NULL)
          txn->abort();
        if (onsafe != NULL)
          CLEANUP(onsafe);
        derr(1) << ".writing new collection failed" << dendl;
        return -EIO;
      }
  }

  if (txn != NULL)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);

  dout(4) << "..create_collection OK" << dendl;
  return 0;
}

int OSBDB::destroy_collection(coll_t c, Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      derr(1) << "not mounted" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  scoped_lock __lock(&lock);
  dout(2) << "destroy_collection " << hex << c << dec << dendl;

  Dbt key (COLLECTIONS_KEY, 1);
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);
  DbTxn *txn = NULL;

  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  if (db->get (txn, &key, &value, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".collection list doesn't exist" << dendl;
      return -ENOENT; // XXX
    }

  stored_colls *scp = (stored_colls *) value.get_data();
  auto_ptr<stored_colls> valueBuf (scp);
  if (scp->count == 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".collection " << c << " not listed" << dendl;
      return -ENOENT;
    }
  uint32_t ins = binary_search<coll_t> (scp->colls, scp->count, c);
  dout(4) << "..insertion point is " << ins << dendl;
  if (ins >= scp->count || scp->colls[ins] != c)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".collection " << c << " not listed" << dendl;
      return -ENOENT;
    }

  dout(4) << "..collections list is " << scp << dendl;

  // Move the rest of the list down in memory, if needed.
  if (ins < scp->count)
    {
      size_t n = scp->count - ins - 1;
      dout(4) << "..shift list down " << n << dendl;
      memmove (&scp->colls[ins], &scp->colls[ins + 1], n);
    }

  dout(4) << "..collections list is " << scp << dendl;

  // Modify the record size to be one less.
  Dbt nvalue (scp, value.get_size() - sizeof (coll_t));
  nvalue.set_flags (DB_DBT_USERMEM);
  if (db->put (txn, &key, &nvalue, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".putting modified collection list failed" << dendl;
      return -EIO;
    }

  // Delete the collection.
  Dbt collKey (&c, sizeof (coll_t));
  if (db->del (txn, &collKey, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".deleting collection failed" << dendl;
      return -EIO;
    }

  if (txn != NULL)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);
  dout(4) << "..destroy_collection OK" << dendl;
  return 0;
}

bool OSBDB::collection_exists(coll_t c)
{
  if (!mounted)
    {
      derr(1) << "not mounted" << dendl;
      return -EINVAL;
    }

  dout(2) << "collection_exists " << hex << c << dec << dendl;

  /*Dbt key (COLLECTIONS_KEY, 1);
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  if (db->get (NULL, &key, &value, 0) != 0)
    {
      dout(4) << "..no collection list; return false" << dendl;
      return false;
    }

  stored_colls *scp = (stored_colls *) value.get_data();
  auto_ptr<stored_colls> sc (scp);
  dout(5) << "..collection list is " << scp << dendl;
  if (scp->count == 0)
    {
      dout(4) << "..empty collection list; return false" << dendl;
      return false;
    }
  uint32_t ins = binary_search<coll_t> (scp->colls, scp->count, c);
  dout(4) << "..insertion point is " << ins << dendl;

  int ret = (scp->colls[ins] == c);
  dout(4) << "..returns " << ret << dendl;
  return ret;*/

  Dbt key (&c, sizeof (coll_t));
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);
  if (db->get (NULL, &key, &value, 0) != 0)
    {
      dout(4) << "..no collection, return false" << dendl;
      return false;
    }
  void *val = value.get_data();
  free (val);
  dout(4) << "..collection exists; return true" << dendl;
  return true;
}

int OSBDB::collection_stat(coll_t c, struct stat *st)
{
  if (!mounted)
    {
      derr(1) << "not mounted" << dendl;
      return -EINVAL;
    }

  dout(2) << "collection_stat " << c << dendl;
  // XXX is this needed?
  return -ENOSYS;
}

int OSBDB::collection_add(coll_t c, object_t o, Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      dout(2) << "not mounted" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  scoped_lock __lock(&lock);
  dout(2) << "collection_add " << hex << c << dec << " " << o << dendl;

  Dbt key (&c, sizeof (coll_t));
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);
  DbTxn *txn = NULL;

  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  if (db->get (txn, &key, &value, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << "failed to find collection" << dendl;
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
      if (ins < scp->count && scp->objects[ins] == o)
        {
          if (txn != NULL)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          derr(1) << "collection already has object" << dendl;
          return -EEXIST;
        }
    }

  // Make room for the new value, and add it.
  sz += sizeof (object_t);
  scp = (stored_coll *) realloc (scp, sz);
  sc.release();
  sc.reset (scp);
  dout(3) << "..current collection: " << scp << dendl;
  if (ins < scp->count - 1)
    {
      size_t n = (scp->count - ins) * sizeof (object_t);
      dout(3) << "..move up " << n << " bytes" << dendl;
      memmove (&scp->objects[ins + 1], &scp->objects[ins], n);
    }
  scp->count++;
  scp->objects[ins] = o;

  dout(3) << "..collection: " << scp << dendl;

  Dbt nvalue (scp, sz);
  if (db->put (txn, &key, &nvalue, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << "..putting modified collection failed" << dendl;
      return -EIO;
    }

  if (txn != NULL)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);
  dout(4) << "..collection add OK" << dendl;
  return 0;
}

int OSBDB::collection_remove(coll_t c, object_t o, Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      derr(1) << "not mounted" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  scoped_lock __lock(&lock);
  dout(2) << "collection_remove " << hex << c << dec << " " << o << dendl;

  Dbt key (&c, sizeof (coll_t));
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);
  DbTxn *txn = NULL;
 
  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  if (db->get (txn, &key, &value, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      dout(1) << "..collection doesn't exist" << dendl;
      return -ENOENT;
    }

  stored_coll *scp = (stored_coll *) value.get_data();
  auto_ptr<stored_coll> sc (scp);

  dout(5) << "..collection is " << scp << dendl;
  if (scp->count == 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      dout(1) << "..collection is empty" << dendl;
      return -ENOENT;
    }
  uint32_t ins = binary_search<object_t> (scp->objects, scp->count, o);
  dout(4) << "..insertion point is " << ins << dendl;
  if (ins >= scp->count || scp->objects[ins] != o)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      dout(1) << "..object not in collection" << dendl;
      return -ENOENT;
    }

  if (ins < scp->count - 1)
    {
      size_t n = (scp->count - ins - 1) * sizeof (object_t);
      dout(5) << "..moving " << n << " bytes down" << dendl;
      memmove (&scp->objects[ins], &scp->objects[ins + 1], n);
    }
  scp->count--;

  dout(3) << "..collection " << scp << dendl;

  Dbt nval (scp, value.get_size() - sizeof (object_t));
  if (db->put (txn, &key, &nval, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << "..putting modified collection failed" << dendl;
      return -EIO;
    }

  if (txn != NULL)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);
  dout(4) << "..collection remove OK" << dendl;
  return 0;
}

int OSBDB::collection_list(coll_t c, list<object_t>& o)
{
  if (!mounted)
    {
      derr(1) << "not mounted" << dendl;
      return -EINVAL;
    }

  Dbt key (&c, sizeof (coll_t));
  Dbt value;
  DbTxn *txn = NULL;

  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  if (db->get (txn, &key, &value, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      return -ENOENT;
    }

  stored_coll *scp = (stored_coll *) value.get_data();
  auto_ptr<stored_coll> sc (scp);
  for (uint32_t i = 0; i < scp->count; i++)
    o.push_back (scp->objects[i]);

  if (txn != NULL)
    txn->commit (0);
  return 0;
}

 // Attributes

int OSBDB::_setattr(object_t oid, const char *name,
                    const void *value, size_t size, Context *onsafe,
                    DbTxn *txn)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  if (strlen (name) >= OSBDB_MAX_ATTR_LEN)
    {
      derr(1) << "name too long: " << name << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -ENAMETOOLONG;
    }

  scoped_lock __lock(&lock);

  // Add name to attribute list, if needed.
  attrs_id aids = new_attrs_id (oid);
  Dbt attrs_key (&aids, sizeof_attrs_id());
  Dbt attrs_val;
  attrs_val.set_flags (DB_DBT_MALLOC);
  stored_attrs *sap = NULL;
  size_t sz = 0;

  dout(3) << "  getting " << aids << dendl;
  if (db->get (txn, &attrs_key, &attrs_val, 0) != 0)
    {
      dout(2) << "  first attribute" << dendl;
      sz = sizeof (stored_attrs);
      sap = (stored_attrs *) malloc(sz);
      sap->count = 0;
    }
  else
    {
      sz = attrs_val.get_size();
      sap = (stored_attrs *) attrs_val.get_data();
      dout(2) << "..add to list of " << sap->count << " attrs" << dendl;
    }
  auto_ptr<stored_attrs> sa (sap);

  attr_name _name;
  strncpy (_name.name, name, OSBDB_MAX_ATTR_LEN);

  int ins = 0;
  if (sap->count > 0)
    ins = binary_search<attr_name> (sap->names, sap->count, _name);
  dout(3) << "..insertion point is " << ins << dendl;
  if (sap->count == 0 ||
      (ins >= sap->count || strcmp (sap->names[ins].name, name) != 0))
    {
      sz += sizeof (attr_name);
      dout(3) << "..realloc " << ((void *) sap) << " to "
              << dec << sz << dendl;
      sap = (stored_attrs *) realloc (sap, sz);
      dout(3) << "..returns " << ((void *) sap) << dendl;
      sa.release ();
      sa.reset (sap);
      int n = (sap->count - ins) * sizeof (attr_name);
      if (n > 0)
        {
          dout(3) << "..move " << n  << " bytes from 0x"
                  << hex << (&sap->names[ins]) << " to 0x"
                  << hex << (&sap->names[ins+1]) << dec << dendl;
          memmove (&sap->names[ins+1], &sap->names[ins], n);
        }
      memset (&sap->names[ins], 0, sizeof (attr_name));
      strncpy (sap->names[ins].name, name, OSBDB_MAX_ATTR_LEN);
      sap->count++;

      Dbt newAttrs_val (sap, sz);
      newAttrs_val.set_ulen (sz);
      newAttrs_val.set_flags (DB_DBT_USERMEM);
      dout(3) << "..putting " << aids << dendl;
      if (db->put (txn, &attrs_key, &newAttrs_val, 0) != 0)
        {
          derr(1) << ".writing attributes list failed" << dendl;
          if (onsafe != NULL)
            CLEANUP(onsafe);
          return -EIO;
        }
    }
  else
    {
      dout(3) << "..attribute " << name << " already exists" << dendl;
    }

  dout(5) << "..attributes list: " << sap << dendl;

  // Add the attribute.
  attr_id aid = new_attr_id (oid, name);
  Dbt attr_key (&aid, sizeof (aid));
  Dbt attr_val ((void *) value, size);
  dout(3) << "..writing attribute key " << aid << dendl;
  if (db->put (txn, &attr_key, &attr_val, 0) != 0)
    {
      derr(1) << ".writing attribute key failed" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EIO;
    }

  dout(4) << "..setattr OK" << dendl;
  if (onsafe != NULL)
    COMMIT(onsafe);
  return 0;
}

int OSBDB::setattr(object_t oid, const char *name,
                   const void *value, size_t size,
                   Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  DbTxn *txn = NULL;
  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  dout(2) << "setattr " << oid << ":" << name << " => ("
          << size << " bytes)" << dendl;
  int ret = _setattr (oid, name, value, size, onsafe, txn);
  if (ret == 0)
    {
      if (txn != NULL)
        txn->commit (0);
    }
  else
    {
      if (txn != NULL)
        txn->abort();
    }
  return ret;
}

int OSBDB::setattrs(object_t oid, map<string,bufferptr>& aset,
                    Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  DbTxn *txn = NULL;

  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  map<string,bufferptr>::iterator it;
  for (it = aset.begin(); it != aset.end(); it++)
    {
      string name = it->first;
      bufferptr value = it->second;
      int ret = _setattr (oid, name.c_str(), value.c_str(),
                          value.length(), onsafe, txn);
      if (ret != 0)
        {
          if (txn != NULL)
            txn->abort();
          return ret;
        }
    }

  if (txn != NULL)
    txn->commit (0);
  return 0;
}

int OSBDB::_getattr (object_t oid, const char *name, void *value, size_t size)
{
  if (!mounted)
    return -EINVAL;

  dout(2) << "_getattr " << oid << " " << name << " " << size << dendl;

  attr_id aid = new_attr_id (oid, name);
  Dbt key (&aid, sizeof (aid));
  Dbt val (value, size);
  val.set_ulen (size);
  val.set_doff (0);
  val.set_dlen (size);
  val.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);

  int ret;
  if ((ret = db->get (NULL, &key, &val, 0)) != 0)
    {
      derr(1) << ".getting value failed: " << db_strerror (ret) << dendl; 
      return -ENOENT;
    }

  dout(4) << ".._getattr OK; returns " << val.get_size() << dendl;
  return val.get_size();
}

int OSBDB::getattr(object_t oid, const char *name, void *value, size_t size)
{
  if (!mounted)
    return -EINVAL;

  return _getattr (oid, name, value, size);
}

int OSBDB::getattrs(object_t oid, map<string,bufferptr>& aset)
{
  if (!mounted)
    return -EINVAL;

  for (map<string,bufferptr>::iterator it = aset.begin();
       it != aset.end(); it++)
    {
      int ret = _getattr (oid, (*it).first.c_str(),
                          (*it).second.c_str(),
                          (*it).second.length());
      if (ret < 0)
        return ret;
    }
  return 0;
}

int OSBDB::rmattr(object_t oid, const char *name, Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  scoped_lock __lock(&lock);
  dout(2) << "rmattr " << oid << " " << name << dendl;

  attrs_id aids = new_attrs_id (oid);
  Dbt askey (&aids, sizeof_attrs_id());
  Dbt asvalue;
  asvalue.set_flags (DB_DBT_MALLOC);

  DbTxn *txn = NULL;

  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  if (db->get (txn, &askey, &asvalue, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -ENOENT;
    }

  stored_attrs *sap = (stored_attrs *) asvalue.get_data();
  auto_ptr<stored_attrs> sa (sap);

  dout(5) << "..attributes list " << sap << dendl;

  if (sap->count == 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".empty attribute list" << dendl;
      return -ENOENT;
    }

  attr_name _name;
  memset(&_name, 0, sizeof (_name));
  strncpy (_name.name, name, OSBDB_MAX_ATTR_LEN);
  int ins = binary_search<attr_name> (sap->names, sap->count, _name);
  dout(4) << "..insertion point is " << ins << dendl;
  if (ins >= sap->count || strcmp (sap->names[ins].name, name) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".attribute not found in list" << dendl;
      return -ENOENT;
    }

  // Shift the later elements down by one, if needed.
  int n = (sap->count - ins) * sizeof (attr_name);
  if (n > 0)
    {
      dout(4) << "..shift down by " << n << dendl;
      memmove (&(sap->names[ins]), &(sap->names[ins + 1]), n);
    }
  sap->count--;
  dout(5) << "..attributes list now " << sap << dendl;

  asvalue.set_size(asvalue.get_size() - sizeof (attr_name));
  int ret;
  if ((ret = db->put (txn, &askey, &asvalue, 0)) != 0)
    {
      derr(1) << "put stored_attrs " << db_strerror (ret) << dendl;
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EIO;
    }

  // Remove the attribute.
  attr_id aid = new_attr_id (oid, name);
  Dbt key (&aid, sizeof (aid));
  if ((ret = db->del (txn, &key, 0)) != 0)
    {
      derr(1) << "deleting " << aid << ": " << db_strerror(ret) << dendl;
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EIO;
    }

  if (txn != NULL)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);
  dout(4) << "..rmattr OK" << dendl;
  return 0;
}

int OSBDB::listattr(object_t oid, char *attrs, size_t size)
{
  if (!mounted)
    return -EINVAL;

  dout(2) << "listattr " << oid << dendl;

  attrs_id aids = new_attrs_id (oid);
  Dbt key (&aids, sizeof_attrs_id());
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  // XXX Transactions for read atomicity???

  int ret;
  if ((ret = db->get (NULL, &key, &value, 0)) != 0)
    {
      derr(1) << "fetching " << aids << ": " << db_strerror (ret)
              << dendl;
      return -ENOENT;
    }

  stored_attrs *attrsp = (stored_attrs *) value.get_data();
  auto_ptr<stored_attrs> _attrs (attrsp);
  size_t s = 0;
  char *p = attrs;
  for (unsigned i = 0; i < attrsp->count && s < size; i++)
    {
      int n = MIN (OSBDB_MAX_ATTR_LEN,
                   MIN (strlen (attrsp->names[i].name), size - s - 1));
      strncpy (p, attrsp->names[i].name, n);
      p[n] = '\0';
      p = p + n + 1;
    }

  dout(4) << "listattr OK" << dendl;
  return 0;
}

 // Collection attributes.

int OSBDB::collection_setattr(coll_t cid, const char *name,
                              const void *value, size_t size,
                              Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  scoped_lock __lock(&lock);
  dout(2) << "collection_setattr " << hex << cid << dec << " " << name
          << " (" << size << " bytes)" << dendl;
  if (strlen (name) >= OSBDB_MAX_ATTR_LEN)
    {
      derr(1) << "name too long" << dendl;
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -ENAMETOOLONG;
    }

  // Add name to attribute list, if needed.
  coll_attrs_id aids = new_coll_attrs_id (cid);
  Dbt attrs_key (&aids, sizeof_coll_attrs_id());
  Dbt attrs_val;
  attrs_val.set_flags (DB_DBT_MALLOC);
  stored_attrs *sap = NULL;
  size_t sz = 0;

  DbTxn *txn = NULL;
  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  dout(3) << "  getting " << aids << dendl;
  if (db->get (txn, &attrs_key, &attrs_val, 0) != 0)
    {
      dout(2) << "  first attribute" << dendl;
      sz = sizeof (stored_attrs);
      sap = (stored_attrs *) malloc(sz);
      sap->count = 0;
    }
  else
    {
      sz = attrs_val.get_size();
      sap = (stored_attrs *) attrs_val.get_data();
      dout(2) << "  add to list of " << sap->count << " attrs" << dendl;
    }
  auto_ptr<stored_attrs> sa (sap);

  attr_name _name;
  strncpy (_name.name, name, OSBDB_MAX_ATTR_LEN);

  int ins = 0;
  if (sap->count > 0)
    ins = binary_search<attr_name> (sap->names, sap->count, _name);
  dout(3) << "  insertion point is " << ins << dendl;
  if (ins >= sap->count || strcmp (sap->names[ins].name, name) != 0)
    {
      sz += sizeof (attr_name);
      dout(3) << "  realloc " << hex << ((void *) sap) << " to "
              << dec << sz << dendl;
      sap = (stored_attrs *) realloc (sap, sz);
      dout(3) << "  returns " << hex << ((void *) sap) << dec << dendl;
      sa.release ();
      sa.reset (sap);
      int n = (sap->count - ins) * sizeof (attr_name);
      if (n > 0)
        {
          dout(3) << "  move " << n  << " bytes from 0x"
                  << hex << (&sap->names[ins]) << " to 0x"
                  << hex << (&sap->names[ins+1]) << dec << dendl;
          memmove (&sap->names[ins+1], &sap->names[ins], n);
        }
      memset (&sap->names[ins], 0, sizeof (attr_name));
      strncpy (sap->names[ins].name, name, OSBDB_MAX_ATTR_LEN);
      sap->count++;

      Dbt newAttrs_val (sap, sz);
      newAttrs_val.set_ulen (sz);
      newAttrs_val.set_flags (DB_DBT_USERMEM);
      dout(3) << "  putting " << aids << dendl;
      if (db->put (txn, &attrs_key, &newAttrs_val, 0) != 0)
        {
          if (txn != NULL)
            txn->abort();
          if (onsafe != NULL)
            CLEANUP(onsafe);
          derr(1) << ".putting new attributes failed" << dendl;
          return -EIO;
        }
    }
  else
    {
      dout(3) << "..attribute " << name << " already exists" << dendl;
    }

  dout(3) << "..attributes list: " << sap << dendl;

  // Add the attribute.
  coll_attr_id aid = new_coll_attr_id (cid, name);
  Dbt attr_key (&aid, sizeof (aid));
  Dbt attr_val ((void *) value, size);
  dout(3) << "  writing attribute key " << aid << dendl;
  if (db->put (txn, &attr_key, &attr_val, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".putting attribute failed" << dendl;
      return -EIO;
    }

  if (txn != NULL)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);

  dout(4) << "..collection setattr OK" << dendl;
  return 0;
}

int OSBDB::collection_rmattr(coll_t cid, const char *name,
                             Context *onsafe)
{
  dout(6) << "Context " << hex << onsafe << dec << dendl;
  if (!mounted)
    {
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EINVAL;
    }

  scoped_lock __lock(&lock);
  dout(2) << "collection_rmattr " << hex << cid << dec
          << " " << name << dendl;

  coll_attrs_id aids = new_coll_attrs_id (cid);
  Dbt askey (&aids, sizeof_coll_attrs_id());
  Dbt asvalue;
  asvalue.set_flags (DB_DBT_MALLOC);

  DbTxn *txn = NULL;
  if (transactional)
    env->txn_begin (NULL, &txn, 0);

  if (db->get (txn, &askey, &asvalue, 0) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".no attributes list" << dendl;
      return -ENOENT;
    }

  stored_attrs *sap = (stored_attrs *) asvalue.get_data();
  auto_ptr<stored_attrs> sa (sap);

  dout(5) << "..attributes list " << sap << dendl;
  if (sap->count == 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".empty attributes list" << dendl;
      return -ENOENT;
    }

  attr_name _name;
  memset(&_name, 0, sizeof (_name));
  strncpy (_name.name, name, OSBDB_MAX_ATTR_LEN);
  int ins = binary_search<attr_name> (sap->names, sap->count, _name);
  if (ins >= sap->count || strcmp (sap->names[ins].name, name) != 0)
    {
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      derr(1) << ".attribute not listed" << dendl;
      return -ENOENT;
    }

  // Shift the later elements down by one, if needed.
  int n = (sap->count - ins) * sizeof (attr_name);
  if (n > 0)
    {
      dout(4) << "..shift down by " << n << dendl;
      memmove (&(sap->names[ins]), &(sap->names[ins + 1]), n);
    }
  sap->count--;
  dout(5) << "..attributes list now " << sap << dendl;

  asvalue.set_size(asvalue.get_size() - sizeof (attr_name));
  int ret;
  if ((ret = db->put (txn, &askey, &asvalue, 0)) != 0)
    {
      derr(1) << "put stored_attrs " << db_strerror (ret) << dendl;
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EIO;
    }

  // Remove the attribute.
  coll_attr_id aid = new_coll_attr_id (cid, name);
  Dbt key (&aid, sizeof (aid));
  if ((ret = db->del (txn, &key, 0)) != 0)
    {
      derr(1) << "deleting " << aid << ": " << db_strerror(ret) << dendl;
      if (txn != NULL)
        txn->abort();
      if (onsafe != NULL)
        CLEANUP(onsafe);
      return -EIO;
    }

  if (txn != NULL)
    txn->commit (0);
  if (onsafe != NULL)
    COMMIT(onsafe);

  dout(4) << "..collection rmattr OK" << dendl;
  return 0;
}

int OSBDB::collection_getattr(coll_t cid, const char *name,
                              void *value, size_t size)
{
  if (!mounted)
    return -EINVAL;

  dout(2) << "collection_getattr " << hex << cid << dec
          << " " << name << dendl;

  // XXX transactions/read isolation?

  coll_attr_id caid = new_coll_attr_id (cid, name);
  Dbt key (&caid, sizeof (caid));
  Dbt val (value, size);
  val.set_ulen (size);
  val.set_dlen (size);
  val.set_flags (DB_DBT_USERMEM | DB_DBT_PARTIAL);

  if (db->get (NULL, &key, &val, 0) != 0)
    {
      derr(1) << ".no attribute entry" << dendl;
      return -ENOENT;
    }

  dout(4) << "..collection getattr OK; returns " << val.get_size() << dendl;
  return val.get_size();
}

int OSBDB::collection_listattr(coll_t cid, char *attrs, size_t size)
{
  if (!mounted)
    return -EINVAL;

  dout(2) << "collection_listattr " << hex << cid << dec << dendl;

  // XXX transactions/read isolation?

  coll_attrs_id caids = new_coll_attrs_id (cid);
  Dbt key (&caids, sizeof_coll_attrs_id());
  Dbt value;
  value.set_flags (DB_DBT_MALLOC);

  int ret;
  if ((ret = db->get (NULL, &key, &value, 0)) != 0)
    {
      derr(1) << "fetching " << caids << ": " << db_strerror (ret)
              << dendl;
      return -ENOENT;
    }

  stored_attrs *attrsp = (stored_attrs *) value.get_data();
  auto_ptr<stored_attrs> _attrs (attrsp);
  size_t s = 0;
  char *p = attrs;
  for (unsigned i = 0; i < attrsp->count && s < size; i++)
    {
      int n = MIN (OSBDB_MAX_ATTR_LEN,
                   MIN (strlen (attrsp->names[i].name), size - s - 1));
      strncpy (p, attrsp->names[i].name, n);
      p[n] = '\0';
      p = p + n + 1;
    }
  return 0;
}

 // Sync.

void OSBDB::sync (Context *onsync)
{
  if (!mounted)
    return;

  sync();

  if (onsync != NULL)
    {
      g_timer.add_event_after(0.1, onsync);
    }
}

void OSBDB::sync()
{
  if (!mounted)
    return;

  if (transactional)
    {
      env->log_flush (NULL);
      env->lsn_reset (device.c_str(), 0);
    }
  db->sync(0);
}
