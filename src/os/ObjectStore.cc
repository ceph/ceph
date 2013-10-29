// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#include <ctype.h>
#include <sstream>
#include "include/memory.h"
#include "ObjectStore.h"
#include "common/Formatter.h"
#include "FileStore.h"
#include "MemStore.h"
#include "common/safe_io.h"

ObjectStore *ObjectStore::create(CephContext *cct,
				 const string& type,
				 const string& data,
				 const string& journal)
{
  if (type == "filestore") {
    return new FileStore(data, journal);
  }
  if (type == "memstore") {
    return new MemStore(cct, data);
  }
  return NULL;
}

int ObjectStore::write_meta(const std::string& key,
			    const std::string& value)
{
  string v = value;
  v += "\n";
  int r = safe_write_file(path.c_str(), key.c_str(),
			  v.c_str(), v.length());
  if (r < 0)
    return r;
  return 0;
}

int ObjectStore::read_meta(const std::string& key,
			   std::string *value)
{
  char buf[4096];
  int r = safe_read_file(path.c_str(), key.c_str(),
			 buf, sizeof(buf));
  if (r <= 0)
    return r;
  // drop trailing newlines
  while (r && isspace(buf[r-1])) {
    --r;
  }
  *value = string(buf, r);
  return 0;
}




ostream& operator<<(ostream& out, const ObjectStore::Sequencer& s)
{
  return out << "osr(" << s.get_name() << " " << &s << ")";
}

unsigned ObjectStore::apply_transactions(Sequencer *osr,
					 list<Transaction*> &tls,
					 Context *ondisk)
{
  // use op pool
  Cond my_cond;
  Mutex my_lock("ObjectStore::apply_transaction::my_lock");
  int r = 0;
  bool done;
  C_SafeCond *onreadable = new C_SafeCond(&my_lock, &my_cond, &done, &r);

  queue_transactions(osr, tls, onreadable, ondisk);

  my_lock.Lock();
  while (!done)
    my_cond.Wait(my_lock);
  my_lock.Unlock();
  return r;
}

int ObjectStore::queue_transactions(
  Sequencer *osr,
  list<Transaction*>& tls,
  Context *onreadable,
  Context *oncommit,
  Context *onreadable_sync,
  Context *oncomplete,
  TrackedOpRef op = TrackedOpRef())
{
  RunOnDeleteRef _complete(new RunOnDelete(oncomplete));
  Context *_onreadable = new Wrapper<RunOnDeleteRef>(
    onreadable, _complete);
  Context *_oncommit = new Wrapper<RunOnDeleteRef>(
    oncommit, _complete);
  return queue_transactions(osr, tls, _onreadable, _oncommit,
			    onreadable_sync, op);
}

void ObjectStore::Transaction::dump(ceph::Formatter *f)
{
  f->open_array_section("ops");
  iterator i = begin();
  int op_num = 0;
  bool stop_looping = false;
  while (i.have_op() && !stop_looping) {
    int op = i.get_op();
    f->open_object_section("op");
    f->dump_int("op_num", op_num);

    switch (op) {
    case Transaction::OP_NOP:
      f->dump_string("op_name", "nop");
      break;
    case Transaction::OP_TOUCH:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	f->dump_string("op_name", "touch");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;
      
    case Transaction::OP_WRITE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	bufferlist bl;
	i.get_bl(bl);
	f->dump_string("op_name", "write");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
        f->dump_unsigned("length", len);
        f->dump_unsigned("offset", off);
        f->dump_unsigned("bufferlist length", bl.length());
      }
      break;
      
    case Transaction::OP_ZERO:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	f->dump_string("op_name", "zero");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
        f->dump_unsigned("offset", off);
	f->dump_unsigned("length", len);
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	f->dump_string("op_name", "trim_cache");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_unsigned("offset", off);
	f->dump_unsigned("length", len);
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	f->dump_string("op_name", "truncate");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_unsigned("offset", off);
      }
      break;
      
    case Transaction::OP_REMOVE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	f->dump_string("op_name", "remove");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	string name = i.get_attrname();
	bufferlist bl;
	i.get_bl(bl);
	f->dump_string("op_name", "setattr");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_string("name", name);
	f->dump_unsigned("length", bl.length());
      }
      break;
      
    case Transaction::OP_SETATTRS:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	map<string, bufferptr> aset;
	i.get_attrset(aset);
	f->dump_string("op_name", "setattrs");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->open_object_section("attr_lens");
	for (map<string,bufferptr>::iterator p = aset.begin();
	    p != aset.end(); ++p) {
	  f->dump_unsigned(p->first.c_str(), p->second.length());
	}
	f->close_section();
      }
      break;

    case Transaction::OP_RMATTR:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	string name = i.get_attrname();
	f->dump_string("op_name", "rmattr");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_string("name", name);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	f->dump_string("op_name", "rmattrs");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;
      
    case Transaction::OP_CLONE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	ghobject_t noid = i.get_oid();
	f->dump_string("op_name", "clone");
	f->dump_stream("collection") << cid;
	f->dump_stream("src_oid") << oid;
	f->dump_stream("dst_oid") << noid;
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	ghobject_t noid = i.get_oid();
 	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	f->dump_string("op_name", "clonerange");
	f->dump_stream("collection") << cid;
	f->dump_stream("src_oid") << oid;
	f->dump_stream("dst_oid") << noid;
	f->dump_unsigned("offset", off);
	f->dump_unsigned("len", len);
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	ghobject_t noid = i.get_oid();
 	uint64_t srcoff = i.get_length();
	uint64_t len = i.get_length();
 	uint64_t dstoff = i.get_length();
	f->dump_string("op_name", "clonerange2");
	f->dump_stream("collection") << cid;
	f->dump_stream("src_oid") << oid;
	f->dump_stream("dst_oid") << noid;
	f->dump_unsigned("src_offset", srcoff);
	f->dump_unsigned("len", len);
	f->dump_unsigned("dst_offset", dstoff);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	coll_t cid = i.get_cid();
	f->dump_string("op_name", "mkcoll");
	f->dump_stream("collection") << cid;
      }
      break;

    case Transaction::OP_RMCOLL:
      {
	coll_t cid = i.get_cid();
	f->dump_string("op_name", "rmcoll");
	f->dump_stream("collection") << cid;
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ocid = i.get_cid();
	coll_t ncid = i.get_cid();
	ghobject_t oid = i.get_oid();
	f->dump_string("op_name", "collection_add");
	f->dump_stream("src_collection") << ocid;
	f->dump_stream("dst_collection") << ncid;
	f->dump_stream("oid") << oid;
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	f->dump_string("op_name", "collection_remove");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
       }
      break;

    case Transaction::OP_COLL_MOVE:
       {
	coll_t ocid = i.get_cid();
	coll_t ncid = i.get_cid();
	ghobject_t oid = i.get_oid();
	f->open_object_section("collection_move");
	f->dump_stream("src_collection") << ocid;
	f->dump_stream("dst_collection") << ncid;
	f->dump_stream("oid") << oid;
	f->close_section();
       }
      break;


    case Transaction::OP_COLL_SETATTR:
      {
	coll_t cid = i.get_cid();
	string name = i.get_attrname();
	bufferlist bl;
	i.get_bl(bl);
	f->dump_string("op_name", "collection_setattr");
	f->dump_stream("collection") << cid;
	f->dump_string("name", name);
	f->dump_unsigned("length", bl.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = i.get_cid();
	string name = i.get_attrname();
	f->dump_string("op_name", "collection_rmattr");
	f->dump_stream("collection") << cid;
	f->dump_string("name", name);
      }
      break;

    case Transaction::OP_STARTSYNC:
      f->dump_string("op_name", "startsync");
      break;

    case Transaction::OP_COLL_RENAME:
      {
	coll_t cid(i.get_cid());
	coll_t ncid(i.get_cid());
	f->dump_string("op_name", "collection_rename");
	f->dump_stream("src_collection") << cid;
	f->dump_stream("dst_collection") << ncid;
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	f->dump_string("op_name", "omap_clear");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;

    case Transaction::OP_OMAP_SETKEYS:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	map<string, bufferlist> aset;
	i.get_attrset(aset);
	f->dump_string("op_name", "omap_setkeys");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->open_object_section("attr_lens");
	for (map<string, bufferlist>::iterator p = aset.begin();
	    p != aset.end(); ++p) {
	  f->dump_unsigned(p->first.c_str(), p->second.length());
	}
	f->close_section();
      }
      break;

    case Transaction::OP_OMAP_RMKEYS:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	set<string> keys;
	i.get_keyset(keys);
	f->dump_string("op_name", "omap_rmkeys");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;

    case Transaction::OP_OMAP_SETHEADER:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	bufferlist bl;
	i.get_bl(bl);
	f->dump_string("op_name", "omap_setheader");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_stream("header_length") << bl.length();
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION:
      {
	coll_t cid(i.get_cid());
	uint32_t bits(i.get_u32());
	uint32_t rem(i.get_u32());
	coll_t dest(i.get_cid());
	f->dump_string("op_name", "op_split_collection_create");
	f->dump_stream("collection") << cid;
	f->dump_stream("bits") << bits;
	f->dump_stream("rem") << rem;
	f->dump_stream("dest") << dest;
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION2:
      {
	coll_t cid(i.get_cid());
	uint32_t bits(i.get_u32());
	uint32_t rem(i.get_u32());
	coll_t dest(i.get_cid());
	f->dump_string("op_name", "op_split_collection");
	f->dump_stream("collection") << cid;
	f->dump_stream("bits") << bits;
	f->dump_stream("rem") << rem;
	f->dump_stream("dest") << dest;
      }
      break;

    case Transaction::OP_OMAP_RMKEYRANGE:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	string first, last;
	first = i.get_key();
	last = i.get_key();
	f->dump_string("op_name", "op_omap_rmkeyrange");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_string("first", first);
	f->dump_string("last", last);
      }
      break;

    default:
      f->dump_string("op_name", "unknown");
      f->dump_unsigned("op_code", op);
      stop_looping = true;
      break;
    }
    f->close_section();
    op_num++;
  }
  f->close_section();
}

void ObjectStore::Transaction::generate_test_instances(list<ObjectStore::Transaction*>& o)
{
  o.push_back(new Transaction);

  Transaction *t = new Transaction;
  t->nop();
  o.push_back(t);
  
  t = new Transaction;
  coll_t c("foocoll");
  coll_t c2("foocoll2");
  ghobject_t o1(hobject_t("obj", "", 123, 456, -1, ""));
  ghobject_t o2(hobject_t("obj2", "", 123, 456, -1, ""));
  ghobject_t o3(hobject_t("obj3", "", 123, 456, -1, ""));
  t->touch(c, o1);
  bufferlist bl;
  bl.append("some data");
  t->write(c, o1, 1, bl.length(), bl);
  t->zero(c, o1, 22, 33);
  t->truncate(c, o1, 99);
  t->remove(c, o1);
  o.push_back(t);

  t = new Transaction;
  t->setattr(c, o1, "key", bl);
  map<string,bufferptr> m;
  m["a"] = buffer::copy("this", 4);
  m["b"] = buffer::copy("that", 4);
  t->setattrs(c, o1, m);
  t->rmattr(c, o1, "b");
  t->rmattrs(c, o1);

  t->clone(c, o1, o2);
  t->clone(c, o1, o3);
  t->clone_range(c, o1, o2, 1, 12, 99);

  t->create_collection(c);
  t->collection_add(c, c2, o1);
  t->collection_add(c, c2, o2);
  t->collection_move(c, c2, o3);
  t->remove_collection(c);
  t->collection_setattr(c, "this", bl);
  t->collection_rmattr(c, "foo");
  t->collection_setattrs(c, m);
  t->collection_rename(c, c2);
  o.push_back(t);  
}

int ObjectStore::collection_list(coll_t c, vector<hobject_t>& o)
{
  vector<ghobject_t> go;
  int ret = collection_list(c, go);
  if (ret == 0) {
    o.reserve(go.size());
    for (vector<ghobject_t>::iterator i = go.begin(); i != go.end() ; ++i)
      o.push_back(i->hobj);
  }
  return ret;
}

int ObjectStore::collection_list_partial(coll_t c, hobject_t start,
			      int min, int max, snapid_t snap,
				      vector<hobject_t> *ls, hobject_t *next)
{
  vector<ghobject_t> go;
  ghobject_t gnext, gstart(start);
  int ret = collection_list_partial(c, gstart, min, max, snap, &go, &gnext);
  if (ret == 0) {
    *next = gnext.hobj;
    ls->reserve(go.size());
    for (vector<ghobject_t>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(i->hobj);
  }
  return ret;
}

int ObjectStore::collection_list_range(coll_t c, hobject_t start, hobject_t end,
			    snapid_t seq, vector<hobject_t> *ls)
{
  vector<ghobject_t> go;
  ghobject_t gstart(start), gend(end);
  int ret = collection_list_range(c, gstart, gend, seq, &go);
  if (ret == 0) {
    ls->reserve(go.size());
    for (vector<ghobject_t>::iterator i = go.begin(); i != go.end() ; ++i)
      ls->push_back(i->hobj);
  }
  return ret;
}
