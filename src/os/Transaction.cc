// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "ObjectStore.h"
#include "common/Formatter.h"

void ObjectStore::Transaction::dump(ceph::Formatter *f)
{
  f->open_array_section("ops");
  iterator i = begin();
  int op_num = 0;
  bool stop_looping = false;
  while (i.have_op() && !stop_looping) {
    int op = i.decode_op();
    f->open_object_section("op");
    f->dump_int("op_num", op_num);

    switch (op) {
    case Transaction::OP_NOP:
      f->dump_string("op_name", "nop");
      break;
    case Transaction::OP_TOUCH:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	f->dump_string("op_name", "touch");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;
      
    case Transaction::OP_WRITE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	uint64_t off = i.decode_length();
	uint64_t len = i.decode_length();
	bufferlist bl;
	i.decode_bl(bl);
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
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	uint64_t off = i.decode_length();
	uint64_t len = i.decode_length();
	f->dump_string("op_name", "zero");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
        f->dump_unsigned("offset", off);
	f->dump_unsigned("length", len);
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	uint64_t off = i.decode_length();
	uint64_t len = i.decode_length();
	f->dump_string("op_name", "trim_cache");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_unsigned("offset", off);
	f->dump_unsigned("length", len);
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	uint64_t off = i.decode_length();
	f->dump_string("op_name", "truncate");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_unsigned("offset", off);
      }
      break;
      
    case Transaction::OP_REMOVE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	f->dump_string("op_name", "remove");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	string name = i.decode_attrname();
	bufferlist bl;
	i.decode_bl(bl);
	f->dump_string("op_name", "setattr");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_string("name", name);
	f->dump_unsigned("length", bl.length());
      }
      break;
      
    case Transaction::OP_SETATTRS:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	map<string, bufferptr> aset;
	i.decode_attrset(aset);
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
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	string name = i.decode_attrname();
	f->dump_string("op_name", "rmattr");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_string("name", name);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	f->dump_string("op_name", "rmattrs");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;
      
    case Transaction::OP_CLONE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	ghobject_t noid = i.decode_oid();
	f->dump_string("op_name", "clone");
	f->dump_stream("collection") << cid;
	f->dump_stream("src_oid") << oid;
	f->dump_stream("dst_oid") << noid;
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	ghobject_t noid = i.decode_oid();
	uint64_t off = i.decode_length();
	uint64_t len = i.decode_length();
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
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	ghobject_t noid = i.decode_oid();
	uint64_t srcoff = i.decode_length();
	uint64_t len = i.decode_length();
	uint64_t dstoff = i.decode_length();
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
	coll_t cid = i.decode_cid();
	f->dump_string("op_name", "mkcoll");
	f->dump_stream("collection") << cid;
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        coll_t cid = i.decode_cid();
        uint32_t type = i.decode_u32();
        f->dump_string("op_name", "coll_hint");
        f->dump_stream("collection") << cid;
        f->dump_unsigned("type", type);
        bufferlist hint;
        i.decode_bl(hint);
        bufferlist::iterator hiter = hint.begin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ::decode(pg_num, hiter);
          ::decode(num_objs, hiter);
          f->dump_unsigned("pg_num", pg_num);
          f->dump_unsigned("expected_num_objects", num_objs);
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
	coll_t cid = i.decode_cid();
	f->dump_string("op_name", "rmcoll");
	f->dump_stream("collection") << cid;
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ncid = i.decode_cid();
	coll_t ocid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	f->dump_string("op_name", "collection_add");
	f->dump_stream("src_collection") << ocid;
	f->dump_stream("dst_collection") << ncid;
	f->dump_stream("oid") << oid;
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
	coll_t cid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	f->dump_string("op_name", "collection_remove");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
       }
      break;

    case Transaction::OP_COLL_MOVE:
       {
	coll_t ocid = i.decode_cid();
	coll_t ncid = i.decode_cid();
	ghobject_t oid = i.decode_oid();
	f->open_object_section("collection_move");
	f->dump_stream("src_collection") << ocid;
	f->dump_stream("dst_collection") << ncid;
	f->dump_stream("oid") << oid;
	f->close_section();
       }
      break;


    case Transaction::OP_COLL_SETATTR:
      {
	coll_t cid = i.decode_cid();
	string name = i.decode_attrname();
	bufferlist bl;
	i.decode_bl(bl);
	f->dump_string("op_name", "collection_setattr");
	f->dump_stream("collection") << cid;
	f->dump_string("name", name);
	f->dump_unsigned("length", bl.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = i.decode_cid();
	string name = i.decode_attrname();
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
	coll_t cid(i.decode_cid());
	coll_t ncid(i.decode_cid());
	f->dump_string("op_name", "collection_rename");
	f->dump_stream("src_collection") << cid;
	f->dump_stream("dst_collection") << ncid;
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	f->dump_string("op_name", "omap_clear");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;

    case Transaction::OP_OMAP_SETKEYS:
      {
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	map<string, bufferlist> aset;
	i.decode_attrset(aset);
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
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	set<string> keys;
	i.decode_keyset(keys);
	f->dump_string("op_name", "omap_rmkeys");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
      }
      break;

    case Transaction::OP_OMAP_SETHEADER:
      {
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	bufferlist bl;
	i.decode_bl(bl);
	f->dump_string("op_name", "omap_setheader");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_stream("header_length") << bl.length();
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION:
      {
	coll_t cid(i.decode_cid());
	uint32_t bits(i.decode_u32());
	uint32_t rem(i.decode_u32());
	coll_t dest(i.decode_cid());
	f->dump_string("op_name", "op_split_collection_create");
	f->dump_stream("collection") << cid;
	f->dump_stream("bits") << bits;
	f->dump_stream("rem") << rem;
	f->dump_stream("dest") << dest;
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION2:
      {
	coll_t cid(i.decode_cid());
	uint32_t bits(i.decode_u32());
	uint32_t rem(i.decode_u32());
	coll_t dest(i.decode_cid());
	f->dump_string("op_name", "op_split_collection");
	f->dump_stream("collection") << cid;
	f->dump_stream("bits") << bits;
	f->dump_stream("rem") << rem;
	f->dump_stream("dest") << dest;
      }
      break;

    case Transaction::OP_OMAP_RMKEYRANGE:
      {
	coll_t cid(i.decode_cid());
	ghobject_t oid = i.decode_oid();
	string first, last;
	first = i.decode_key();
	last = i.decode_key();
	f->dump_string("op_name", "op_omap_rmkeyrange");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_string("first", first);
	f->dump_string("last", last);
      }
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
	coll_t old_cid(i.decode_cid());
	ghobject_t old_oid = i.decode_oid();
	coll_t new_cid(i.decode_cid());
	ghobject_t new_oid = i.decode_oid();
	f->dump_string("op_name", "op_coll_move_rename");
	f->dump_stream("old_collection") << old_cid;
	f->dump_stream("old_oid") << old_oid;
	f->dump_stream("new_collection") << new_cid;
	f->dump_stream("new_oid") << new_oid;
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        coll_t cid = i.decode_cid();
        ghobject_t oid = i.decode_oid();
        uint64_t expected_object_size = i.decode_length();
        uint64_t expected_write_size = i.decode_length();
        f->dump_string("op_name", "op_setallochint");
        f->dump_stream("collection") << cid;
        f->dump_stream("oid") << oid;
        f->dump_stream("expected_object_size") << expected_object_size;
        f->dump_stream("expected_write_size") << expected_write_size;
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
  t->collection_move(c, c2, o3);
  t->remove_collection(c);
  t->collection_setattr(c, "this", bl);
  t->collection_rmattr(c, "foo");
  t->collection_setattrs(c, m);
  o.push_back(t);  
}
