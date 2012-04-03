
#include "ObjectStore.h"
#include "common/Formatter.h"

ostream& operator<<(ostream& out, const ObjectStore::Sequencer& s)
{
  return out << "osr(" << s.get_name() << " " << &s << ")";
}

void ObjectStore::Transaction::dump(ceph::Formatter *f)
{
  f->open_array_section("ops");
  iterator i = begin();
  int op_num = 0;
  while (i.have_op()) {
    int op = i.get_op();
    switch (op) {
    case Transaction::OP_NOP:
      f->open_object_section("nop");
      f->close_section();
      break;
    case Transaction::OP_TOUCH:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	f->open_object_section("touch");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->close_section();
      }
      break;
      
    case Transaction::OP_WRITE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	bufferlist bl;
	i.get_bl(bl);
	f->open_object_section("write");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_unsigned("offset", off);
	f->dump_unsigned("length", len);
	f->close_section();
      }
      break;
      
    case Transaction::OP_ZERO:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	f->open_object_section("zero");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_unsigned("offset", off);
	f->dump_unsigned("length", len);
	f->close_section();
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	f->open_object_section("trim_cache");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_unsigned("offset", off);
	f->dump_unsigned("length", len);
	f->close_section();
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	f->open_object_section("truncate");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_unsigned("offset", off);
	f->close_section();
      }
      break;
      
    case Transaction::OP_REMOVE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	f->open_object_section("remove");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->close_section();
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	string name = i.get_attrname();
	bufferlist bl;
	i.get_bl(bl);
	f->open_object_section("setattr");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_string("name", name);
	f->dump_unsigned("length", bl.length());
	f->close_section();
      }
      break;
      
    case Transaction::OP_SETATTRS:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	map<string, bufferptr> aset;
	i.get_attrset(aset);
	f->open_object_section("setattrs");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->open_object_section("attr_lens");
	for (map<string,bufferptr>::iterator p = aset.begin(); p != aset.end(); ++p)
	  f->dump_unsigned(p->first.c_str(), p->second.length());
	f->close_section();
	f->close_section();
      }
      break;

    case Transaction::OP_RMATTR:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	string name = i.get_attrname();
	f->open_object_section("rmattr");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_string("name", name);
	f->close_section();
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	f->open_object_section("rmattrs");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->close_section();
      }
      break;
      
    case Transaction::OP_CLONE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	hobject_t noid = i.get_oid();
	f->open_object_section("clone");
	f->dump_stream("collection") << cid;
	f->dump_stream("src_oid") << oid;
	f->dump_stream("dst_oid") << noid;
	f->close_section();
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	hobject_t noid = i.get_oid();
 	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	f->open_object_section("clonerange");
	f->dump_stream("collection") << cid;
	f->dump_stream("src_oid") << oid;
	f->dump_stream("dst_oid") << noid;
	f->dump_unsigned("offset", off);
	f->dump_unsigned("len", len);
	f->close_section();
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	hobject_t noid = i.get_oid();
 	uint64_t srcoff = i.get_length();
	uint64_t len = i.get_length();
 	uint64_t dstoff = i.get_length();
	f->open_object_section("clonerange2");
	f->dump_stream("collection") << cid;
	f->dump_stream("src_oid") << oid;
	f->dump_stream("dst_oid") << noid;
	f->dump_unsigned("src_offset", srcoff);
	f->dump_unsigned("len", len);
	f->dump_unsigned("dst_offset", dstoff);
	f->close_section();
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	coll_t cid = i.get_cid();
	f->open_object_section("mkcoll");
	f->dump_stream("collection") << cid;
	f->close_section();
      }
      break;

    case Transaction::OP_RMCOLL:
      {
	coll_t cid = i.get_cid();
	f->open_object_section("rmcoll");
	f->dump_stream("collection") << cid;
	f->close_section();
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ocid = i.get_cid();
	coll_t ncid = i.get_cid();
	hobject_t oid = i.get_oid();
	f->open_object_section("collection_add");
	f->dump_stream("src_collection") << ocid;
	f->dump_stream("dst_collection") << ncid;
	f->dump_stream("oid") << oid;
	f->close_section();
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	f->open_object_section("collection_remove");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->close_section();
       }
      break;

    case Transaction::OP_COLL_MOVE:
       {
	coll_t ocid = i.get_cid();
	coll_t ncid = i.get_cid();
	hobject_t oid = i.get_oid();
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
	f->open_object_section("collection_setattr");
	f->dump_stream("collection") << cid;
	f->dump_string("name", name);
	f->dump_unsigned("length", bl.length());
	f->close_section();
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = i.get_cid();
	string name = i.get_attrname();
	f->open_object_section("collection_rmattr");
	f->dump_stream("collection") << cid;
	f->dump_string("name", name);
	f->close_section();
      }
      break;

    case Transaction::OP_STARTSYNC:
      f->open_object_section("startsync");
      f->close_section();
      break;

    case Transaction::OP_COLL_RENAME:
      {
	coll_t cid(i.get_cid());
	coll_t ncid(i.get_cid());
	f->open_object_section("collection_rename");
	f->dump_stream("src_collection") << cid;
	f->dump_stream("dst_collection") << ncid;
	f->close_section();
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
	coll_t cid(i.get_cid());
	hobject_t oid = i.get_oid();
	f->open_object_section("omap_clear");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->close_section();
      }
      break;

    case Transaction::OP_OMAP_SETKEYS:
      {
	coll_t cid(i.get_cid());
	hobject_t oid = i.get_oid();
	map<string, bufferlist> aset;
	i.get_attrset(aset);
	f->open_object_section("omap_setkeys");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->open_object_section("attr_lens");
	for (map<string, bufferlist>::iterator p = aset.begin();
	     p != aset.end();
	     ++p)
	  f->dump_unsigned(p->first.c_str(), p->second.length());
	f->close_section();
	f->close_section();
      }
      break;

    case Transaction::OP_OMAP_RMKEYS:
      {
	coll_t cid(i.get_cid());
	hobject_t oid = i.get_oid();
	set<string> keys;
	i.get_keyset(keys);
	f->open_object_section("omap_rmkeys");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->close_section();
      }
      break;

    case Transaction::OP_OMAP_SETHEADER:
      {
	coll_t cid(i.get_cid());
	hobject_t oid = i.get_oid();
	bufferlist bl;
	i.get_bl(bl);
	f->open_object_section("omap_setheader");
	f->dump_stream("collection") << cid;
	f->dump_stream("oid") << oid;
	f->dump_stream("header_length") << bl.length();
	f->close_section();
      }
      break;

    default:
      f->open_object_section("unknown");
      f->dump_unsigned("opcode", op);
      f->close_section();
      return;
    }
    op_num++;
  }
  f->close_section();
}

void ObjectStore::Transaction::dump(ostream& out)
{
  iterator i = begin();
  int op_num = 0;
  while (i.have_op()) {
    int op = i.get_op();
    switch (op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	out << op_num << ": touch " << cid << " " << oid << "\n";
      }
      break;
      
    case Transaction::OP_WRITE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	bufferlist bl;
	i.get_bl(bl);
	out << op_num << ": write " << cid << " " << oid << " " << off << "~" << len << " (" << bl.length() << ")\n";
      }
      break;
      
    case Transaction::OP_ZERO:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	out << op_num << ": zero " << cid << " " << oid << " " << off << "~" << len << "\n";
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	out << op_num << ": trim_from_cache " << cid << " " << oid << " " << off << "~" << len << "\n";
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	out << op_num << ": truncate " << cid << " " << oid << " " << off << "\n";
      }
      break;
      
    case Transaction::OP_REMOVE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	out << op_num << ": remove " << cid << " " << oid << "\n";
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	string name = i.get_attrname();
	bufferlist bl;
	i.get_bl(bl);
	out << op_num << ": setattr " << cid << " " << oid << " " << name << " (" << bl.length() << ")\n";
      }
      break;
      
    case Transaction::OP_SETATTRS:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	map<string, bufferptr> aset;
	i.get_attrset(aset);
	out << op_num << ": setattrs " << cid << " " << oid << " " << aset << "\n";
      }
      break;

    case Transaction::OP_RMATTR:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	string name = i.get_attrname();
	out << op_num << ": rmattr " << cid << " " << oid << " " << name << "\n";
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	out << op_num << ": rmattrs " << cid << " " << oid << "\n";
      }
      break;
      
    case Transaction::OP_CLONE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	hobject_t noid = i.get_oid();
	out << op_num << ": clone " << cid << " " << oid << " -> " << noid << "\n";
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	hobject_t noid = i.get_oid();
 	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	out << op_num << ": clone_range " << cid << " " << oid << " -> " << noid << " " << off << "~" << len << " -> " << off << "\n";
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	hobject_t noid = i.get_oid();
 	uint64_t srcoff = i.get_length();
	uint64_t len = i.get_length();
 	uint64_t dstoff = i.get_length();
	out << op_num << ": clone_range " << cid << " " << oid << " -> " << noid << " " << srcoff << "~" << len << " -> " << dstoff << "\n";
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	coll_t cid = i.get_cid();
	out << op_num << ": mkcoll " << cid << "\n";
      }
      break;

    case Transaction::OP_RMCOLL:
      {
	coll_t cid = i.get_cid();
	out << op_num << ": rmcoll " << cid << "\n";
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ocid = i.get_cid();
	coll_t ncid = i.get_cid();
	hobject_t oid = i.get_oid();
	out << op_num << ": coll_add " << ocid << " " << ncid << " " << oid << "\n";
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
	coll_t cid = i.get_cid();
	hobject_t oid = i.get_oid();
	out << op_num << ": coll_remove " << cid << " " << oid << "\n";
       }
      break;

    case Transaction::OP_COLL_MOVE:
       {
	coll_t ocid = i.get_cid();
	coll_t ncid = i.get_cid();
	hobject_t oid = i.get_oid();
	out << op_num << ": coll_move " << ocid << " " << ncid << " " << oid << "\n";
       }
      break;

    case Transaction::OP_COLL_SETATTR:
      {
	coll_t cid = i.get_cid();
	string name = i.get_attrname();
	bufferlist bl;
	i.get_bl(bl);
	out << op_num << ": coll_setattr " << cid << " " << name << " (" << bl.length() << ")\n";
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = i.get_cid();
	string name = i.get_attrname();
	out << op_num << ": coll_rmattr " << cid << " " << name << "\n";
      }
      break;

    case Transaction::OP_STARTSYNC:
      out << op_num << ": startsync\n";
      break;

    case Transaction::OP_COLL_RENAME:
      {
	coll_t cid(i.get_cid());
	coll_t ncid(i.get_cid());
	out << op_num << ": coll_rename " << cid << " -> " << ncid << "\n";
      }
      break;
    case Transaction::OP_OMAP_CLEAR:
      {
	coll_t cid(i.get_cid());
	hobject_t oid = i.get_oid();
	out << op_num << ": tmap_clear " << cid << "   " << oid << "\n";
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
	coll_t cid(i.get_cid());
	hobject_t oid = i.get_oid();
	map<string, bufferlist> aset;
	i.get_attrset(aset);
	out << op_num << ": tmap_setkeys " << cid << "   " << oid << "\n";
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
	coll_t cid(i.get_cid());
	hobject_t oid = i.get_oid();
	set<string> keys;
	i.get_keyset(keys);
	out << op_num << ": tmap_rmkeys " << cid << "   " << oid << "\n";
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
	coll_t cid(i.get_cid());
	hobject_t oid = i.get_oid();
	bufferlist bl;
	i.get_bl(bl);
	out << op_num << ": tmap_setheader" << cid << "   " << oid << "\n";
      }
      break;

    default:
      out << op_num << ": unknown op code " << op << "\n";
      return;
    }
    op_num++;
  }
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
  hobject_t o1("obj", "", 123, 456);
  hobject_t o2("obj2", "", 123, 456);
  hobject_t o3("obj3", "", 123, 456);
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
