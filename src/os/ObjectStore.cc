
#include "ObjectStore.h"



void ObjectStore::Transaction::dump(ostream& out)
{
  iterator i = begin();
  int op_num = 0;
  while (i.have_op()) {
    int op = i.get_op();
    op_num++;
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

    default:
      out << op_num << ": unknown op code " << op << "\n";
      return;
    }
  }
}

