// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#include <string.h>
#include <map>
#include <sstream>
#include <fstream>

#include "include/types.h"
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "osdc/Journaler.h"
#include "mds/mdstypes.h"
#include "mds/LogEvent.h"
#include "mds/InoTable.h"
#include "mds/CDentry.h"

#include "mds/events/ENoOp.h"
#include "mds/events/EUpdate.h"

#include "mds/JournalPointer.h"
// #include "JournalScanner.h"
// #include "EventOutput.h"
// #include "Dumper.h"
// #include "Resetter.h"

// #include "JournalTool.h"
#include "MetaTool.h"
#include "type_helper.hpp"
#include "include/object.h"

WRITE_RAW_ENCODER(char)
WRITE_RAW_ENCODER(unsigned char)

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": "

using namespace std;

void MetaTool::meta_op::release()
{
  for (const auto& i : inodes) {
    delete i.second;
  }

  while (!sub_ops.empty()) {
    delete sub_ops.top();
    sub_ops.pop();
  }
}

void MetaTool::inode_meta_t::decode_json(JSONObj *obj)
{
  unsigned long long tmp;
  JSONDecoder::decode_json("snapid_t", tmp, obj, true);
  _f.val = tmp;
  JSONDecoder::decode_json("itype", tmp, obj, true);
  _t = tmp;
  if (NULL == _i)
    _i = new InodeStore;
  JSONDecoder::decode_json("store", *_i, obj, true);
}

void MetaTool::usage()
{
  generic_client_usage();
}

int MetaTool::main(string& mode,
                   string& rank_str,
                   string& minfo,
                   string&ino,
                   string& out,
                   string& in,
                   bool confirm
                   )
{
  int r = 0;

  std::string manual_meta_pool;
  std::string manual_data_pool;
  std::string manual_rank_num;
  bool manual_mode = false;
  if (minfo != "") {
    vector<string> v;
    string_split(minfo, v);
    manual_meta_pool = v.size() >= 1 ? v[0] : "";
    manual_data_pool = v.size() >= 2 ? v[1] : "";
    manual_rank_num = v.size() >= 3 ? v[2] : "";
    std::cout << "("<< minfo<< ")=>"
              << " mpool: " << manual_meta_pool
              << " dpool: " << manual_data_pool
              << " rank: " << manual_rank_num
              << std::endl;
    if (!manual_meta_pool.empty() && !manual_data_pool.empty() && !manual_rank_num.empty()) {
      std::cout << "you specify rank: " << manual_rank_num
                << " mpool: " << manual_meta_pool
                << " dpool: " << manual_data_pool
                << "\nstart manual mode!!"<< std::endl;
      manual_mode = true;
    }
  }

  // RADOS init
  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    cerr << "RADOS unavailable" << std::endl;
    return r;
  }

  if (_debug)
    cout << "MetaTool: connecting to RADOS..." << std::endl;
  r = rados.connect();
  if (r < 0) {
    cerr << "couldn't connect to cluster: " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (!manual_mode) {
    r = role_selector.parse(*fsmap, rank_str);
    if (r != 0) {
      cerr << "Couldn't determine MDS rank." << std::endl;
      return r;
    }

    auto& fs = fsmap->get_filesystem(role_selector.get_ns());
    auto& mds_map = fs.get_mds_map();

    // prepare io for meta pool
    int64_t const pool_id = mds_map.get_metadata_pool();
    features = mds_map.get_up_features();
    if (features == 0)
      features = CEPH_FEATURES_SUPPORTED_DEFAULT;
    else if (features != CEPH_FEATURES_SUPPORTED_DEFAULT) {
      cout << "I think we need to check the feature! : " << features << std::endl;
      return -1;
    }

    std::string pool_name;
    r = rados.pool_reverse_lookup(pool_id, &pool_name);
    if (r < 0) {
      cerr << "Pool " << pool_id << " named in MDS map not found in RADOS!" << std::endl;
      return r;
    }

    if (_debug)
      cout << "MetaTool: creating IoCtx.." << std::endl;
    r = rados.ioctx_create(pool_name.c_str(), io_meta);
    assert(r == 0);
    output.dup(io_meta);

    // prepare io for data pool
    for (const auto p : mds_map.get_data_pools()) {
      r = rados.pool_reverse_lookup(p, &pool_name);
      if (r < 0) {
        cerr << "Pool " << pool_id << " named in MDS map not found in RADOS!" << std::endl;
        return r;
      }
      librados::IoCtx* io_data = new librados::IoCtx;
      r = rados.ioctx_create(pool_name.c_str(), *io_data);
      assert(r == 0);
      io_data_v.push_back(io_data);
    }

    for (auto role : role_selector.get_roles()) {
      rank = role.rank;

      r =  process(mode, ino, out, in, confirm);
      cout << "executing for rank " << rank << " op[" <<mode<< "] ret : " << r << std::endl;
    }

  } else {
    features = CEPH_FEATURES_SUPPORTED_DEFAULT;
    r = rados.ioctx_create(manual_meta_pool.c_str(), io_meta);
    assert(r == 0);

    librados::IoCtx* io_data = new librados::IoCtx;
    r = rados.ioctx_create(manual_data_pool.c_str(), *io_data);
    assert(r == 0);
    io_data_v.push_back(io_data);


    rank = conv_t<int>(manual_rank_num);
    r = process(mode, ino, out, in, confirm);
    cout << "op[" << mode << "] ret : " << r << std::endl;
  }
  return r;
}

int MetaTool::process(string& mode, string& ino, string out, string in, bool confirm)
{
  if (mode == "showm") {
    return show_meta_info(ino, out);
  } else if (mode == "showfn") {
    return show_fnode(ino, out);
  } else if (mode == "listc") {
    return list_meta_info(ino, out);
  } else if (mode == "amend") {
    return amend_meta_info(ino, in, confirm);
  } else if (mode == "amendfn") {
    return amend_fnode(in, confirm);
  } else {
    cerr << "bad command '" << mode << "'" << std::endl;
    return -EINVAL;
  }
}
int MetaTool::show_fnode(string& ino, string& out)
{
  if (ino != "0") {
    inodeno_t i_ino = std::stoull(ino.c_str(), nullptr, 0);
    meta_op op(_debug, out);
    meta_op::sub_op* nsop = new meta_op::sub_op(&op);
    nsop->sub_op_t = meta_op::OP_SHOW_FN;
    nsop->sub_ino_t = meta_op::INO_DIR;
    nsop->ino = i_ino;
    op.push_op(nsop);
    return op_process(op);
  } else {
    cerr << "parameter error? : ino = " << ino << std::endl;
  }
  return 0;
}
int MetaTool::amend_fnode(string& in, bool confirm)
{
  meta_op op(_debug, "", in, confirm);
  meta_op::sub_op* nsop = new meta_op::sub_op(&op);
  nsop->sub_op_t = meta_op::OP_AMEND_FN;
  nsop->sub_ino_t = meta_op::INO_DIR;
  nsop->ino = 0;
  op.push_op(nsop);
  return op_process(op);
}
int MetaTool::amend_meta_info(string& ino, string& in, bool confirm)
{
  if (ino != "0" && in != "") {
    inodeno_t i_ino = std::stoull(ino.c_str(), nullptr, 0);
    meta_op op(_debug, "", in, confirm);
    meta_op::sub_op* nsop = new meta_op::sub_op(&op);
    nsop->sub_op_t = meta_op::OP_AMEND;
    nsop->sub_ino_t = meta_op::INO_DIR;
    nsop->ino = i_ino;
    op.push_op(nsop);
    return op_process(op);
  } else {
    cerr << "parameter error? : ino = " << ino << std::endl;
  }
  return 0;
}
int MetaTool::list_meta_info(string& ino, string& out)
{
  if (ino != "0") {
    inodeno_t i_ino = std::stoull(ino.c_str(), nullptr, 0);
    meta_op op(_debug, out);
    meta_op::sub_op* nsop = new meta_op::sub_op(&op);
    nsop->sub_op_t = meta_op::OP_LIST;
    nsop->sub_ino_t = meta_op::INO_DIR;
    nsop->ino = i_ino;
    op.push_op(nsop);
    return op_process(op);
  } else {
    cerr << "parameter error? : ino = " << ino << std::endl;
  }
  return 0;
}
int MetaTool::show_meta_info(string& ino, string& out)
{
  if (ino != "0") {
    inodeno_t i_ino = std::stoull(ino.c_str(), nullptr, 0);
    meta_op op(_debug, out);

    meta_op::sub_op* nsop = new meta_op::sub_op(&op);
    nsop->sub_op_t = meta_op::OP_SHOW;
    nsop->sub_ino_t = meta_op::INO_DIR;
    nsop->ino = i_ino;
    op.push_op(nsop);
    return op_process(op);
  } else {
    cerr << "parameter error? : ino = " << ino << std::endl;
  }
  return 0;
}

int MetaTool::op_process(meta_op& op)
{
  int r = 0;
  while (!op.no_sops()) {
    if (_debug)
      std::cout << "process : " << op.top_op()->detail() << std::endl;
    switch(op.top_op()->sub_op_t) {
    case meta_op::OP_LIST:
      r = list_meta(op);
      break;
    case meta_op::OP_LTRACE:
      r = file_meta(op);
      break;
    case meta_op::OP_SHOW:
      r = show_meta(op);
      break;
    case meta_op::OP_AMEND:
      r = amend_meta(op);
      break;
    case meta_op::OP_SHOW_FN:
      r = show_fn(op);
      break;
    case meta_op::OP_AMEND_FN:
      r = amend_fn(op);
      break;
    default:
      cerr << "unknow op" << std::endl;
    }
    if (r == 0)
      op.pop_op();
    else if (r < 0)
      op.clear_sops();
  }
  op.release();
  return r;
}

int MetaTool::amend_meta(meta_op &op)
{
  meta_op::sub_op* sop = op.top_op();
  auto item = op.inodes.find(sop->ino);
  auto item_k = op.okeys.find(sop->ino);
  if (item != op.inodes.end() && item_k != op.okeys.end()) {
    if (_amend_meta(item_k->second, *(item->second), op.infile(), op) < 0)
      return -1;
  } else {
    if (op.inodes.empty()) {
      meta_op::sub_op* nsop = new meta_op::sub_op(&op);
      nsop->sub_op_t = meta_op::OP_LIST;
      nsop->sub_ino_t = meta_op::INO_DIR;
      nsop->trace_level = 0;
      nsop->ino_c = sop->ino;
      op.push_op(nsop);
      return 1;
    } else {
      return -1;
    }
  }
  return 0;
}

void MetaTool::inode_meta_t::encode(::ceph::bufferlist& bl, uint64_t features)
{
    ::encode(_f, bl);
    ::encode(_t, bl);
    _i->encode_bare(bl, features);
}
int MetaTool::_amend_meta(string& k, inode_meta_t& inode_meta, const string& fn, meta_op& op)
{
  JSONParser parser;
  if (!parser.parse(fn.c_str())) {
    cout << "Error parsing create user response" << std::endl;
    return -1;
  }
  
  try {
    inode_meta.decode_json(&parser);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -1;
  }
  
  if (!op.confirm_chg() || op.is_debug()) {
    cout << "you will amend info of inode ==>: " << std::endl;
    _show_meta(inode_meta, "");
  }
  
  if (!op.confirm_chg()) {
    cout << "warning: this operation is irreversibl!!!\n"
         << "         You must confirm that all logs of mds have been flushed!!!\n"
         << "         if you want amend it, please add --yes-i-really-really-mean-it!!!"
         << std::endl;
    return -1;
  }
  
  bufferlist bl;
  inode_meta.encode(bl, features);
  map<string, bufferlist> to_set;
  to_set[k].swap(bl);
  inode_backpointer_t bp;
  if (!op.top_op()->get_ancestor(bp))
    return -1;
  frag_t frag;
  auto item = op.inodes.find(bp.dirino);
  if (item != op.inodes.end()) {
    frag = item->second->get_meta()->pick_dirfrag(bp.dname);
  }
  string oid = obj_name(bp.dirino, frag);
  int ret = io_meta.omap_set(oid, to_set);
  to_set.clear();
  return ret;
}
int MetaTool::show_fn(meta_op &op)
{
  meta_op::sub_op* sop = op.top_op();
  auto item = op.inodes.find(sop->ino);
  if (item != op.inodes.end()) {
    if (_show_fn(*(item->second), op.outfile()) < 0)
      return -1;
  } else {
    if (op.inodes.empty()) {
      meta_op::sub_op* nsop = new meta_op::sub_op(&op);
      nsop->sub_op_t = meta_op::OP_LIST;
      nsop->sub_ino_t = meta_op::INO_DIR;
      nsop->trace_level = 0;
      nsop->ino_c = sop->ino;
      op.push_op(nsop);
      return 1;
    } else
      return -1;
  }
  return 0;
}
int MetaTool::_show_fn(inode_meta_t& inode_meta, const string& fn)
{
  std::list<frag_t> frags;
  inode_meta.get_meta()->dirfragtree.get_leaves(frags);
  std::stringstream ds;
  std::string format = "json";
  std::string oids;
  Formatter* f = Formatter::create(format);
  f->enable_line_break();
  f->open_object_section("fnodes");
  for (const auto &frag : frags) {
    bufferlist hbl;
    string oid = obj_name(inode_meta.get_meta()->inode->ino, frag);
    int ret = io_meta.omap_get_header(oid, &hbl);
    if (ret < 0) {
      std::cerr << __func__ << " : can't find oid("<< oid << ")" << std::endl;
      return -1;
    }
    {
      fnode_t got_fnode;
      try {
        auto p = hbl.cbegin();
        ::decode(got_fnode, p);
      } catch (const buffer::error &err) {
        cerr << "corrupt fnode header in " << oid
             << ": " << err.what() << std::endl;
        return -1;
      }
      if (!oids.empty())
        oids += ",";
      oids += oid;
      f->open_object_section(oid.c_str());
      got_fnode.dump(f);
      f->close_section();
    }
  }
  f->dump_string("oids", oids.c_str());
  f->close_section();
  f->flush(ds);
  if (fn != "") {
    ofstream o;
    o.open(fn);
    if (o) {
      o << ds.str();
      o.close();
    } else {
      cout << "out to file (" << fn << ") failed" << std::endl;
      cout << ds.str() << std::endl;
    }
  } else
    std::cout << ds.str() << std::endl;
  return 0;
}
int MetaTool::amend_fn(meta_op &op)
{
  if (_amend_fn(op.infile(), op.confirm_chg()) < 0)
    return -1;
  return 0;
}
int MetaTool::_amend_fn(const string& fn, bool confirm)
{
  JSONParser parser;
  if (!parser.parse(fn.c_str())) {
    cout << "Error parsing create user response : " << fn << std::endl;
    return -1;
  }
  if (!confirm) {
    cout << "warning: this operation is irreversibl!!!\n"
         << "         You must confirm that all logs of mds have been flushed!!!\n"
         << "         if you want amend it, please add --yes-i-really-really-mean-it!!!"
         << std::endl;
    return -1;
  }
  try {
    string tmp;
    JSONDecoder::decode_json("oids", tmp, &parser, true);
    string::size_type pos1, pos2;
    vector<string> v;
    string c = ",";
    pos2 = tmp.find(c);
    pos1 = 0;
    while (string::npos != pos2) {
      v.push_back(tmp.substr(pos1, pos2-pos1));
      pos1 = pos2 + c.size();
      pos2 = tmp.find(c, pos1);
    }
    if (pos1 != tmp.length())
      v.push_back(tmp.substr(pos1));
    int ret = 0;
    for (auto i : v) {
      cout << "amend frag : " << i << "..." << std::endl;
      fnode_t fnode;
      JSONDecoder::decode_json(i.c_str(), fnode, &parser, true);
      bufferlist bl;
      fnode.encode(bl);
      ret = io_meta.omap_set_header(i, bl);
      if (ret < 0)
        return ret;
    }
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -1;
  }
  return 0;
}
int MetaTool::show_meta(meta_op &op)
{
  meta_op::sub_op* sop = op.top_op();
  auto item = op.inodes.find(sop->ino);
  if (item != op.inodes.end()) {
    if (_show_meta(*(item->second), op.outfile()) < 0)
      return -1;
  } else {
    if (op.inodes.empty()) {
      meta_op::sub_op* nsop = new meta_op::sub_op(&op);
      nsop->sub_op_t = meta_op::OP_LIST;
      nsop->sub_ino_t = meta_op::INO_DIR;
      nsop->trace_level = 0;
      nsop->ino_c = sop->ino;
      op.push_op(nsop);
      return 1;
    } else {
      return -1;
    }
  }
  return 0;
}
int MetaTool::_show_meta(inode_meta_t& inode_meta, const string& fn)
{
  std::stringstream ds;
  std::string format = "json";
  InodeStore& inode_data = *inode_meta.get_meta();
  Formatter* f = Formatter::create(format);
  f->enable_line_break();
  f->open_object_section("meta");
  f->dump_unsigned("snapid_t", inode_meta.get_snapid());
  f->dump_unsigned("itype", inode_meta.get_type());
  f->open_object_section("store");
  inode_data.dump(f);
  try {
    if (inode_data.snap_blob.length()) {
      sr_t srnode;
      auto p = inode_data.snap_blob.cbegin();
      decode(srnode, p);
      f->open_object_section("snap_blob");
      srnode.dump(f);
      f->close_section();
    }
  } catch (const buffer::error &err) {
    cerr << "corrupt decode in snap_blob"
         << ": " << err.what() << std::endl;
    return -1;
  }

  f->close_section();
  f->close_section();
  f->flush(ds);

  if (fn != "") {
    ofstream o;
    o.open(fn);
    if (o) {
      o << ds.str();
      o.close();
    } else {
      cout << "out to file (" << fn << ") failed" << std::endl;
      cout << ds.str() << std::endl;
    }

  } else
    std::cout << ds.str() << std::endl;
  return 0;
}
int MetaTool::list_meta(meta_op &op)
{
  meta_op::sub_op* sop = op.top_op();

  bool list_all = false;
  string oid;
  inodeno_t ino = sop->ino_c;
  frag_t frag = sop->frag;

  if (sop->ino_c == 0) {
    list_all = true;
    oid = obj_name(sop->ino, frag);
  } else {
    if (_debug)
      std::cout << __func__ << " : " << sop->trace_level << " " << op.ancestors.size() << std::endl;
    inode_backpointer_t bp;
    if (sop->get_c_ancestor(bp)) {
      auto item = op.inodes.find(bp.dirino);
      if (item != op.inodes.end()) {
        frag = item->second->get_meta()->pick_dirfrag(bp.dname);
      }
      oid = obj_name(bp.dirino, frag);
    } else {
      meta_op::sub_op* nsop = new meta_op::sub_op(&op);
      nsop->ino = sop->ino_c;
      nsop->sub_op_t = meta_op::OP_LTRACE;
      nsop->sub_ino_t = meta_op::INO_DIR;
      op.push_op(nsop);
      return 1;
    }
  }
  if (_debug)
    std::cout << __func__ << " : " << string(list_all?"listall ":"info ") << oid << " "<< ino << std::endl;
  bufferlist hbl;
  int ret = io_meta.omap_get_header(oid, &hbl);
  if (ret < 0) {
    std::cerr << __func__ << " : can't find it, maybe it (ino:"<< sop->ino<< ")isn't a normal dir!" << std::endl;
    return -1;
  }

  if (hbl.length() == 0) {   // obj has splite
    if (list_all) {
      if (frag == frag_t()) {
        auto item = op.inodes.find(sop->ino);
        if (item != op.inodes.end()) {
            inodeno_t tmp = sop->ino;
            op.pop_op();
            std::list<frag_t> frags;
            item->second->get_meta()->dirfragtree.get_leaves(frags);
            for (const auto &frag : frags) {
              meta_op::sub_op* nsop = new meta_op::sub_op(&op);
              nsop->ino = tmp;
              nsop->sub_op_t = meta_op::OP_LIST;
              nsop->sub_ino_t = meta_op::INO_DIR;
              nsop->frag = frag;
              op.push_op(nsop);
            }
        } else {
          meta_op::sub_op* nsop = new meta_op::sub_op(&op);
          nsop->ino_c = sop->ino;
          nsop->sub_op_t = meta_op::OP_LIST;
          nsop->sub_ino_t = meta_op::INO_DIR;
          op.push_op(nsop);
        }
        return 1;
      } else {
        cerr << __func__ << " missing some data (" << oid << ")???" << std::endl;
        return -1;
      }
    } else {
      if (frag == frag_t()) {
        inode_backpointer_t bp;
        if (sop->get_c_ancestor(bp)) {
          meta_op::sub_op* nsop = new meta_op::sub_op(&op);
          nsop->ino_c = bp.dirino;
          nsop->sub_op_t = meta_op::OP_LIST;
          nsop->sub_ino_t = meta_op::INO_DIR;
          nsop->trace_level = sop->trace_level + 1;
          op.push_op(nsop);
          return 1;
        } else {
          cerr << __func__ << "can't find obj(" << oid << ") ,miss ancestors or miss some objs??? " << std::endl;
          return -1;
        }
      } else {
        cerr << __func__ << "missing some objs(" << oid << ")??? " << std::endl;
        return -1;
      }
    }
  }

  fnode_t got_fnode;
  try {
    auto p = hbl.cbegin();
    ::decode(got_fnode, p);
  } catch (const buffer::error &err) {
    cerr << "corrupt fnode header in " << oid
         << ": " << err.what() << std::endl;
    return -1;
  }

  if (_debug) {
    std::string format = "json";
    Formatter* f = Formatter::create(format);
    f->enable_line_break();
    f->dump_string("type", "--fnode--");
    f->open_object_section("fnode");
    got_fnode.dump(f);
    f->close_section();
    f->flush(std::cout);
    std::cout << std::endl;
  }

  // print children
  std::map<string, bufferlist> out_vals;
  int max_vals = 5;
  io_meta.omap_get_vals(oid, "", max_vals, &out_vals);

  bool force_dirty = false;
  const set<snapid_t> *snaps = NULL;
  unsigned pos = out_vals.size() - 1;
  std::string last_dname;
  for (map<string, bufferlist>::iterator p = out_vals.begin();
       p != out_vals.end();
       ++p, --pos) {
    string dname;
    snapid_t last;
    dentry_key_t::decode_helper(p->first, dname, last);
    if (_debug)
      last_dname = dname;
    try {
      if (!list_all) {
        if (show_child(p->first, dname, last, p->second, pos, snaps,
                       &force_dirty, ino, &op) == 1) {
          return 0;
        }
      } else {
        cout << "dname : " << dname << " " << last << std::endl;
        if (show_child(p->first, dname, last, p->second, pos, snaps,
                       &force_dirty) == 1)
          return 0;
      }
    } catch (const buffer::error &err) {
      derr << "Corrupt dentry '" << dname << "' : "
           << err.what() << "(" << "" << ")" << dendl;
      return -1;
    }
  }
  while (out_vals.size() == (size_t)max_vals) {
    out_vals.clear();
    io_meta.omap_get_vals(oid, last_dname, max_vals, &out_vals);
    pos = out_vals.size() - 1;
    for (map<string, bufferlist>::iterator p = (++out_vals.begin());
         p != out_vals.end();
         ++p, --pos) {
      string dname;
      snapid_t last;
      dentry_key_t::decode_helper(p->first, dname, last);
      last_dname = dname;
      try {
        if (!list_all) {
          if (show_child(p->first, dname, last, p->second, pos, snaps,
                         &force_dirty, ino, &op) == 1) {
            return 0;
          }
        } else {
          cout << "dname : " << dname << " " << last << std::endl;
          if (show_child(p->first, dname, last, p->second, pos, snaps,
                         &force_dirty) == 1)
            return 0;
        }
      } catch (const buffer::error &err) {
          derr << "Corrupt dentry '" << dname << "' : "
               << err.what() << "(" << "" << ")" << dendl;
          return -1;
      }
    }
  }

  if (!list_all) {
    cerr << __func__ << "miss obj(ino:" << ino << ")??? " << std::endl;
    return -1;
  }
  return 0;
}

int MetaTool::file_meta(meta_op &op)
{
  int r = 0;
  if (op.top_op()->sub_ino_t ==  meta_op::INO_DIR) {
    r = _file_meta(op, io_meta);
  } else if (op.top_op()->sub_ino_t == meta_op::INO_F) {
    for (auto i = io_data_v.begin(); i != io_data_v.end(); ++i)
      if ((r = _file_meta(op, **i)) == 1)
        break;
  }
  if (r == 1) {
    inode_backpointer_t bp;
    if (op.top_op()->get_ancestor(bp)) {
      return 0;
    } else {
      std::cerr << "no trace for obj (ino:" << op.top_op()->ino <<")??" << std::endl;
      return -1;
    }
  } else if (op.top_op()->sub_ino_t == meta_op::INO_DIR) {
    std::cerr << "\tmaybe it's a file(ino:" << op.top_op()->ino << ")" << std::endl;
    op.top_op()->sub_ino_t = meta_op::INO_F;
    return 1;
  }
    
  std::cerr << "can't get (ino:" << op.top_op()->ino <<")trace??" << std::endl;
  return -1;
}

int MetaTool::_file_meta(meta_op &op, librados::IoCtx& io)
{
  inodeno_t ino = op.top_op()->ino;
  std::string oid = obj_name(ino);
  bufferlist pointer_bl;
  std::map<std::string, bufferlist> attrset;
  int r = 0;
  bool have_data = false;
  r = io.getxattrs (oid.c_str(), attrset);
  if (0 == r) {
    std::stringstream ds;
    std::string format = "json";
    Formatter* f = Formatter::create(format);
    auto item = attrset.find("parent");
    if (item != attrset.end()) {
      inode_backtrace_t i_bt;
      try {
        bufferlist::const_iterator q = item->second.cbegin();
        i_bt.decode(q);
        f->open_array_section("info");
        have_data = true;
        if (i_bt.ancestors.size() > 0)
          op.ancestors[ino] = i_bt.ancestors[0];
        f->dump_string("type", "--i_bt--");
        f->open_object_section("parent");
        i_bt.dump(f);
        f->close_section();
      } catch (buffer::error &e) {
        cerr << "failed to decode parent of " << oid << std::endl;
        return -1;
      }
    } else {
      cerr << oid << " in " << io.get_pool_name()  << " , but no parent" << std::endl;
      return -1;
    }

    item = attrset.find("layout");
    if (item != attrset.end()) {
      file_layout_t layout;
      try {
        auto q = item->second.cbegin();
        layout.decode(q);
        f->dump_string("type", "--layout--");
        f->open_object_section("layout");
        layout.dump(f);
        f->close_section();

      } catch (buffer::error &e) {
        cerr << "failed to decode layout of " << oid << std::endl;
        return -1;
      }
    } else {
      cerr << oid << " in " << io.get_pool_name()  << " , but no layout" << std::endl;
    }
    if (have_data) {
      f->close_section();
      f->flush(ds);
      if (_debug)
        cout << ino << " : "<< ds.str() << std::endl;
      return 1;
    }
  }
  return 0;
}
std::string MetaTool::obj_name(inodeno_t ino, uint64_t offset, const char *suffix) const
{
    char name[60];
  snprintf(name, sizeof(name), "%llx.%08llx%s", (long long unsigned)ino, (long long unsigned)offset, suffix ? suffix : "");
  return std::string(name);
}
std::string MetaTool::obj_name(inodeno_t ino, frag_t fg, const char *suffix) const
{
  char name[60];
  snprintf(name, sizeof(name), "%llx.%08llx%s", (long long unsigned)ino, (long long unsigned)fg, suffix ? suffix : "");
  return std::string(name);
}

std::string MetaTool::obj_name(const char* ino, uint64_t offset, const char *suffix) const
{
  char name[60];
  snprintf(name, sizeof(name), "%s.%08llx%s", ino, (long long unsigned)offset, suffix ? suffix : "");
  std::string out = name;
  transform(out.begin(), out.end(), out.begin(),::tolower);
  return out;
}

int MetaTool::show_child(std::string_view key,
                         std::string_view dname,
                         const snapid_t last,
                         bufferlist &bl,
                         const int pos,
                         const std::set<snapid_t> *snaps,
                         bool *force_dirty,
                         inodeno_t sp_ino,
                         meta_op* op)
{
  bufferlist::const_iterator q = bl.cbegin();

  snapid_t first;
  ::decode(first, q);

  // marker
  char type;
  ::decode(type, q);

  if (_debug)
    std::cout << pos << " type '" << type << "' dname '" << dname
              << " [" << first << "," << last << "]"
              << std::endl;
  // bool stale = false;
  if (snaps && last != CEPH_NOSNAP) {
    derr << "!!!! erro !!!!" << dendl;
    return -1;
  }

  // CDentry *dn = NULL;
  // look for existing dentry for _last_ snap, can't process snap of obj
  //if *(stale)
  //    dn = lookup_exact_snap(dname, last);
  //else
  //    dn = lookup(dname, last);
  if (type == 'L' || type == 'l') {
    // hard link
    inodeno_t ino;
    unsigned char d_type;
    mempool::mds_co::string alternate_name;

    CDentry::decode_remote(type, ino, d_type, alternate_name, q);

    if (sp_ino > 0) {
      if (sp_ino == ino) {
        std::cout << "find hard link : " << ino << "," << d_type << std::endl;
        return 1;
      }
    }

    std::cout << "hard link : " << ino << "," << d_type << std::endl;
  } else if (type == 'I' || type == 'i') {
    // inode
    // load inode data before lookuping up or constructing CInode
    InodeStore& inode_data = *(new InodeStore);
    if (type == 'i') {
      mempool::mds_co::string alternate_name;

      DECODE_START(2, q);
      if (struct_v >= 2)
        decode(alternate_name, q);
      inode_data.decode(q);
      DECODE_FINISH(q);
    } else {
      inode_data.decode_bare(q);
    }

    std::stringstream ds;
    std::string format = "json";
    Formatter* f = Formatter::create(format);
    f->enable_line_break();
    f->open_object_section("meta");
    f->dump_unsigned("snapid_t", first);
    f->dump_unsigned("itype", type);
    f->open_object_section("store");
    inode_data.dump(f);
    try {
      if (inode_data.snap_blob.length()) {
        sr_t srnode;
        auto p = inode_data.snap_blob.cbegin();
        srnode.decode(p);
        f->open_object_section("snap_blob");
        srnode.dump(f);
        f->close_section();
      }
    } catch (const buffer::error &err) {
      cerr << "corrupt decode in snap_blob"
           << ": " << err.what() << std::endl;
    }
    f->close_section();
    f->close_section();
    f->flush(ds);

    if (sp_ino > 0 && op != NULL && sp_ino == inode_data.inode->ino) {
      inode_meta_t* tmp = new inode_meta_t(first, type, &inode_data);
      op->inodes[inode_data.inode->ino] = tmp;
      op->okeys[inode_data.inode->ino] = key.data();
      return 1;
    } else {
      delete &inode_data;
    }

    if (sp_ino == 0) {
      cout << ds.str() << std::endl;
    }
    } else {
      std::cerr << __func__ << "unknow type : " << dname << "," << type << std::endl;
    }
  return 0;
}
