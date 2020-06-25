// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef METATOOL_H__
#define METATOOL_H__

#include "MDSUtility.h"
#include "RoleSelector.h"
#include <vector>
#include <stack>
using std::stack;
#include "mds/mdstypes.h"
#include "mds/LogEvent.h"
#include "mds/events/EMetaBlob.h"

#include "include/rados/librados.hpp"
#include "common/ceph_json.h"

using ::ceph::bufferlist;
class MetaTool : public MDSUtility
{
public:
  class inode_meta_t {
  public:
    inode_meta_t(snapid_t f = CEPH_NOSNAP, char t = 255, InodeStore* i = NULL):
        _f(f),_t(t),_i(i) {
    };
    snapid_t get_snapid() const { 
      return _f;
    }
    InodeStore* get_meta() const {
      if (_t == 'I')
        return _i;
      else
        return NULL;
    }
    int get_type() const {
      return _t;
    }
    void decode_json(JSONObj *obj);
    void encode(::ceph::bufferlist& bl, uint64_t features);
  private:
    snapid_t _f;
    char _t;
    InodeStore* _i;
  };
private:
  class meta_op {
  public:
    meta_op(bool debug = false, string out = "", string in = "", bool confirm = false):
        _debug(debug),
        _out(out),
        _in(in),
        _confirm(confirm)
      {}
    void release();
    typedef enum {
      OP_LIST = 0,
      OP_LTRACE,
      OP_SHOW,
      OP_AMEND,
      OP_SHOW_FN,
      OP_AMEND_FN,
      OP_NO
    } op_type;

    typedef enum {
      INO_DIR = 0,
      INO_F
    } ino_type;

    static string op_type_name(op_type& t) {
      string name;
      switch (t) {
      case OP_LIST:
        name = "list dir";
        break;
      case OP_LTRACE:
        name = "load trace";
        break;
      case OP_SHOW:
        name = "show info";
        break;
      case OP_AMEND:
        name = "amend info";
        break;
      case OP_SHOW_FN:
        name = "show fnode";
        break;
      case OP_AMEND_FN:
        name = "amend fnode";
        break;
      case OP_NO:
        name = "noop";
        break;
      default:
        name = "unknow op type";
      }
      return name;
    }
    static string ino_type_name(ino_type& t) {
      string name;
      switch (t) {
      case INO_DIR:
        name = "dir";
        break;
      case INO_F:
        name = "file";
        break;
      default:
        name = "unknow file type";
      }
      return name;
    }
    class sub_op {
    public:
      sub_op(meta_op* mop):
          trace_level(0),
          _proc(false),
          _mop(mop)
        {}
      void print() {
        std::cout << detail() << std::endl;
      }
      string detail() {
        std::stringstream ds;
        ds << " [sub_op]" << op_type_name(sub_op_t) << "|"
           << ino_type_name(sub_ino_t) << "|"
           << ino << "|"
           << frag << "|"
           << ino_c << "|"
           << trace_level << "|"
           << name;
        return ds.str();
      }
      bool get_c_ancestor(inode_backpointer_t& bp) {
        if (!_mop || !ino_c)
          return false;
        auto item = _mop->ancestors.find(ino_c);
        if (item != _mop->ancestors.end()) {
          bp = item->second;
          return true;
        } else
          return false;
      }
      bool get_ancestor(inode_backpointer_t& bp) {
        if (!_mop || !ino)
          return false;
        auto item = _mop->ancestors.find(ino);
        if (item != _mop->ancestors.end()) {
          bp = item->second;
          return true;
        } else
          return false;
      }
      op_type sub_op_t;
      ino_type sub_ino_t;
      inodeno_t ino;
      frag_t frag;
      inodeno_t ino_c;
      unsigned trace_level;
      std::string name;
      bool _proc;
      meta_op* _mop;
    };
      
    std::map<inodeno_t, inode_backpointer_t > ancestors;
    std::map<inodeno_t, inode_meta_t* > inodes;
    std::map<inodeno_t, string > okeys;
      
    void clear_sops() {
      while(!no_sops())
        pop_op();
    }
    bool no_sops() {
      return sub_ops.empty();
    }
    void push_op(sub_op* sop) {
      if (_debug)
        std::cout << "<<====" << sop->detail() << std::endl;
      sub_ops.push(sop);
    }
    sub_op* top_op() {
      return sub_ops.top();
    }
    void pop_op() {
      sub_op* sop = sub_ops.top();
      if (_debug)
        std::cout << "====>>" << sop->detail() << std::endl;;
      delete sop;
      sub_ops.pop();
    }
    string outfile() {
      return _out;
    }
    string infile() {
      return _in;
    }
    bool is_debug() {
      return _debug;
    }
    bool confirm_chg() {
      return _confirm;
    }
  private:
    stack<sub_op*> sub_ops;
    bool _debug;
    string _out;
    string _in;
    bool _confirm;
  };
  MDSRoleSelector role_selector;
  mds_rank_t rank;
    
  // I/O handles
  librados::Rados rados;
  librados::IoCtx io_meta;
  std::vector<librados::IoCtx*> io_data_v;
  librados::IoCtx output;
  bool _debug;
  uint64_t features;

  std::string obj_name(inodeno_t ino, frag_t fg = frag_t(), const char *suffix = NULL) const;
  std::string obj_name(inodeno_t ino, uint64_t offset, const char *suffix = NULL) const;
  std::string obj_name(const char* ino, uint64_t offset, const char *suffix = NULL) const;

  // 0 : continue to find 
  // 1 : stop to find it
  int show_child(std::string_view key,
                 std::string_view dname,
                 const snapid_t last,
                 bufferlist &bl,
                 const int pos,
                 const std::set<snapid_t> *snaps,
                 bool *force_dirty,
                 inodeno_t sp_ino = 0,
                 meta_op* op = NULL
                 );

  int process(string& mode, string& ino, string out, string in, bool confirm);
  int show_meta_info(string& ino, string& out);
  int list_meta_info(string& ino, string& out);
  int amend_meta_info(string& ino, string& in, bool confirm);
  int show_fnode(string& ino, string& out);
  int amend_fnode(string& in, bool confirm);
  int op_process(meta_op &op);
  int list_meta(meta_op &op);
  int file_meta(meta_op &op);
  int show_meta(meta_op &op);
  int amend_meta(meta_op &op);
  int show_fn(meta_op &op);
  int amend_fn(meta_op &op);
  public:
  int _file_meta(meta_op &op, librados::IoCtx& io);
  int _show_meta(inode_meta_t& i, const string& fn);
  int _amend_meta(string &k, inode_meta_t& i, const string& fn, meta_op& op);
  int _show_fn(inode_meta_t& i, const string& fn);
  int _amend_fn(const string& fn, bool confirm);
  static unsigned long long conv2hexino(const char* ino);
  void usage();
  MetaTool(bool debug=false):
      _debug(debug) {}
  ~MetaTool() {}

  int main(string& mode,
           string& rank_str,
           string& minfo,
           string&ino,
           string& out,
           string& in,
           bool confirm = false
           );
};
#endif // METATOOL_H__
