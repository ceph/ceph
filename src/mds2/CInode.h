#ifndef CEPH_CINODE_H
#define CEPH_CINODE_H
#include "mds/mdstypes.h"
#include "CObject.h"

class LogSegment;
class Session;
class MDCache;

typedef std::map<string, bufferptr> xattr_map_t;

class CInode : public CObject {
protected:
  MDCache *mdcache;

  inode_t inode;
  std::string symlink;
  xattr_map_t xattrs;

  struct projected_inode_t {
    inode_t inode;
    xattr_map_t xattrs;
    bool xattrs_projected;
    projected_inode_t(const inode_t &i, const xattr_map_t *px)
      : inode(i), xattrs_projected(false) { 
      if (px) {
	xattrs = *px;
	xattrs_projected = true;
      }
    }
  };
  list<projected_inode_t> projected_nodes;

  CDentry *parent;
  list<CDentry*> projected_parent;

  CDir *dir;

  void first_get();
  void last_put();
public:
  // pins
  static const int PIN_DIRFRAG =		-1;
  // states
  static const unsigned STATE_FREEING =		(1<<0);

  CInode(MDCache *_mdcache) :
    CObject("CInode"), mdcache(_mdcache), parent(NULL), dir(NULL) {}

  inodeno_t ino() const { return inode.ino; }
  vinodeno_t vino() const { return vinodeno_t(inode.ino, CEPH_NOSNAP); }
  uint8_t d_type() const { return IFTODT(inode.mode); }

  bool is_root() const { return ino() == MDS_INO_ROOT; }
  bool is_stray() const { return MDS_INO_IS_STRAY(ino()); }
  bool is_mdsdir() const { return MDS_INO_IS_MDSDIR(ino()); }
  bool is_base() const { return is_root() || is_mdsdir(); }
  bool is_system() const { return ino() < MDS_INO_SYSTEM_BASE; }
  mds_rank_t get_stray_owner() const {
    return (mds_rank_t)MDS_INO_STRAY_OWNER(ino());
  }

  bool is_dir() const { return inode.is_dir(); }
  bool is_file() const { return inode.is_file(); }
  bool is_symlink() const { return inode.is_symlink(); }


  inode_t* __get_inode() {  return &inode; }
  const inode_t* get_inode() const {  return &inode; }
  const inode_t* get_projected_inode() const {
    mutex_assert_locked_by_me();
    if (projected_nodes.empty())
      return &inode;
    else
      return &projected_nodes.back().inode;
  }
  xattr_map_t* __get_xattrs() { return &xattrs; }
  const xattr_map_t* get_xattrs() const { return &xattrs; }
  const xattr_map_t* get_projected_xattrs() const {
    mutex_assert_locked_by_me();
    for (auto p = projected_nodes.rbegin(); p != projected_nodes.rend(); ++p)
      if (p->xattrs_projected)
	return &p->xattrs;
    return &xattrs;
  }
  version_t get_version() const { return inode.version; }
  version_t get_projected_version() const {
    if (projected_nodes.empty())
      return inode.version;
    else
      return projected_nodes.back().inode.version;
  }
  void __set_symlink(const std::string& s) { symlink = s; }
  const std::string& get_symlink() const { return symlink; }

  bool is_projected() const { return !projected_nodes.empty(); }
  inode_t *project_inode(xattr_map_t **ppx=0);
  void pop_and_dirty_projected_inode(LogSegment *ls);

  version_t pre_dirty();
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t projected_dirv, LogSegment *ls);
  void mark_clean();

  void set_primary_parent(CDentry *p) {
    assert(parent == NULL);
    parent = p;
  }
  void remove_primary_parent(CDentry *dn) {
    assert(dn == parent);
    parent = NULL;
  }

  CDentry* get_parent_dn() const { return parent; }
  CDentry* get_projected_parent_dn() const {
    mutex_assert_locked_by_me();
    if (projected_parent.empty())
      return parent;
    return  projected_parent.back();
  }

  void push_projected_parent(CDentry *dn) {
    mutex_assert_locked_by_me();
    projected_parent.push_back(dn);
  }
  CDentry* pop_projected_parent() {
    mutex_assert_locked_by_me();
    assert(!projected_parent.empty());
    CDentry *dn = projected_parent.front();
    projected_parent.pop_front();
    return dn;
  }

  CDentryRef get_lock_parent_dn();
  CDentryRef get_lock_projected_parent_dn();
  bool is_projected_ancestor_of(CInode *other) const;

  frag_t pick_dirfrag(const string &dn) const {
    return frag_t();
  }
  CDirRef get_dirfrag(frag_t fg) {
    mutex_assert_locked_by_me();
    assert(fg == frag_t());
    return CDirRef(dir);
  }
  CDirRef get_or_open_dirfrag(frag_t fg);

  int encode_inodestat(bufferlist& bl, Session *session, unsigned max_bytes);

  void name_stray_dentry(string& dname) const;
};

ostream& operator<<(ostream& out, const CInode& in);
#endif
