#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "messages/MClientRequest.h"

#define dout_subsys ceph_subsys_mds

ostream& operator<<(ostream& out, const CDentry& dn)
{
  return out;
}

CInode* CDentry::get_dir_inode() const
{
  return get_dir()->get_inode();
}

void CDentry::first_get()
{ 
}
void CDentry::last_put()
{
}

CDentry::linkage_t* CDentry::_project_linkage()
{
  // dirfrag must be locked
  dir->get_inode()->mutex_assert_locked_by_me();
  projected_linkages.push_back(linkage_t());
  return &projected_linkages.back();
}

void CDentry::push_projected_linkage(CInode *in)
{
  _project_linkage()->inode = in;
  in->push_projected_parent(this);
}

void CDentry::pop_projected_linkage()
{
  dir->get_inode()->mutex_assert_locked_by_me();
  assert(!projected_linkages.empty());
  linkage_t& n = projected_linkages.front();

  if (n.remote_ino) {
    dir->link_remote_inode(this, n.remote_ino, n.remote_d_type);
    if (n.inode) {
//      linkage.inode = n.inode;
//      linkage.inode->add_remote_parent(this);
    }
  } else if (n.inode) {
    CDentry* dn = n.inode->pop_projected_parent();
    assert(dn == this);
    dir->link_primary_inode(this, n.inode);
  }

  projected_linkages.pop_front();
}

void CDentry::link_inode_work(inodeno_t ino, uint8_t d_type)
{
  dir->get_inode()->mutex_assert_locked_by_me();
  assert(linkage.is_null());
  linkage.set_remote(ino, d_type);
}

void CDentry::link_inode_work(CInode *in)
{
  dir->get_inode()->mutex_assert_locked_by_me();
  in->mutex_assert_locked_by_me();

  assert(linkage.is_null());
  linkage.inode = in; 
  in->set_primary_parent(this);

  if (in->get_num_ref())
    get(CDentry::PIN_INODEPIN);
}

void CDentry::unlink_inode_work()
{
  dir->get_inode()->mutex_assert_locked_by_me();
  CInode *in = linkage.get_inode();

  if (linkage.is_remote()) {
    // remote
    linkage.set_remote(0, 0);
    if (in) {
      //dn->unlink_remote(dn->get_linkage());
    }
  } else {
    // primary
    assert(linkage.is_primary());
    in->mutex_assert_locked_by_me();

    // unpin dentry?
    if (in->get_num_ref())
      put(CDentry::PIN_INODEPIN);

    // detach inode
    in->remove_primary_parent(this);
    linkage.inode = 0;
  }
}

version_t CDentry::pre_dirty(version_t min)
{ 
  projected_version = dir->pre_dirty(min);
  dout(10) << " pre_dirty " << *this << dendl;
  return projected_version;
}

void CDentry::_mark_dirty(LogSegment *ls)
{ 
  // state+pin
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    dir->dec_num_dirty();
    get(PIN_DIRTY);
 //   assert(ls);
  }
//  if (ls) 
//    ls->dirty_dentries.push_back(&item_dirty);
}

void CDentry::mark_dirty(version_t pv, LogSegment *ls)
{ 
  dout(10) << " mark_dirty " << *this << dendl;

  // i now live in this new dir version
  assert(pv <= projected_version);
  version = pv;
  _mark_dirty(ls);

  // mark dir too
  dir->mark_dirty(pv, ls);
}


void CDentry::mark_clean()
{
  dout(10) << " mark_clean " << *this << dendl;
  assert(is_dirty());

  // state+pin
  state_clear(STATE_DIRTY | STATE_NEW);
  dir->dec_num_dirty();
  put(PIN_DIRTY);

//  item_dirty.remove_myself();
}

void CDentry::mark_new()
{
  dout(10) << " mark_new " << *this << dendl;
  state_set(STATE_NEW);
}

void CDentry::clear_new()
{
  dout(10) << " clear_new " << *this << dendl;
  state_set(STATE_NEW);
}
