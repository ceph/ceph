#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/sched.h>
#include <linux/wait.h>

#include "ceph_debug.h"

int ceph_debug_caps = -1 __read_mostly;
#define DOUT_MASK DOUT_MASK_CAPS
#define DOUT_VAR ceph_debug_caps
#include "super.h"

#include "decode.h"
#include "messenger.h"


#define MAX_CAP_STR 20
static char cap_str[MAX_CAP_STR][40];
static spinlock_t cap_str_lock = SPIN_LOCK_UNLOCKED;
static int last_cap_str;

static char *gcap_string(char *s, int c)
{
	if (c & CEPH_CAP_GRDCACHE)
		*s++ = 'c';
	if (c & CEPH_CAP_GEXCL)
		*s++ = 'x';
	if (c & CEPH_CAP_GRD)
		*s++ = 'r';
	if (c & CEPH_CAP_GWR)
		*s++ = 'w';
	if (c & CEPH_CAP_GWRBUFFER)
		*s++ = 'b';
	if (c & CEPH_CAP_GLAZYIO)
		*s++ = 'l';
	return s;
}

const char *ceph_cap_string(int caps)
{
	int i;
	char *s;
	int c;

	spin_lock(&cap_str_lock);
	i = last_cap_str++;
	if (last_cap_str == MAX_CAP_STR)
		last_cap_str = 0;
	spin_unlock(&cap_str_lock);

	s = cap_str[i];

	if (caps & CEPH_CAP_PIN)
		*s++ = 'p';

	c = (caps >> CEPH_CAP_SAUTH) & 3;
	if (c) {
		*s++ = 'A';
		s = gcap_string(s, c);
	}

	c = (caps >> CEPH_CAP_SLINK) & 3;
	if (c) {
		*s++ = 'L';
		s = gcap_string(s, c);
	}

	c = (caps >> CEPH_CAP_SXATTR) & 3;
	if (c) {
		*s++ = 'X';
		s = gcap_string(s, c);
	}

	c = caps >> CEPH_CAP_SFILE;
	if (c) {
		*s++ = 'F';
		s = gcap_string(s, c);
	}
	
	if (s == cap_str[i])
		*s++ = '-';
	*s = 0;
	return cap_str[i];
}


/*
 * Find ceph_cap for given mds, if any.
 *
 * Called with i_lock held.
 */
static struct ceph_cap *__get_cap_for_mds(struct inode *inode, int mds)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_cap *cap;
	struct rb_node *n = ci->i_caps.rb_node;

	while (n) {
		cap = rb_entry(n, struct ceph_cap, ci_node);
		if (mds < cap->mds)
			n = n->rb_left;
		else if (mds > cap->mds)
			n = n->rb_right;
		else
			return cap;
	}
	return NULL;
}

/*
 * Return id of any MDS with a cap, preferably WR|WRBUFFER|EXCL, else
 * -1.
 */
static int __ceph_get_cap_mds(struct ceph_inode_info *ci, u32 *mseq)
{
	struct ceph_cap *cap;
	int mds = -1;
	struct rb_node *p;

	/* prefer mds with WR|WRBUFFER|EXCL caps */
	for (p = rb_first(&ci->i_caps); p; p = rb_next(p)) {
		cap = rb_entry(p, struct ceph_cap, ci_node);
		mds = cap->mds;
		if (mseq)
			*mseq = cap->mseq;
		if (cap->issued & (CEPH_CAP_FILE_WR |
				   CEPH_CAP_FILE_WRBUFFER |
				   CEPH_CAP_FILE_EXCL))
			break;
	}
	return mds;
}

int ceph_get_cap_mds(struct inode *inode)
{
	int mds;
	spin_lock(&inode->i_lock);
	mds = __ceph_get_cap_mds(ceph_inode(inode), NULL);
	spin_unlock(&inode->i_lock);
	return mds;
}

/*
 * Called under i_lock.
 */
static void __insert_cap_node(struct ceph_inode_info *ci,
			      struct ceph_cap *new)
{
	struct rb_node **p = &ci->i_caps.rb_node;
	struct rb_node *parent = NULL;
	struct ceph_cap *cap = NULL;

	while (*p) {
		parent = *p;
		cap = rb_entry(parent, struct ceph_cap, ci_node);
		if (new->mds < cap->mds)
			p = &(*p)->rb_left;
		else if (new->mds > cap->mds)
			p = &(*p)->rb_right;
		else
			BUG();
	}

	rb_link_node(&new->ci_node, parent, p);
	rb_insert_color(&new->ci_node, &ci->i_caps);
}

/*
 * Add a capability under the given MDS session, after processing
 * the snapblob (to update the snap realm hierarchy).
 *
 * Bump i_count when adding it's first cap.
 *
 * Caller should hold session snap_rwsem (read), s_mutex.
 *
 * @fmode can be negative, in which case it is ignored.
 */
int ceph_add_cap(struct inode *inode,
		 struct ceph_mds_session *session, u64 cap_id,
		 int fmode, unsigned issued, unsigned wanted,
		 unsigned seq, unsigned mseq, u64 realmino,
		 unsigned ttl_ms, unsigned long ttl_from,
		 struct ceph_cap *new_cap)
{
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	struct ceph_inode_info *ci = ceph_inode(inode);
	struct ceph_cap *cap;
	int mds = session->s_mds;
	int is_first = 0;

	dout(10, "add_cap %p mds%d cap %llx %s seq %d\n", inode,
	     session->s_mds, cap_id, ceph_cap_string(issued), seq);
retry:
	spin_lock(&inode->i_lock);
	cap = __get_cap_for_mds(inode, mds);
	if (!cap) {
		if (new_cap) {
			cap = new_cap;
			new_cap = NULL;
		} else {
			spin_unlock(&inode->i_lock);
			new_cap = kmalloc(sizeof(*cap), GFP_NOFS);
			if (new_cap == NULL)
				return -ENOMEM;
			goto retry;
		}

		cap->issued = cap->implemented = 0;
		cap->flushing = 0;
		cap->mds_wanted = wanted;
		cap->mds = mds;

		is_first = RB_EMPTY_ROOT(&ci->i_caps);  /* grab inode later */
		cap->ci = ci;
		__insert_cap_node(ci, cap);

		/* clear out old exporting info?  (i.e. on cap import) */
		if (ci->i_cap_exporting_mds == mds) {
			ci->i_cap_exporting_issued = 0;
			ci->i_cap_exporting_mseq = 0;
			ci->i_cap_exporting_mds = -1;
		}

		/* add to session cap list */
		cap->session = session;
		list_add(&cap->session_caps, &session->s_caps);
		session->s_nr_caps++;
		INIT_LIST_HEAD(&cap->session_rdcaps);
	}
	if ((cap->issued & ~CEPH_CAP_EXPIREABLE) == 0) {
		/* move to tail of session rdcaps lru */
		if (!list_empty(&cap->session_rdcaps))
		    list_del(&cap->session_rdcaps);
		list_add_tail(&cap->session_rdcaps, &session->s_rdcaps);
	}
	if (!ci->i_snap_realm) {
		struct ceph_snap_realm *realm = ceph_get_snap_realm(mdsc,
								    realmino);
		if (realm) {
			ci->i_snap_realm = realm;
			list_add(&ci->i_snap_realm_item, &realm->inodes_with_caps);
		} else {
			derr(0, "couldn't find snap realm mdsc=%p realmino=%llu\n",
				mdsc, realmino);
		}
	}

	/*
	 * if we are newly issued FILE_RDCACHE, clear I_COMPLETE; we
	 * don't know what happened to this directory while we didn't
	 * have the cap.
	 */
	if (S_ISDIR(inode->i_mode) &&
	    (issued & CEPH_CAP_FILE_RDCACHE) &&
	    (cap->issued & CEPH_CAP_FILE_RDCACHE) == 0) {
		dout(10, " marking %p NOT complete\n", inode);
		ci->i_ceph_flags &= ~CEPH_I_COMPLETE;
	}

	dout(10, "add_cap inode %p (%llx.%llx) cap %s now %s seq %d mds%d\n",
	     inode, ceph_vinop(inode), ceph_cap_string(issued),
	     ceph_cap_string(issued|cap->issued), seq, mds);
	cap->cap_id = cap_id;
	cap->issued = issued;
	cap->implemented |= issued;
	cap->seq = seq;
	cap->mseq = mseq;
	cap->gen = session->s_cap_gen;
	cap->expires = ttl_from + (ttl_ms * HZ / 1000);
	if (fmode >= 0)
		__ceph_get_fmode(ci, fmode);
	spin_unlock(&inode->i_lock);
	if (is_first)
		igrab(inode);
	wake_up(&ci->i_cap_wq);
	kfree(new_cap);
	return 0;
}

/*
 * Return true if cap has not timed out and belongs to the current
 * generation of the MDS session.
 */
static int __cap_is_valid(struct ceph_cap *cap)
{
	unsigned long ttl;
	u32 gen;

	spin_lock(&cap->session->s_cap_lock);
	gen = cap->session->s_cap_gen;
	ttl = cap->session->s_cap_ttl;
	spin_unlock(&cap->session->s_cap_lock);

	if (cap->gen < gen || time_after_eq(jiffies, ttl)) {
		dout(30, "__cap_is_valid %p cap %p issued %s "
		     "but STALE (gen %u vs %u)\n", &cap->ci->vfs_inode,
		     cap, ceph_cap_string(cap->issued), cap->gen, gen);
		return 0;
	}

	if (time_after_eq(jiffies, cap->expires) &&
	    (cap->issued & ~CEPH_CAP_EXPIREABLE) == 0) {
		dout(30, "__cap_is_valid %p cap %p issued %s "
		     "but readonly and expired\n", &cap->ci->vfs_inode,
		     cap, ceph_cap_string(cap->issued));
		return 0;
	}

	return 1;
}

/*
 * Return set of valid cap bits issued to us.  Note that caps time
 * out, and may be invalidated in bulk if the client session times out
 * and session->s_cap_gen is bumped.
 */
int __ceph_caps_issued(struct ceph_inode_info *ci, int *implemented)
{
	int have = ci->i_snap_caps;
	struct ceph_cap *cap;
	struct rb_node *p;

	if (implemented)
		*implemented = 0;
	for (p = rb_first(&ci->i_caps); p; p = rb_next(p)) {
		cap = rb_entry(p, struct ceph_cap, ci_node);
		if (!__cap_is_valid(cap))
			continue;
		dout(30, "__ceph_caps_issued %p cap %p issued %s\n",
		     &ci->vfs_inode, cap, ceph_cap_string(cap->issued));
		have |= cap->issued;
		if (implemented)
			*implemented |= cap->implemented;
	}
	return have;
}

/*
 * Return mask of fields dirtied while holding *_EXCL or FILE_WR caps.
 * Include both per-inode 'dirty' fields and 'flushing' caps.
 */
int __ceph_caps_dirty(struct ceph_inode_info *ci)
{
	struct inode *inode = &ci->vfs_inode;
	int dirty = ci->i_dirty_caps;
	struct ceph_cap *cap;
	struct rb_node *p;

	for (p = rb_first(&ci->i_caps); p; p = rb_next(p)) {
		cap = rb_entry(p, struct ceph_cap, ci_node);
		if (!__cap_is_valid(cap))
			continue;
		dout(30, "__ceph_caps_dirty %p cap %p flushing %s\n",
		     inode, cap, ceph_cap_string(cap->flushing));
		dirty |= cap->flushing;
	}
	dout(30, "__ceph_caps_dirty %p dirty %s\n", inode,
	     ceph_cap_string(dirty));
	return dirty;
}

/*
 * Return mask of caps currently being revoked by an MDS.
 */
int ceph_caps_revoking(struct ceph_inode_info *ci)
{
	struct inode *inode = &ci->vfs_inode;
	struct ceph_cap *cap;
	struct rb_node *p;
	int revoking = 0;

	spin_lock(&inode->i_lock);
	for (p = rb_first(&ci->i_caps); p; p = rb_next(p)) {
		cap = rb_entry(p, struct ceph_cap, ci_node);
		if (__cap_is_valid(cap))
			revoking |= cap->implemented & ~cap->issued;
	}
	spin_unlock(&inode->i_lock);
	dout(30, "ceph_caps_revoking %p revoking %s\n", inode,
	     ceph_cap_string(revoking));
	return revoking;
}

/*
 * called under i_lock
 */
static int __ceph_is_any_caps(struct ceph_inode_info *ci)
{
	return !RB_EMPTY_ROOT(&ci->i_caps) || ci->i_cap_exporting_mds >= 0;
}

/*
 * caller should hold i_lock, and session s_mutex.
 * returns true if this is the last cap.  if so, caller should iput.
 */
static int __ceph_remove_cap(struct ceph_cap *cap)
{
	struct ceph_mds_session *session = cap->session;
	struct ceph_inode_info *ci = cap->ci;
	struct ceph_mds_client *mdsc = &ceph_client(ci->vfs_inode.i_sb)->mdsc;

	dout(20, "__ceph_remove_cap %p from %p\n", cap, &ci->vfs_inode);

	/* remove from session list */
	list_del_init(&cap->session_caps);
	if (!list_empty(&cap->session_rdcaps))
		list_del_init(&cap->session_rdcaps);
	session->s_nr_caps--;

	/* remove from inode list */
	rb_erase(&cap->ci_node, &ci->i_caps);
	cap->session = NULL;

	kfree(cap);

	if (!__ceph_is_any_caps(ci)) {
		list_del_init(&ci->i_snap_realm_item);
		ceph_put_snap_realm(mdsc, ci->i_snap_realm);
		ci->i_snap_realm = NULL;
		return 1;
	}
	return 0;
}

/*
 * caller should hold session s_mutex.
 */
void ceph_remove_cap(struct ceph_cap *cap)
{
	struct inode *inode = &cap->ci->vfs_inode;
	int was_last;

	spin_lock(&inode->i_lock);
	was_last = __ceph_remove_cap(cap);
	spin_unlock(&inode->i_lock);
	if (was_last)
		iput(inode);
}

/*
 *
 * (Re)queue cap at the end of the delayed cap release list.
 *
 * Caller holds i_lock
 *    -> we take mdsc->cap_delay_lock
 */
static void __cap_delay_requeue(struct ceph_mds_client *mdsc,
				struct ceph_inode_info *ci)
{
	ci->i_hold_caps_until = round_jiffies(jiffies + CEPH_CAP_DELAY);
	dout(10, "__cap_delay_requeue %p at %lu\n", &ci->vfs_inode,
	     ci->i_hold_caps_until);
	spin_lock(&mdsc->cap_delay_lock);
	if (!mdsc->stopping) {
		if (list_empty(&ci->i_cap_delay_list))
			igrab(&ci->vfs_inode);
		else
			list_del_init(&ci->i_cap_delay_list);
		list_add_tail(&ci->i_cap_delay_list, &mdsc->cap_delay_list);
	}
	spin_unlock(&mdsc->cap_delay_lock);
}

/*
 * Cancel delayed work on cap.
 * caller hold s_mutex
 */
static void __cap_delay_cancel(struct ceph_mds_client *mdsc,
			       struct ceph_inode_info *ci)
{
	dout(10, "__cap_delay_cancel %p\n", &ci->vfs_inode);
	if (list_empty(&ci->i_cap_delay_list))
		return;
	spin_lock(&mdsc->cap_delay_lock);
	list_del_init(&ci->i_cap_delay_list);
	spin_unlock(&mdsc->cap_delay_lock);
	iput(&ci->vfs_inode);
}

/*
 * Build and send a cap message to the given MDS.
 *
 * Caller should be holding s_mutex.
 */
static void send_cap_msg(struct ceph_mds_client *mdsc, u64 ino, u64 cid, int op,
			 int caps, int wanted, int dirty, u64 seq, u64 mseq,
			 u64 size, u64 max_size,
			 struct timespec *mtime, struct timespec *atime,
			 u64 time_warp_seq,
			 uid_t uid, gid_t gid, mode_t mode,
			 u64 follows, int mds)
{
	struct ceph_mds_caps *fc;
	struct ceph_msg *msg;

	dout(10, "send_cap_msg %s %llx %llx caps %s wanted %s dirty %s"
	     " seq %llu/%llu follows %lld size %llu\n", ceph_cap_op_name(op),
	     cid, ino, ceph_cap_string(caps), ceph_cap_string(wanted),
	     ceph_cap_string(dirty),
	     seq, mseq, follows, size);

	msg = ceph_msg_new(CEPH_MSG_CLIENT_CAPS, sizeof(*fc), 0, 0, NULL);
	if (IS_ERR(msg))
		return;

	fc = msg->front.iov_base;

	memset(fc, 0, sizeof(*fc));

	fc->cap_id = cpu_to_le64(cid);
	fc->op = cpu_to_le32(op);
	fc->seq = cpu_to_le32(seq);
	fc->migrate_seq = cpu_to_le32(mseq);
	fc->caps = cpu_to_le32(caps);
	fc->wanted = cpu_to_le32(wanted);
	fc->dirty = cpu_to_le32(dirty);
	fc->ino = cpu_to_le64(ino);
	fc->snap_follows = cpu_to_le64(follows);

	fc->size = cpu_to_le64(size);
	fc->max_size = cpu_to_le64(max_size);
	if (mtime)
		ceph_encode_timespec(&fc->mtime, mtime);
	if (atime)
		ceph_encode_timespec(&fc->atime, atime);
	fc->time_warp_seq = cpu_to_le32(time_warp_seq);

	fc->uid = cpu_to_le32(uid);
	fc->gid = cpu_to_le32(gid);
	fc->mode = cpu_to_le32(mode);

	ceph_send_msg_mds(mdsc, msg, mds);
}

/*
 *
 * Send a cap msg on the given inode.  Make note of max_size
 * reported/requested from mds, revoked caps that have now been
 * implemented.
 *
 * Also, try to invalidate page cache if we are dropping RDCACHE.
 * Note that this will leave behind any locked pages... FIXME!
 *
 * called with i_lock, then drops it.
 * caller should hold snap_rwsem (read), s_mutex.
 */
static void __send_cap(struct ceph_mds_client *mdsc,
		       struct ceph_mds_session *session,
		       struct ceph_cap *cap,
		       int used, int want, int retain)
	__releases(cap->ci->vfs_inode->i_lock)
{
	struct ceph_inode_info *ci = cap->ci;
	struct inode *inode = &ci->vfs_inode;
	u64 cap_id = cap->cap_id;
	int held = cap->issued | cap->implemented;
	int revoking = cap->implemented & ~cap->issued;
	int dropping = cap->issued & ~retain;
	int op = (retain == 0) ? CEPH_CAP_OP_RELEASE : CEPH_CAP_OP_UPDATE;
	int keep;
	u64 seq, mseq, time_warp_seq, follows;
	u64 size, max_size;
	struct timespec mtime, atime;
	int wake = 0;
	mode_t mode;
	uid_t uid;
	gid_t gid;
	int dirty;
	int flushing;
	int last_cap = 0;

	dout(10, "__send_cap cap %p session %p %s -> %s (revoking %s)\n",
	     cap, cap->session,
	     ceph_cap_string(held), ceph_cap_string(held & retain),
	     ceph_cap_string(revoking));

	dirty = __ceph_caps_dirty(ci);
	cap->flushing |= dirty & held;
	if (cap->flushing) {
		ci->i_dirty_caps &= ~cap->flushing;
		dout(10, "__send_cap flushing %s, dirty_caps now %s\n",
		     ceph_cap_string(cap->flushing),
		     ceph_cap_string(ci->i_dirty_caps));
	}

	cap->issued &= retain;  /* drop bits we don't want */

	if (revoking && (revoking & used) == 0) {
		cap->implemented = cap->issued;
		/*
		 * Wake up any waiters on wanted -> needed transition.
		 * This is due to the weird transition from buffered
		 * to sync IO... we need to flush dirty pages _before_
		 * allowing sync writes to avoid reordering.
		 */
		wake = 1;
	}

	cap->mds_wanted = want;

	keep = cap->issued;
	flushing = cap->flushing;
	seq = cap->seq;
	mseq = cap->mseq;
	size = inode->i_size;
	ci->i_reported_size = size;
	max_size = ci->i_wanted_max_size;
	ci->i_requested_max_size = max_size;
	mtime = inode->i_mtime;
	atime = inode->i_atime;
	time_warp_seq = ci->i_time_warp_seq;
	follows = ci->i_snap_realm->cached_context->seq;
	uid = inode->i_uid;
	gid = inode->i_gid;
	mode = inode->i_mode;

	/* close out cap? */
	if (flushing == 0 && keep == 0)
		last_cap = __ceph_remove_cap(cap);
	
	spin_unlock(&inode->i_lock);

	if (dropping & CEPH_CAP_FILE_RDCACHE) {
		/* invalidate what we can */
		dout(20, "invalidating pages on %p\n", inode);
		invalidate_mapping_pages(&inode->i_data, 0, -1);
	}

	send_cap_msg(mdsc, ceph_vino(inode).ino, cap_id,
		     op, keep, want, flushing, seq, mseq,
		     size, max_size, &mtime, &atime, time_warp_seq,
		     uid, gid, mode,
		     follows, session->s_mds);

	if (wake)
		wake_up(&ci->i_cap_wq);
	if (last_cap)
		iput(inode);
}


/*
 * When a snapshot is taken, clients accumulate "dirty" data on inodes
 * with capabilities in ceph_cap_snaps to describe the file state at
 * the time the snapshot was taken.  This must be flushed
 * asynchronously back to the MDS once sync writes complete and dirty
 * data is written out.
 *
 * Called under i_lock.  Takes s_mutex as needed.
 */
void __ceph_flush_snaps(struct ceph_inode_info *ci,
			struct ceph_mds_session **psession)
{
	struct inode *inode = &ci->vfs_inode;
	int mds;
	struct list_head *p;
	struct ceph_cap_snap *capsnap;
	u32 mseq;
	struct ceph_mds_client *mdsc = &ceph_inode_to_client(inode)->mdsc;
	struct ceph_mds_session *session = NULL; /* if session != NULL, we hold
						    session->s_mutex */
	u64 next_follows = 0;  /* keep track of how far we've gotten through the
			     i_cap_snaps list, and skip these entries next time
			     around to avoid an infinite loop */

	if (psession)
		session = *psession;

	dout(10, "__flush_snaps %p\n", inode);
retry:
	list_for_each(p, &ci->i_cap_snaps) {
		capsnap = list_entry(p, struct ceph_cap_snap, ci_item);

		/* avoid an infiniute loop after retry */
		if (capsnap->follows < next_follows)
			continue;
		/*
		 * we need to wait for sync writes to complete and for dirty
		 * pages to be written out.
		 */
		if (capsnap->dirty_pages || capsnap->writing)
			continue;

		/* pick mds, take s_mutex */
		mds = __ceph_get_cap_mds(ci, &mseq);
		if (session && session->s_mds != mds) {
			dout(30, "oops, wrong session %p mutex\n", session);
			mutex_unlock(&session->s_mutex);
			ceph_put_mds_session(session);
			session = NULL;
		}
		if (!session) {
			spin_unlock(&inode->i_lock);
			mutex_lock(&mdsc->mutex);
			session = __ceph_lookup_mds_session(mdsc, mds);
			mutex_unlock(&mdsc->mutex);
			if (session) {
				dout(10, "inverting session/ino locks on %p\n",
				     session);
				mutex_lock(&session->s_mutex);
			}
			/*
			 * if session == NULL, we raced against a cap
			 * deletion.  retry, and we'll get a better
			 * @mds value next time.
			 */
			spin_lock(&inode->i_lock);
			goto retry;
		}

		atomic_inc(&capsnap->nref);
		spin_unlock(&inode->i_lock);

		dout(10, "flush_snaps %p cap_snap %p follows %lld size %llu\n",
		     inode, capsnap, next_follows, capsnap->size);
		send_cap_msg(mdsc, ceph_vino(inode).ino, 0,
			     CEPH_CAP_OP_FLUSHSNAP, capsnap->issued, 0,
			     capsnap->dirty, 0, mseq,
			     capsnap->size, 0,
			     &capsnap->mtime, &capsnap->atime,
			     capsnap->time_warp_seq,
			     capsnap->uid, capsnap->gid, capsnap->mode,
			     capsnap->follows, mds);

		next_follows = capsnap->follows + 1;
		ceph_put_cap_snap(capsnap);

		spin_lock(&inode->i_lock);
		goto retry;
	}

	/* we flushed them all; remove this inode from the queue */
	spin_lock(&mdsc->snap_flush_lock);
	list_del_init(&ci->i_snap_flush_item);
	spin_unlock(&mdsc->snap_flush_lock);

	if (psession)
		*psession = session;
	else if (session) {
		mutex_unlock(&session->s_mutex);
		ceph_put_mds_session(session);
	}
}

static void ceph_flush_snaps(struct ceph_inode_info *ci)
{
	struct inode *inode = &ci->vfs_inode;

	spin_lock(&inode->i_lock);
	__ceph_flush_snaps(ci, NULL);
	spin_unlock(&inode->i_lock);
}


/*
 * Swiss army knife function to examine currently used, wanted versus
 * held caps.  Release, flush, ack revoked caps to mds as appropriate.
 *
 * @is_delayed indicates caller is delayed work and we should not
 * delay further.
 */
void ceph_check_caps(struct ceph_inode_info *ci, int is_delayed, int drop,
		     struct ceph_mds_session *session)
{
	struct ceph_client *client = ceph_inode_to_client(&ci->vfs_inode);
	struct ceph_mds_client *mdsc = &client->mdsc;
	struct inode *inode = &ci->vfs_inode;
	struct ceph_cap *cap;
	int file_wanted, used;
	int took_snap_rwsem = 0;             /* true if mdsc->snap_rwsem held */
	int want, retain, revoking;
	int mds = -1;   /* keep track of how far we've gone through i_caps list
			   to avoid an infinite loop on retry */
	struct rb_node *p;
	int tried_invalidate = 0;

	/* if we are unmounting, flush any unused caps immediately. */
	if (mdsc->stopping)
		is_delayed = 1;

	spin_lock(&inode->i_lock);

	/* flush snaps first time around only */
	if (!list_empty(&ci->i_cap_snaps))
		__ceph_flush_snaps(ci, &session);
	goto retry_locked;
retry:
	spin_lock(&inode->i_lock);
retry_locked:
	file_wanted = __ceph_caps_file_wanted(ci);
	used = __ceph_caps_used(ci);

	want = file_wanted | used;
	
	retain = want;
	if (!mdsc->stopping && inode->i_nlink > 0) {
		/*
		 * we cannot retain anything outside of
		 * (wanted|EXPIREABLE), or else we run the risk of the
		 * MDS' wanted value getting out of sync, and we have
		 * no mechanism to adjust that (besides open()).
		 */
		retain |= CEPH_CAP_EXPIREABLE;

		/* keep any EXCL bits too, while we are holding caps anyway */
		if (want || S_ISDIR(inode->i_mode))
			retain |= CEPH_CAP_ANY_EXCL;
	}
	retain &= ~drop;

	dout(10, "check_caps %p file_wanted %s used %s retain %s issued %s\n",
	     inode, ceph_cap_string(file_wanted), ceph_cap_string(used),
	     ceph_cap_string(retain),
	     ceph_cap_string(__ceph_caps_issued(ci, NULL)));

	/*
	 * Reschedule delayed caps release, unless we are called from
	 * the delayed work handler (i.e. this _is_ the delayed release)
	 */
	if (!is_delayed)
		__cap_delay_requeue(mdsc, ci);

	/*
	 * If we no longer need to hold onto old our caps, and we may
	 * have cached pages, but don't want them, then try to invalidate.
	 * If we fail, it's because pages are locked.... try again later.
	 */
	if ((!time_before(jiffies, ci->i_hold_caps_until) || mdsc->stopping) &&
	    ci->i_wrbuffer_ref == 0 &&               /* no dirty pages... */
	    ci->i_rdcache_gen &&                     /* may have cached pages */
	    file_wanted == 0 &&                      /* no open files */
	    !tried_invalidate) {
		u32 invalidating_gen = ci->i_rdcache_gen;
		int ret;

		dout(10, "check_caps trying to invalidate on %p\n", inode);
		spin_unlock(&inode->i_lock);
		ret = invalidate_inode_pages2(&inode->i_data);
		spin_lock(&inode->i_lock);
		if (ret == 0 && invalidating_gen == ci->i_rdcache_gen) {
			/* success. */
			ci->i_rdcache_gen = 0;
			ci->i_rdcache_revoking = 0;
		} else {
			dout(10, "check_caps failed to invalidate pages\n");
			/* we failed to invalidate pages.  check these
			   caps again later. */
			if (is_delayed)
				__cap_delay_requeue(mdsc, ci);
		}
		tried_invalidate = 1;
		goto retry_locked;
	}

	for (p = rb_first(&ci->i_caps); p; p = rb_next(p)) {
		cap = rb_entry(p, struct ceph_cap, ci_node);

		/* avoid looping forever */
		if (mds >= cap->mds)
			continue;

		/* NOTE: no side-effects allowed, until we take s_mutex */

		revoking = cap->implemented & ~cap->issued;
		if (revoking)
			dout(10, "mds%d revoking %s\n", cap->mds,
			     ceph_cap_string(revoking));

		/* request larger max_size from MDS& ~drop;? */
		if (ci->i_wanted_max_size > ci->i_max_size &&
		    ci->i_wanted_max_size > ci->i_requested_max_size)
			goto ack;

		/* approaching file_max? */
		if ((cap->issued & CEPH_CAP_FILE_WR) &&
		    (inode->i_size << 1) >= ci->i_max_size &&
		    (ci->i_reported_size << 1) < ci->i_max_size) {
			dout(10, "i_size approaching max_size\n");
			goto ack;
		}

		/* completed revocation? going down and there are no caps? */
		if (revoking && (revoking & used) == 0) {
			dout(10, "completed revocation of %s\n",
			     ceph_cap_string(cap->implemented & ~cap->issued));
			goto ack;
		}

		/* shutting down? */
		if (!revoking && mdsc->stopping && (used == 0))
			goto ack;

		if ((cap->issued & ~retain) == 0 &&
		    cap->mds_wanted == want)
			continue;     /* nothing extra, wanted is correct */

		/* delay cap release for a bit? */
		if (!is_delayed &&
		    time_before(jiffies, ci->i_hold_caps_until)) {
			dout(30, "delaying cap release\n");
			continue;
		}

ack:
		if (session && session != cap->session) {
			dout(30, "oops, wrong session %p mutex\n", session);
			mutex_unlock(&session->s_mutex);
			session = NULL;
		}
		if (!session) {
			session = cap->session;
			if (mutex_trylock(&session->s_mutex) == 0) {
				dout(10, "inverting session/ino locks on %p\n",
				     session);
				spin_unlock(&inode->i_lock);
				if (took_snap_rwsem) {
					up_read(&mdsc->snap_rwsem);
					took_snap_rwsem = 0;
				}
				mutex_lock(&session->s_mutex);
				goto retry;
			}
		}
		/* take snap_rwsem after session mutex */
		if (!took_snap_rwsem) {
			if (down_read_trylock(&mdsc->snap_rwsem) == 0) {
				dout(10, "inverting snap/in locks on %p\n",
				     inode);
				spin_unlock(&inode->i_lock);
				down_read(&mdsc->snap_rwsem);
				took_snap_rwsem = 1;
				goto retry;
			}
			took_snap_rwsem = 1;
		}

		mds = cap->mds;  /* remember mds, so we don't repeat */

		/* __send_cap drops i_lock */
		__send_cap(mdsc, session, cap, used, want, retain);

		goto retry; /* retake i_lock and restart our cap scan. */
	}

	/* okay */
	spin_unlock(&inode->i_lock);

	if (session)
		mutex_unlock(&session->s_mutex);
	if (took_snap_rwsem)
		up_read(&mdsc->snap_rwsem);
}

/*
 * Track references to capabilities we hold, so that we don't release
 * them to the MDS prematurely.
 *
 * Protected by i_lock.
 */
static void __take_cap_refs(struct ceph_inode_info *ci, int got)
{
	if (got & CEPH_CAP_FILE_RD)
		ci->i_rd_ref++;
	if (got & CEPH_CAP_FILE_RDCACHE)
		ci->i_rdcache_ref++;
	if (got & CEPH_CAP_FILE_WR)
		ci->i_wr_ref++;
	if (got & CEPH_CAP_FILE_WRBUFFER) {
		ci->i_wrbuffer_ref++;
		dout(30, "__take_cap_refs %p wrbuffer %d -> %d (?)\n",
		     &ci->vfs_inode, ci->i_wrbuffer_ref-1, ci->i_wrbuffer_ref);
	}
}

/*
 * Try to grab cap references.  Specify those refs we @want, and the
 * minimal set we @need.  Also include the larger offset we are writing
 * to (when applicable), and check against max_size here as well.
 * Note that caller is responsible for ensuring max_size increases are
 * requested from the MDS.
 */
int ceph_get_cap_refs(struct ceph_inode_info *ci, int need, int want, int *got,
		      loff_t endoff)
{
	struct inode *inode = &ci->vfs_inode;
	int ret = 0;
	int have, implemented;

	dout(30, "get_cap_refs %p need %s want %s\n", inode,
	     ceph_cap_string(need), ceph_cap_string(want));
	spin_lock(&inode->i_lock);
	if (need & CEPH_CAP_FILE_WR) {
		if (endoff >= 0 && endoff > (loff_t)ci->i_max_size) {
			dout(20, "get_cap_refs %p endoff %llu > maxsize %llu\n",
			     inode, endoff, ci->i_max_size);
			goto sorry;
		}
		/*
		 * If a sync write is in progress, we must wait, so that we
		 * can get a final snapshot value for size+mtime.
		 */
		if (__ceph_have_pending_cap_snap(ci)) {
			dout(20, "get_cap_refs %p cap_snap_pending\n", inode);
			goto sorry;
		}
	}
	have = __ceph_caps_issued(ci, &implemented);

	/*
	 * disallow writes while a truncate is pending
	 */
	if (ci->i_truncate_pending)
		have &= ~CEPH_CAP_FILE_WR;

	if ((have & need) == need) {
		/*
		 * Look at (implemented & ~have & not) so that we keep waiting
		 * on transition from wanted -> needed caps.  This is needed
		 * for WRBUFFER|WR -> WR to avoid a new WR sync write from
		 * going before a prior buffered writeback happens.
		 */
		int not = want & ~(have & need);
		int revoking = implemented & ~have;
		dout(30, "get_cap_refs %p have %s but not %s (revoking %s)\n",
		     inode, ceph_cap_string(have), ceph_cap_string(not),
		     ceph_cap_string(revoking));
		if ((revoking & not) == 0) {
			*got = need | (have & want);
			__take_cap_refs(ci, *got);
			ret = 1;
		}
	} else {
		dout(30, "get_cap_refs %p have %s needed %s\n", inode,
		     ceph_cap_string(have), ceph_cap_string(need));
	}
sorry:
	spin_unlock(&inode->i_lock);
	dout(30, "get_cap_refs %p ret %d got %s\n", inode,
	     ret, ceph_cap_string(*got));
	return ret;
}

/*
 * Release cap refs.
 *
 * If we released the last ref on any given cap, call ceph_check_caps
 * to release (or schedule a release).
 *
 * If we are releasing a WR cap (from a sync write), finalize any affected
 * cap_snap, and wake up any waiters.
 */
void ceph_put_cap_refs(struct ceph_inode_info *ci, int had)
{
	struct inode *inode = &ci->vfs_inode;
	int last = 0, flushsnaps = 0, wake = 0;
	struct ceph_cap_snap *capsnap;

	spin_lock(&inode->i_lock);
	if (had & CEPH_CAP_FILE_RD)
		if (--ci->i_rd_ref == 0)
			last++;
	if (had & CEPH_CAP_FILE_RDCACHE)
		if (--ci->i_rdcache_ref == 0)
			last++;
	if (had & CEPH_CAP_FILE_WRBUFFER) {
		if (--ci->i_wrbuffer_ref == 0)
			last++;
		dout(30, "put_cap_refs %p wrbuffer %d -> %d (?)\n",
		     inode, ci->i_wrbuffer_ref+1, ci->i_wrbuffer_ref);
	}
	if (had & CEPH_CAP_FILE_WR)
		if (--ci->i_wr_ref == 0) {
			last++;
			if (!list_empty(&ci->i_cap_snaps)) {
				capsnap = list_entry(ci->i_cap_snaps.next,
						     struct ceph_cap_snap,
						     ci_item);
				if (capsnap->writing) {
					capsnap->writing = 0;
					flushsnaps =
						__ceph_finish_cap_snap(ci,
								       capsnap);
					wake = 1;
				}
			}
		}
	spin_unlock(&inode->i_lock);

	dout(30, "put_cap_refs %p had %s %s\n", inode, ceph_cap_string(had),
	     last ? "last" : "");

	if (last && !flushsnaps)
		ceph_check_caps(ci, 0, 0, NULL);
	else if (flushsnaps)
		ceph_flush_snaps(ci);
	if (wake)
		wake_up(&ci->i_cap_wq);
}

/*
 * Release @nr WRBUFFER refs on dirty pages for the given @snapc snap
 * context.  Adjust per-snap dirty page accounting as appropriate.
 * Once all dirty data for a cap_snap is flushed, flush snapped file
 * metadata back to the MDS.  If we dropped the last ref, call
 * ceph_check_caps.
 */
void ceph_put_wrbuffer_cap_refs(struct ceph_inode_info *ci, int nr,
				struct ceph_snap_context *snapc)
{
	struct inode *inode = &ci->vfs_inode;
	int last = 0;
	int last_snap = 0;
	int found = 0;
	struct list_head *p;
	struct ceph_cap_snap *capsnap = NULL;

	spin_lock(&inode->i_lock);
	ci->i_wrbuffer_ref -= nr;
	last = !ci->i_wrbuffer_ref;

	if (ci->i_head_snapc == snapc) {
		ci->i_wrbuffer_ref_head -= nr;
		if (!ci->i_wrbuffer_ref_head) {
			ceph_put_snap_context(ci->i_head_snapc);
			ci->i_head_snapc = NULL;
		}
		dout(30, "put_wrbuffer_cap_refs on %p head %d/%d -> %d/%d %s\n",
		     inode,
		     ci->i_wrbuffer_ref+nr, ci->i_wrbuffer_ref_head+nr,
		     ci->i_wrbuffer_ref, ci->i_wrbuffer_ref_head,
		     last ? " LAST" : "");
	} else {
		list_for_each(p, &ci->i_cap_snaps) {
			capsnap = list_entry(p, struct ceph_cap_snap, ci_item);
			if (capsnap->context == snapc) {
				found = 1;
				capsnap->dirty_pages -= nr;
				last_snap = !capsnap->dirty_pages;
				break;
			}
		}
		BUG_ON(!found);
		dout(30, "put_wrbuffer_cap_refs on %p cap_snap %p "
		     " snap %lld %d/%d -> %d/%d %s%s\n",
		     inode, capsnap, capsnap->context->seq,
		     ci->i_wrbuffer_ref+nr, capsnap->dirty_pages + nr,
		     ci->i_wrbuffer_ref, capsnap->dirty_pages,
		     last ? " (wrbuffer last)" : "",
		     last_snap ? " (capsnap last)" : "");
	}

	spin_unlock(&inode->i_lock);

	if (last) {
		ceph_check_caps(ci, 0, 0, NULL);
	} else if (last_snap) {
		ceph_flush_snaps(ci);
		wake_up(&ci->i_cap_wq);
	}
}



/*
 * Handle a cap GRANT message from the MDS.  (Note that a GRANT may
 * actually be a revocation if it specifies a smaller cap set.)
 *
 * caller holds s_mutex.
 * return value:
 *  0 - ok
 *  1 - send the msg back to mds
 *  2 - check_caps
 */
static int handle_cap_grant(struct inode *inode, struct ceph_mds_caps *grant,
			    struct ceph_mds_session *session,
			    struct ceph_cap *cap,
			    void **xattr_data)
	__releases(inode->i_lock)

{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	int seq = le32_to_cpu(grant->seq);
	int newcaps = le32_to_cpu(grant->caps);
	int issued, implemented, used, wanted, dirty;
	u64 size = le64_to_cpu(grant->size);
	u64 max_size = le64_to_cpu(grant->max_size);
	struct timespec mtime, atime, ctime;
	int reply = 0;
	int wake = 0;
	int writeback = 0;
	int revoked_rdcache = 0;
	int invalidate_async = 0;
	int tried_invalidate = 0;
	int ret;

	dout(10, "handle_cap_grant inode %p cap %p mds%d seq %d %s\n",
	     inode, cap, mds, seq, ceph_cap_string(newcaps));
	dout(10, " size %llu max_size %llu, i_size %llu\n", size, max_size,
		inode->i_size);
start:

	cap->gen = session->s_cap_gen;

	/*
	 * Each time we receive RDCACHE anew, we increment i_rdcache_gen.
	 * Also clear I_COMPLETE: we don't know what happened to this directory
	 */
	if ((newcaps & CEPH_CAP_FILE_RDCACHE) &&          /* got RDCACHE */
	    (cap->issued & CEPH_CAP_FILE_RDCACHE) == 0 && /* but not before */
	    (__ceph_caps_issued(ci, NULL) & CEPH_CAP_FILE_RDCACHE) == 0) {
		ci->i_rdcache_gen++;

		if (S_ISDIR(inode->i_mode)) {
			dout(10, " marking %p NOT complete\n", inode);
			ci->i_ceph_flags &= ~CEPH_I_COMPLETE;
		}
	}

	/*
	 * If RDCACHE is being revoked, and we have no dirty buffers,
	 * try to invalidate (once).  (If there are dirty buffers, we
	 * will invalidate _after_ writeback.)
	 */
	if (((cap->issued & ~newcaps) & CEPH_CAP_FILE_RDCACHE) &&
	    !ci->i_wrbuffer_ref && !tried_invalidate) {
		dout(10, "RDCACHE invalidation\n");
		spin_unlock(&inode->i_lock);
		tried_invalidate = 1;

		ret = invalidate_inode_pages2(&inode->i_data);
		spin_lock(&inode->i_lock);
		if (ret < 0) {
			/* there were locked pages.. invalidate later
			   in a separate thread. */
			if (ci->i_rdcache_revoking != ci->i_rdcache_gen) {
				invalidate_async = 1;
				ci->i_rdcache_revoking = ci->i_rdcache_gen;
			}
		} else {
			/* we successfully invalidated those pages */
			revoked_rdcache = 1;
			ci->i_rdcache_gen = 0;
			ci->i_rdcache_revoking = 0;
		}
		goto start;
	}

	issued = __ceph_caps_issued(ci, &implemented);
	issued |= implemented | __ceph_caps_dirty(ci);

	if ((issued & CEPH_CAP_AUTH_EXCL) == 0) {
		inode->i_mode = le32_to_cpu(grant->mode);
		inode->i_uid = le32_to_cpu(grant->uid);
		inode->i_gid = le32_to_cpu(grant->gid);
		dout(20, "%p mode 0%o uid.gid %d.%d\n", inode, inode->i_mode, 
		     inode->i_uid, inode->i_gid);
	}
	
	if ((issued & CEPH_CAP_LINK_EXCL) == 0) {
		inode->i_nlink = le32_to_cpu(grant->nlink);
	}

	if ((issued & CEPH_CAP_XATTR_EXCL) == 0 && grant->xattr_len) {
		int len = le32_to_cpu(grant->xattr_len);
		u64 version = le64_to_cpu(grant->xattr_version);

		if (!(len > 4 && *xattr_data == NULL) &&  /* ENOMEM in caller */
		    version > ci->i_xattr_version) {
			dout(20, " got new xattrs v%llu on %p len %d\n",
			     version, inode, len);
			kfree(ci->i_xattr_data);
			ci->i_xattr_len = len;
			ci->i_xattr_version = version;
			ci->i_xattr_data = *xattr_data;
			*xattr_data = NULL;
		}
	}

	/* size/ctime/mtime/atime? */
	ceph_fill_file_size(inode, issued,
			    le32_to_cpu(grant->truncate_seq),
			    le64_to_cpu(grant->truncate_size), size);
	ceph_decode_timespec(&mtime, &grant->mtime);
	ceph_decode_timespec(&atime, &grant->atime);
	ceph_decode_timespec(&ctime, &grant->ctime);
	ceph_fill_file_time(inode, issued,
			    le32_to_cpu(grant->time_warp_seq), &ctime, &mtime,
			    &atime);

	/* max size increase? */
	if (max_size != ci->i_max_size) {
		dout(10, "max_size %lld -> %llu\n", ci->i_max_size, max_size);
		ci->i_max_size = max_size;
		if (max_size >= ci->i_wanted_max_size) {
			ci->i_wanted_max_size = 0;  /* reset */
			ci->i_requested_max_size = 0;
		}
		wake = 1;
	}

	/* check cap bits */
	wanted = __ceph_caps_wanted(ci);
	used = __ceph_caps_used(ci);
	dirty = __ceph_caps_dirty(ci);
	dout(10, " my wanted = %s, used = %s, dirty %s\n",
	     ceph_cap_string(wanted),
	     ceph_cap_string(used),
	     ceph_cap_string(dirty));
	if (wanted != le32_to_cpu(grant->wanted)) {
		dout(10, "mds wanted %s -> %s\n",
		     ceph_cap_string(le32_to_cpu(grant->wanted)),
		     ceph_cap_string(wanted));
		grant->wanted = cpu_to_le32(wanted);
	}

	cap->seq = seq;

	/* file layout may have changed */
	ci->i_layout = grant->layout;

	/* revocation? */
	if (cap->issued & ~newcaps) {
		dout(10, "revocation: %s -> %s\n", ceph_cap_string(cap->issued),
		     ceph_cap_string(newcaps));
		if ((used & ~newcaps) & CEPH_CAP_FILE_WRBUFFER) {
			writeback = 1; /* will delay ack */
		} else if (dirty & ~newcaps)
			reply = 2;     /* initiate writeback in check_caps */
		else if (((used & ~newcaps) & CEPH_CAP_FILE_RDCACHE) == 0 ||
			   revoked_rdcache) {
			/*
			 * we're not using revoked caps.. ack now.
			 * re-use incoming message.
			 */
			cap->implemented = newcaps;

			grant->size = cpu_to_le64(inode->i_size);
			grant->max_size = 0;  /* don't re-request */
			ceph_encode_timespec(&grant->mtime, &inode->i_mtime);
			ceph_encode_timespec(&grant->atime, &inode->i_atime);
			grant->time_warp_seq = cpu_to_le32(ci->i_time_warp_seq);
			grant->snap_follows =
			     cpu_to_le64(ci->i_snap_realm->cached_context->seq);
			reply = 1;
			wake = 1;
		}
		cap->issued = newcaps;
		goto out;
	}

	/* grant or no-op */
	if (cap->issued == newcaps) {
		dout(10, "caps unchanged: %s -> %s\n",
		     ceph_cap_string(cap->issued), ceph_cap_string(newcaps));
	} else {
		dout(10, "grant: %s -> %s\n", ceph_cap_string(cap->issued),
		     ceph_cap_string(newcaps));
		cap->issued = newcaps;
		cap->implemented |= newcaps;    /* add bits only, to
						 * avoid stepping on a
						 * pending revocation */
		wake = 1;
	}

out:
	spin_unlock(&inode->i_lock);
	if (writeback) {
		/*
		 * queue inode for writeback: we can't actually call
		 * filemap_write_and_wait, etc. from message handler
		 * context.
		 */
		dout(10, "queueing %p for writeback\n", inode);
		if (ceph_queue_writeback(inode))
			igrab(inode);
	}
	if (invalidate_async) {
		dout(10, "queueing %p for page invalidation\n", inode);
		if (ceph_queue_page_invalidation(inode))
			igrab(inode);
	}
	if (wake)
		wake_up(&ci->i_cap_wq);
	return reply;
}


/*
 * Handle FLUSH_ACK from MDS, indicating that metadata we sent to the
 * MDS has been safely recorded.
 */
static void handle_cap_flush_ack(struct inode *inode,
				 struct ceph_mds_caps *m,
				 struct ceph_mds_session *session,
				 struct ceph_cap *cap)
	__releases(inode->i_lock)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	unsigned seq = le32_to_cpu(m->seq);
	int caps = le32_to_cpu(m->caps);
	int dirty = le32_to_cpu(m->dirty);
	int cleaned = dirty & ~caps;
	int removed_last = 0;

	dout(10, "handle_cap_flush_ack inode %p mds%d seq %d cleaned %s,"
	     " flushing %s -> %s\n",
	     inode, session->s_mds, seq, ceph_cap_string(cleaned),
	     ceph_cap_string(cap->flushing),
	     ceph_cap_string(cap->flushing & ~cleaned));
	cap->flushing &= ~cleaned;

	/* did we release this cap, too? */
	if (caps == 0 && seq == cap->seq) {
		BUG_ON(cap->flushing);
		removed_last = __ceph_remove_cap(cap);
		if (removed_last)
			__cap_delay_cancel(&ceph_inode_to_client(inode)->mdsc,
					   ci);
	}
	spin_unlock(&inode->i_lock);
	if (removed_last)
		iput(inode);
}


/*
 * Handle FLUSHSNAP_ACK.  MDS has flushed snap data to disk and we can
 * throw away our cap_snap.
 *
 * Caller hold s_mutex.
 */
static void handle_cap_flushsnap_ack(struct inode *inode,
				     struct ceph_mds_caps *m,
				     struct ceph_mds_session *session)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	u64 follows = le64_to_cpu(m->snap_follows);
	struct list_head *p;
	struct ceph_cap_snap *capsnap;
	int drop = 0;

	dout(10, "handle_cap_flushsnap_ack inode %p ci %p mds%d follows %lld\n",
	     inode, ci, session->s_mds, follows);

	spin_lock(&inode->i_lock);
	list_for_each(p, &ci->i_cap_snaps) {
		capsnap = list_entry(p, struct ceph_cap_snap, ci_item);
		if (capsnap->follows == follows) {
			WARN_ON(capsnap->dirty_pages || capsnap->writing);
			dout(10, " removing cap_snap %p follows %lld\n",
			     capsnap, follows);
			ceph_put_snap_context(capsnap->context);
			list_del(&capsnap->ci_item);
			ceph_put_cap_snap(capsnap);
			drop = 1;
			break;
		} else {
			dout(10, " skipping cap_snap %p follows %lld\n",
			     capsnap, capsnap->follows);
		}
	}
	spin_unlock(&inode->i_lock);
	if (drop)
		iput(inode);
}


/*
 * Handle TRUNC from MDS, indicating file truncation.
 *
 * caller hold s_mutex.
 */
static void handle_cap_trunc(struct inode *inode,
			     struct ceph_mds_caps *trunc,
			     struct ceph_mds_session *session)
	__releases(inode->i_lock)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	int seq = le32_to_cpu(trunc->seq);
	u32 truncate_seq = le32_to_cpu(trunc->truncate_seq);
	u64 truncate_size = le64_to_cpu(trunc->truncate_size);
	u64 size = le64_to_cpu(trunc->size);
	int implemented = 0;
	int dirty = __ceph_caps_dirty(ci);
	int issued = __ceph_caps_issued(ceph_inode(inode), &implemented);
	int queue_trunc = 0;
	
	issued |= implemented | dirty;

	dout(10, "handle_cap_trunc inode %p mds%d seq %d to %lld seq %d\n",
	     inode, mds, seq, truncate_size, truncate_seq);
	queue_trunc = ceph_fill_file_size(inode, issued,
					  truncate_seq, truncate_size, size);
	spin_unlock(&inode->i_lock);

	if (queue_trunc)
		if (queue_work(ceph_client(inode->i_sb)->trunc_wq,
			       &ci->i_vmtruncate_work))
			igrab(inode);
}

/*
 * Handle EXPORT from MDS.  Cap is being migrated _from_ this mds to a
 * different one.  If we are the most recent migration we've seen (as
 * indicated by mseq), make note of the migrating cap bits for the
 * duration (until we see the corresponding IMPORT).
 *
 * caller holds s_mutex
 */
static void handle_cap_export(struct inode *inode, struct ceph_mds_caps *ex,
			      struct ceph_mds_session *session)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	unsigned mseq = le32_to_cpu(ex->migrate_seq);
	struct ceph_cap *cap = NULL, *t;
	struct rb_node *p;
	int was_last = 0;
	int remember = 1;

	dout(10, "handle_cap_export inode %p ci %p mds%d mseq %d\n",
	     inode, ci, mds, mseq);

	spin_lock(&inode->i_lock);

	/* make sure we haven't seen a higher mseq */
	for (p = rb_first(&ci->i_caps); p; p = rb_next(p)) {
		t = rb_entry(p, struct ceph_cap, ci_node);
		if (ceph_seq_cmp(t->mseq, mseq) > 0) {
			dout(10, " higher mseq on cap from mds%d\n",
			     t->session->s_mds);
			remember = 0;
		}
		if (t->session->s_mds == mds)
			cap = t;
	}

	if (cap) {
		if (remember) {
			/* make note */
			ci->i_cap_exporting_mds = mds;
			ci->i_cap_exporting_mseq = mseq;
			ci->i_cap_exporting_issued = cap->issued;
		}
		was_last = __ceph_remove_cap(cap);
	} else {
		WARN_ON(!cap);
	}

	spin_unlock(&inode->i_lock);
	if (was_last)
		iput(inode);
}

/*
 * Handle cap IMPORT.  If there are temp bits from an older EXPORT,
 * clean them up.
 *
 * caller holds s_mutex.
 */
static void handle_cap_import(struct ceph_mds_client *mdsc, 
			      struct inode *inode, struct ceph_mds_caps *im,
			      struct ceph_mds_session *session,
			      void *snaptrace, int snaptrace_len)
{
	struct ceph_inode_info *ci = ceph_inode(inode);
	int mds = session->s_mds;
	unsigned issued = le32_to_cpu(im->caps);
	unsigned wanted = le32_to_cpu(im->wanted);
	unsigned seq = le32_to_cpu(im->seq);
	unsigned mseq = le32_to_cpu(im->migrate_seq);
	u64 realmino = le64_to_cpu(im->realm);
	unsigned long ttl_ms = le32_to_cpu(im->ttl_ms);
	u64 cap_id = le64_to_cpu(im->cap_id);

	if (ci->i_cap_exporting_mds >= 0 &&
	    ceph_seq_cmp(ci->i_cap_exporting_mseq, mseq) < 0) {
		dout(10, "handle_cap_import inode %p ci %p mds%d mseq %d"
		     " - cleared exporting from mds%d\n",
		     inode, ci, mds, mseq,
		     ci->i_cap_exporting_mds);
		ci->i_cap_exporting_issued = 0;
		ci->i_cap_exporting_mseq = 0;
		ci->i_cap_exporting_mds = -1;
	} else {
		dout(10, "handle_cap_import inode %p ci %p mds%d mseq %d\n",
		     inode, ci, mds, mseq);
	}
	
	down_write(&mdsc->snap_rwsem);
	ceph_update_snap_trace(mdsc, snaptrace, snaptrace+snaptrace_len,
			       false);
	downgrade_write(&mdsc->snap_rwsem);
	ceph_add_cap(inode, session, cap_id, -1,
		     issued, wanted, seq, mseq, realmino,
		     ttl_ms, jiffies - ttl_ms/2, NULL);
	up_read(&mdsc->snap_rwsem);
}


/*
 * Handle a CEPH_CAPS message from the MDS.
 *
 * Identify the appropriate session, inode, and call the right handler
 * based on the cap op.
 */
void ceph_handle_caps(struct ceph_mds_client *mdsc,
		      struct ceph_msg *msg)
{
	struct super_block *sb = mdsc->client->sb;
	struct ceph_mds_session *session;
	struct inode *inode;
	struct ceph_cap *cap;
	struct ceph_mds_caps *h;
	int mds = le32_to_cpu(msg->hdr.src.name.num);
	int op;
	u32 seq;
	struct ceph_vino vino;
	u64 cap_id;
	u64 size, max_size;
	int check_caps = 0;
	void *xattr_data = NULL;
	int r;

	dout(10, "handle_caps from mds%d\n", mds);

	/* decode */
	if (msg->front.iov_len < sizeof(*h))
		goto bad;
	h = msg->front.iov_base;
	op = le32_to_cpu(h->op);
	vino.ino = le64_to_cpu(h->ino);
	vino.snap = CEPH_NOSNAP;
	cap_id = le64_to_cpu(h->cap_id);
	seq = le32_to_cpu(h->seq);
	size = le64_to_cpu(h->size);
	max_size = le64_to_cpu(h->max_size);

	/* find session */
	mutex_lock(&mdsc->mutex);
	session = __ceph_lookup_mds_session(mdsc, mds);
	mutex_unlock(&mdsc->mutex);
	if (!session) {
		dout(10, "WTF, got cap but no session for mds%d\n", mds);
		return;
	}

	mutex_lock(&session->s_mutex);
	session->s_seq++;
	dout(20, " mds%d seq %lld\n", session->s_mds, session->s_seq);

	/* lookup ino */
	inode = ceph_find_inode(sb, vino);
	dout(20, " op %s ino %llx inode %p\n", ceph_cap_op_name(op), vino.ino,
	     inode);
	if (!inode) {
		dout(10, " i don't have ino %llx, sending release\n", vino.ino);
		goto release;
	}

	/* these will work even if we don't have a cap yet */
	switch (op) {
	case CEPH_CAP_OP_FLUSHSNAP_ACK:
		handle_cap_flushsnap_ack(inode, h, session);
		goto done;

	case CEPH_CAP_OP_EXPORT:
		handle_cap_export(inode, h, session);
		if (list_empty(&session->s_caps))
			ceph_mdsc_flushed_all_caps(mdsc, session);
		goto done;

	case CEPH_CAP_OP_IMPORT:
		handle_cap_import(mdsc, inode, h, session,
				  msg->front.iov_base + sizeof(*h),
				  le32_to_cpu(h->snap_trace_len));
		check_caps = 1; /* we may have sent a RELEASE to the old auth */
		goto done;
	}

	/* preallocate space for xattrs? */
	if (le32_to_cpu(h->xattr_len) > 4)
		xattr_data = kmalloc(le32_to_cpu(h->xattr_len), GFP_NOFS);

	/* the rest require a cap */
	spin_lock(&inode->i_lock);
	cap = __get_cap_for_mds(inode, mds);
	if (!cap) {
		dout(10, "no cap on %p ino %llx.%llx from mds%d, releasing\n",
		     inode, ceph_ino(inode), ceph_snap(inode), mds);
		spin_unlock(&inode->i_lock);
		goto release;
	}
	
	/* note that each of these drops i_lock for us */
	switch (op) {
	case CEPH_CAP_OP_GRANT:
		r = handle_cap_grant(inode, h, session, cap,&xattr_data);
		if (r == 1) {
			dout(10, " sending reply back to mds%d\n", mds);
			ceph_msg_get(msg);
			ceph_send_msg_mds(mdsc, msg, mds);
		} else if (r == 2) {
			ceph_check_caps(ceph_inode(inode), 1, 0, session);
			goto done_unlocked;
		}
		break;

	case CEPH_CAP_OP_FLUSH_ACK:
		handle_cap_flush_ack(inode, h, session, cap);
		if (list_empty(&session->s_caps))
			ceph_mdsc_flushed_all_caps(mdsc, session);
		break;

	case CEPH_CAP_OP_TRUNC:
		handle_cap_trunc(inode, h, session);
		break;

	default:
		spin_unlock(&inode->i_lock);
		derr(10, " unknown cap op %d %s\n", op, ceph_cap_op_name(op));
	}

done:
	mutex_unlock(&session->s_mutex);
done_unlocked:
	ceph_put_mds_session(session);

	kfree(xattr_data);
	if (check_caps)
		ceph_check_caps(ceph_inode(inode), 1, 0, NULL);
	if (inode)
		iput(inode);
	return;

bad:
	derr(10, "corrupt caps message\n");
	return;

release:
	send_cap_msg(mdsc, vino.ino, cap_id, CEPH_CAP_OP_RELEASE,
		     0, 0, 0,
		     seq, 0,
		     size, 0, NULL, NULL, 0,
		     0, 0, 0,
		     0, mds);
	goto done;
}


/*
 * Delayed work handler to process end of delayed cap release LRU list.
 */
void ceph_check_delayed_caps(struct ceph_mds_client *mdsc)
{
	struct ceph_inode_info *ci;

	dout(10, "check_delayed_caps\n");
	while (1) {
		spin_lock(&mdsc->cap_delay_lock);
		if (list_empty(&mdsc->cap_delay_list))
			break;
		ci = list_first_entry(&mdsc->cap_delay_list,
				      struct ceph_inode_info,
				      i_cap_delay_list);
		if (time_before(jiffies, ci->i_hold_caps_until))
			break;
		list_del_init(&ci->i_cap_delay_list);
		spin_unlock(&mdsc->cap_delay_lock);
		dout(10, "check_delayed_caps on %p\n", &ci->vfs_inode);
		ceph_check_caps(ci, 1, 0, NULL);
		iput(&ci->vfs_inode);
	}
	spin_unlock(&mdsc->cap_delay_lock);
}


/*
 * caller must hold session s_mutex
 */
void ceph_trim_session_rdcaps(struct ceph_mds_session *session)
{
	struct inode *inode;
	struct ceph_cap *cap;
	struct list_head *p, *n;
	int wanted;

	dout(10, "trim_rdcaps for mds%d\n", session->s_mds);
	list_for_each_safe(p, n, &session->s_rdcaps) {
		int last_cap = 0;

		cap = list_entry(p, struct ceph_cap, session_rdcaps);

		inode = &cap->ci->vfs_inode;
		spin_lock(&inode->i_lock);

		if (time_before(jiffies, cap->expires)) {
			dout(20, " stopping at %p cap %p expires %lu > %lu\n",
			     inode, cap, cap->expires, jiffies);
			spin_unlock(&inode->i_lock);
			break;
		}

		/* wanted? */
		wanted = __ceph_caps_wanted(cap->ci);
		if (wanted == 0) {
			dout(20, " dropping %p cap %p\n", inode, cap);
			last_cap = __ceph_remove_cap(cap);
		} else {
			dout(20, " keeping %p cap %p (wanted %s)\n", inode, cap,
			     ceph_cap_string(wanted));
		}
		spin_unlock(&inode->i_lock);
		if (last_cap)
			iput(inode);
	}
}
