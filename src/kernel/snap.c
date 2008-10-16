
#include <linux/radix-tree.h>
#include <linux/sort.h>

#include "ceph_debug.h"

int ceph_debug_snap = -1;
#define DOUT_MASK DOUT_MASK_SNAP
#define DOUT_VAR ceph_debug_snap
#define DOUT_PREFIX "snap: "

#include "super.h"
#include "decode.h"


static struct ceph_snap_realm *ceph_get_snap_realm(struct ceph_mds_client *mdsc, u64 ino)
{
	struct ceph_snap_realm *realm;

	realm = radix_tree_lookup(&mdsc->snap_realms, ino);
	if (!realm) {
		realm = kzalloc(sizeof(*realm), GFP_NOFS);
		radix_tree_insert(&mdsc->snap_realms, ino, realm);
		realm->nref = 1;    /* in tree */
		realm->ino = ino;
		INIT_LIST_HEAD(&realm->children);
		INIT_LIST_HEAD(&realm->child_item);
		INIT_LIST_HEAD(&realm->inodes_with_caps);
		dout(20, "get_snap_realm created %llx %p\n", realm->ino, realm);
	}
	dout(20, "get_snap_realm %llx %p %d -> %d\n", realm->ino, realm,
	     realm->nref, realm->nref+1);
	realm->nref++;
	return realm;
}

void ceph_put_snap_realm(struct ceph_snap_realm *realm)
{
	dout(20, "put_snap_realm %llx %p %d -> %d\n", realm->ino, realm,
	     realm->nref, realm->nref-1);
	realm->nref--;
	if (realm->nref == 0) {
		kfree(realm->prior_parent_snaps);
		kfree(realm->snaps);
		ceph_put_snap_context(realm->cached_context);
		kfree(realm);
	}
}

static int adjust_snap_realm_parent(struct ceph_mds_client *mdsc,
					struct ceph_snap_realm *realm,
					u64 parentino)
{
	struct ceph_snap_realm *parent;

	if (realm->parent_ino == parentino)
		return 0;

	parent = ceph_get_snap_realm(mdsc, parentino);
	if (!parent)
		return -ENOMEM;
	dout(20, "adjust_snap_realm_parent %llx %p: %llx %p -> %llx %p\n",
	     realm->ino, realm, realm->parent_ino, realm->parent,
	     parentino, parent);
	if (realm->parent) {
		list_del_init(&realm->child_item);
		ceph_put_snap_realm(realm->parent);
	}
	realm->parent_ino = parentino;
	realm->parent = parent;
	list_add(&realm->child_item, &parent->children);
	return 1;
}


static int cmpu64_rev(const void *a, const void *b)
{
	if (*(u64 *)a < *(u64 *)b)
		return 1;
	if (*(u64 *)a > *(u64 *)b)
		return -1;
	return 0;
}

static int build_snap_context(struct ceph_snap_realm *realm)
{
	struct ceph_snap_realm *parent = realm->parent;
	struct ceph_snap_context *sc;
	int err = 0;
	int i;
	int num = realm->num_prior_parent_snaps + realm->num_snaps;

	if (parent) {
		if (!parent->cached_context) {
			err = build_snap_context(parent);
			if (err)
				goto fail;
		}
		num += parent->cached_context->num_snaps;
	}

	/* do i need to update? */
	if (realm->cached_context && realm->cached_context->seq <= realm->seq &&
	    (!parent ||
	     realm->cached_context->seq <= parent->cached_context->seq)) {
		dout(10, "build_snap_context %llx %p: %p seq %lld (%d snaps)"
		     " (unchanged)\n",
		     realm->ino, realm, realm->cached_context,
		     realm->cached_context->seq,
		     realm->cached_context->num_snaps);
		return 0;
	}

	/* build new */
	err = -ENOMEM;
	sc = kzalloc(sizeof(*sc) + num*sizeof(u64), GFP_NOFS);
	if (!sc)
		goto fail;
	atomic_set(&sc->nref, 1);

	/* build (reverse sorted) snap vector */
	num = 0;
	sc->seq = realm->seq;
	if (parent) {
		for (i = 0; i < parent->cached_context->num_snaps; i++)
			if (parent->cached_context->snaps[i] >=
			    realm->parent_since)
				sc->snaps[num++] =
					parent->cached_context->snaps[i];
		if (parent->cached_context->seq > sc->seq)
			sc->seq = parent->cached_context->seq;
	}
	memcpy(sc->snaps + num, realm->snaps,
	       sizeof(u64)*realm->num_snaps);
	num += realm->num_snaps;
	memcpy(sc->snaps + num, realm->prior_parent_snaps,
	       sizeof(u64)*realm->num_prior_parent_snaps);
	num += realm->num_prior_parent_snaps;

	sort(sc->snaps, num, sizeof(u64), cmpu64_rev, NULL);
	sc->num_snaps = num;
	dout(10, "build_snap_context %llx %p: %p seq %lld (%d snaps)\n",
	     realm->ino, realm, sc, sc->seq, sc->num_snaps);

	if (realm->cached_context)
		ceph_put_snap_context(realm->cached_context);
	realm->cached_context = sc;
	return 0;

fail:
	if (realm->cached_context) {
		ceph_put_snap_context(realm->cached_context);
		realm->cached_context = NULL;
	}
	derr(0, "build_snap_context %llx %p fail %d\n", realm->ino,
	     realm, err);
	return err;
}

static void rebuild_snap_realms(struct ceph_snap_realm *realm)
{
	struct list_head *p;
	struct ceph_snap_realm *child;

	dout(10, "rebuild_snap_realms %llx %p\n", realm->ino, realm);
	build_snap_context(realm);

	list_for_each(p, &realm->children) {
		child = list_entry(p, struct ceph_snap_realm, child_item);
		rebuild_snap_realms(child);
	}
}


static int dup_array(u64 **dst, __le64 *src, int num)
{
	int i;

	if (*dst)
		kfree(*dst);
	if (num) {
		*dst = kmalloc(sizeof(u64) * num, GFP_NOFS);
		if (!*dst)
			return -ENOMEM;
		for (i = 0; i < num; i++)
			(*dst)[i] = le64_to_cpu(src[i]);
	} else
		*dst = NULL;
	return 0;
}

void __ceph_finish_cap_snap(struct ceph_inode_info *ci,
			    struct ceph_cap_snap *capsnap,
			    int used)
{
	struct inode *inode = &ci->vfs_inode;

	capsnap->size = inode->i_size;
	capsnap->mtime = inode->i_mtime;
	capsnap->atime = inode->i_atime;
	capsnap->ctime = inode->i_ctime;
	capsnap->time_warp_seq = ci->i_time_warp_seq;
	if (used & CEPH_CAP_WRBUFFER) {
		dout(10, "finish_cap_snap %p cap_snap %p snapc %p %llu used %d,"
		     " WRBUFFER, delaying\n", inode, capsnap, capsnap->context,
		     capsnap->context->seq, used);
	} else {
		BUG_ON(ci->i_wrbuffer_ref_head);
		BUG_ON(capsnap->dirty);
		__ceph_flush_snaps(ci);
	}
}

void ceph_queue_cap_snap(struct ceph_inode_info *ci,
			 struct ceph_snap_context *snapc)
{
	struct inode *inode = &ci->vfs_inode;
	int used;
	struct ceph_cap_snap *capsnap;

	capsnap = kzalloc(sizeof(*capsnap), GFP_NOFS);
	if (!capsnap) {
		derr(10, "ENOMEM allocating ceph_cap_snap on %p\n", inode);
		return;
	}

	spin_lock(&inode->i_lock);
	used = __ceph_caps_used(ci);
	if (__ceph_have_pending_cap_snap(ci)) {
		dout(10, "queue_cap_snap %p snapc %p seq %llu used %d"
		     " already pending\n", inode, snapc, snapc->seq, used);
		kfree(capsnap);
	} else {
		igrab(inode);
		capsnap->follows = snapc->seq;
		capsnap->context = ceph_get_snap_context(snapc);
		capsnap->issued = __ceph_caps_issued(ci, NULL);
		capsnap->dirty = ci->i_wrbuffer_ref_head;
		ci->i_wrbuffer_ref_head = 0;
		list_add_tail(&capsnap->ci_item, &ci->i_cap_snaps);

		if (used & CEPH_CAP_WR) {
			dout(10, "queue_cap_snap %p cap_snap %p snapc %p"
			     " seq %llu used WR, now pending\n", inode,
			     capsnap, snapc, snapc->seq);
			capsnap->writing = 1;
		} else {
			__ceph_finish_cap_snap(ci, capsnap, used);
		}
	}

	spin_unlock(&inode->i_lock);
}


struct ceph_snap_realm *ceph_update_snap_trace(struct ceph_mds_client *mdsc,
					      void *p, void *e, int must_flush)
{
	struct ceph_mds_snap_realm *ri;
	int err = -ENOMEM;
	__le64 *snaps;
	__le64 *prior_parent_snaps;
	struct ceph_snap_realm *realm, *first = NULL;
	int invalidate = 0;

	dout(10, "update_snap_trace must_flush=%d\n", must_flush);
more:
	ceph_decode_need(&p, e, sizeof(*ri), bad);
	ri = p;
	p += sizeof(*ri);
	ceph_decode_need(&p, e, sizeof(u64)*(le32_to_cpu(ri->num_snaps) +
			    le32_to_cpu(ri->num_prior_parent_snaps)), bad);
	snaps = p;
	p += sizeof(u64) * le32_to_cpu(ri->num_snaps);
	prior_parent_snaps = p;
	p += sizeof(u64) * le32_to_cpu(ri->num_prior_parent_snaps);

	realm = ceph_get_snap_realm(mdsc, le64_to_cpu(ri->ino));
	if (!realm)
		goto fail;
	if (!first) {
		first = realm;
		realm->nref++;
	}

	if (le64_to_cpu(ri->seq) > realm->seq) {
		struct list_head *pi;
		dout(10, "update_snap_trace updating %llx %p %lld -> %lld\n",
		     realm->ino, realm, realm->seq, le64_to_cpu(ri->seq));

		list_for_each(pi, &realm->inodes_with_caps) {
			struct ceph_inode_info *ci =
				list_entry(pi, struct ceph_inode_info,
					   i_snap_realm_item);
			ceph_queue_cap_snap(ci, realm->cached_context);
		}
		dout(20, "update_snap_trace cap flush done\n");

	} else
		dout(10, "update_snap_trace %llx %p seq %lld unchanged\n",
		     realm->ino, realm, realm->seq);

	invalidate += adjust_snap_realm_parent(mdsc, realm,
					      le64_to_cpu(ri->parent));

	if (le64_to_cpu(ri->seq) > realm->seq) {
		realm->seq = le64_to_cpu(ri->seq);
		realm->created = le64_to_cpu(ri->created);
		realm->parent_since = le64_to_cpu(ri->parent_since);

		realm->num_snaps = le32_to_cpu(ri->num_snaps);
		if (dup_array(&realm->snaps, snaps, realm->num_snaps) < 0)
			goto fail;

		realm->num_prior_parent_snaps =
			le32_to_cpu(ri->num_prior_parent_snaps);
		if (dup_array(&realm->prior_parent_snaps, prior_parent_snaps,
			      realm->num_prior_parent_snaps) < 0)
			goto fail;
		invalidate = 1;
	} else if (!realm->cached_context)
		invalidate = 1;

	dout(10, "done with %llx %p, invalidated=%d, %p %p\n", realm->ino,
	     realm, invalidate, p, e);

	if (p >= e && invalidate)
		rebuild_snap_realms(realm);

	ceph_put_snap_realm(realm);
	if (p < e)
		goto more;

	return first;

bad:
	err = -EINVAL;
fail:
	derr(10, "update_snap_trace error %d\n", err);
	return ERR_PTR(err);
}



/*
 * snap
 */

void ceph_handle_snap(struct ceph_mds_client *mdsc,
		      struct ceph_msg *msg)
{
	struct super_block *sb = mdsc->client->sb;
	struct ceph_mds_session *session;
	int mds = le32_to_cpu(msg->hdr.src.name.num);
	u64 split;
	int op;
	int trace_len;
	struct ceph_snap_realm *realm = NULL;
	void *p = msg->front.iov_base;
	void *e = p + msg->front.iov_len;
	struct ceph_mds_snap_head *h;
	int num_split_inos, num_split_realms;
	__le64 *split_inos = NULL, *split_realms = NULL;
	int i;

	/* decode */
	if (msg->front.iov_len < sizeof(*h))
		goto bad;
	h = p;
	op = le32_to_cpu(h->op);
	split = le64_to_cpu(h->split);
	trace_len = le32_to_cpu(h->trace_len);
	num_split_inos = le32_to_cpu(h->num_split_inos);
	num_split_realms = le32_to_cpu(h->num_split_realms);
	p += sizeof(*h);

	dout(10, "handle_snap from mds%d op %s split %llx tracelen %d\n", mds,
	     ceph_snap_op_name(op), split, trace_len);

	/* find session */
	mutex_lock(&mdsc->mutex);
	session = __ceph_get_mds_session(mdsc, mds);
	if (session)
		down_write(&mdsc->snap_rwsem);
	mutex_unlock(&mdsc->mutex);
	if (!session) {
		dout(10, "WTF, got snap but no session for mds%d\n", mds);
		return;
	}

	mutex_lock(&session->s_mutex);
	session->s_seq++;
	mutex_unlock(&session->s_mutex);

	if (op == CEPH_SNAP_OP_SPLIT) {
		struct ceph_mds_snap_realm *ri;

		split_inos = p;
		p += sizeof(u64) * num_split_inos;
		split_realms = p;
		p += sizeof(u64) * num_split_realms;
		ceph_decode_need(&p, e, sizeof(*ri), bad);
		ri = p;

		realm = ceph_get_snap_realm(mdsc, split);
		if (IS_ERR(realm))
			goto out;
		dout(10, "splitting snap_realm %llx %p\n", realm->ino, realm);

		for (i = 0; i < num_split_inos; i++) {
			struct ceph_vino vino = {
				.ino = le64_to_cpu(split_inos[i]),
				.snap = CEPH_NOSNAP,
			};
			struct inode *inode = ceph_find_inode(sb, vino);
			struct ceph_inode_info *ci;
			if (!inode)
				continue;
			ci = ceph_inode(inode);
			spin_lock(&inode->i_lock);
			if (!ci->i_snap_realm)
				goto skip_inode;
			if (ci->i_snap_realm->created > le64_to_cpu(ri->created)) {
				dout(15, " leaving %p in newer realm %llx %p\n",
				     inode, ci->i_snap_realm->ino,
				     ci->i_snap_realm);
				goto skip_inode;
			}
			dout(15, " will move %p to split realm %llx %p\n",
			     inode, realm->ino, realm);
			/*
			 * remove from list, but don't re-add yet.  we
			 * don't want the caps to be flushed (again) by
			 * ceph_update_snap_trace below.
			 */
			list_del_init(&ci->i_snap_realm_item);
			spin_unlock(&inode->i_lock);

			ceph_queue_cap_snap(ci,
					    ci->i_snap_realm->cached_context);

			iput(inode);
			continue;

		skip_inode:
			spin_unlock(&inode->i_lock);
			iput(inode);
		}

		for (i = 0; i < num_split_realms; i++) {
			struct ceph_snap_realm *child =
				ceph_get_snap_realm(mdsc,
					   le64_to_cpu(split_realms[i]));
			if (!child)
				continue;
			adjust_snap_realm_parent(mdsc, child, realm->ino);
			ceph_put_snap_realm(child);
		}

		ceph_put_snap_realm(realm);
	}

	realm = ceph_update_snap_trace(mdsc, p, e,
				       op != CEPH_SNAP_OP_DESTROY);
	if (IS_ERR(realm))
		goto bad;

	if (op == CEPH_SNAP_OP_SPLIT) {
		for (i = 0; i < num_split_inos; i++) {
			struct ceph_vino vino = {
				.ino = le64_to_cpu(split_inos[i]),
				.snap = CEPH_NOSNAP,
			};
			struct inode *inode = ceph_find_inode(sb, vino);
			struct ceph_inode_info *ci;
			if (!inode)
				continue;
			ci = ceph_inode(inode);
			spin_lock(&inode->i_lock);
			/* _now_ add to newly split realm */
			ceph_put_snap_realm(ci->i_snap_realm);
			list_add(&ci->i_snap_realm_item,
				 &realm->inodes_with_caps);
			ci->i_snap_realm = realm;
			realm->nref++;
			spin_unlock(&inode->i_lock);
		}
	}

	ceph_put_snap_realm(realm);
	up_write(&mdsc->snap_rwsem);
	return;

bad:
	derr(10, "corrupt snap message from mds%d\n", mds);
out:
	return;
}



