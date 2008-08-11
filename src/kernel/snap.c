
#include <linux/sort.h>

int ceph_debug_snap = -1;
#define DOUT_VAR ceph_debug_snap
#define DOUT_PREFIX "snap: "

#include "super.h"
#include "decode.h"


struct ceph_snaprealm *ceph_get_snaprealm(struct ceph_client *client, u64 ino)
{
	struct ceph_snaprealm *realm;

	realm = radix_tree_lookup(&client->snaprealms, ino);
	if (!realm) {
		realm = kzalloc(sizeof(*realm), GFP_NOFS);
		radix_tree_insert(&client->snaprealms, ino, realm);
		realm->nref = 1;    /* in tree */
		realm->ino = ino;
		INIT_LIST_HEAD(&realm->children);
		INIT_LIST_HEAD(&realm->child_item);
		INIT_LIST_HEAD(&realm->inodes_with_caps);
		dout(20, "get_snaprealm created %llx %p\n", realm->ino, realm);
	}
	dout(20, "get_snaprealm %llx %p %d -> %d\n", realm->ino, realm,
	     realm->nref, realm->nref+1);
	realm->nref++;
	return realm;
}

struct ceph_snaprealm *ceph_find_snaprealm(struct ceph_client *client, u64 ino)
{
	struct ceph_snaprealm *realm;

	realm = radix_tree_lookup(&client->snaprealms, ino);

	return realm;
}

void ceph_put_snaprealm(struct ceph_snaprealm *realm)
{
	dout(20, "put_snaprealm %llx %p %d -> %d\n", realm->ino, realm,
	     realm->nref, realm->nref-1);
	realm->nref--;
	if (realm->nref == 0) {
		kfree(realm->prior_parent_snaps);
		kfree(realm->snaps);
		ceph_put_snap_context(realm->cached_context);
		kfree(realm);
	}
}

int ceph_adjust_snaprealm_parent(struct ceph_client *client,
				 struct ceph_snaprealm *realm, u64 parentino)
{
	struct ceph_snaprealm *parent;

	if (realm->parent_ino == parentino)
		return 0;

	parent = ceph_get_snaprealm(client, parentino);
	if (!parent)
		return -ENOMEM;
	dout(20, "adjust_snaprealm_parent %llx %p: %llx %p -> %llx %p\n",
	     realm->ino, realm, realm->parent_ino, realm->parent,
	     parentino, parent);
	if (realm->parent) {
		list_del_init(&realm->child_item);
		ceph_put_snaprealm(realm->parent);
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

int ceph_snaprealm_build_context(struct ceph_snaprealm *realm)
{
	struct ceph_snaprealm *parent = realm->parent;
	struct ceph_snap_context *sc;
	int err = 0;
	int i;
	int num = realm->num_prior_parent_snaps + realm->num_snaps;

	if (parent) {
		if (!parent->cached_context) {
			err = ceph_snaprealm_build_context(parent);
			if (err)
				goto fail;
		}
		num += parent->cached_context->num_snaps;
	}

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
	dout(10, "snaprealm_build_context %llx %p : seq %lld %d snaps\n",
	     realm->ino, realm, sc->seq, sc->num_snaps);

	if (realm->cached_context)
		ceph_put_snap_context(realm->cached_context);
	realm->cached_context = sc;
	return 0;

fail:
	if (realm->cached_context) {
		ceph_put_snap_context(realm->cached_context);
		realm->cached_context = 0;
	}
	derr(0, "snaprealm_build_context %llx %p fail %d\n", realm->ino,
	     realm, err);
	return err;
}

void ceph_rebuild_snaprealms(struct ceph_snaprealm *realm)
{
	struct list_head *p;
	struct ceph_snaprealm *child;

	dout(10, "rebuild_snaprealms %llx %p\n", realm->ino, realm);
	ceph_snaprealm_build_context(realm);

	list_for_each(p, &realm->children) {
		child = list_entry(p, struct ceph_snaprealm, child_item);
		ceph_rebuild_snaprealms(child);
	}
}


static int dup_array(u64 **dst, u64 *src, int num)
{
	int i;

	if (*dst)
		kfree(*dst);
	if (num) {
		*dst = kmalloc(sizeof(u64) * num, GFP_NOFS);
		if (!*dst)
			return -1;
		for (i = 0; i < num; i++)
			(*dst)[i] = le64_to_cpu(src[i]);
	} else
		*dst = 0;
	return 0;
}

struct ceph_snaprealm *ceph_update_snap_trace(struct ceph_client *client,
					      void *p, void *e, int must_flush)
{
	struct ceph_mds_snap_realm *ri;
	int err = -ENOMEM;
	u64 *snaps;
	u64 *prior_parent_snaps;
	struct ceph_snaprealm *realm, *first = 0;
	int invalidate = 0;

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

	realm = ceph_get_snaprealm(client, le64_to_cpu(ri->ino));
	if (!realm)
		goto fail;
	if (!first) {
		first = realm;
		realm->nref++;
	}

	if (le64_to_cpu(ri->seq) > realm->seq) {
		dout(10, "update_snap_trace updating %llx %p %lld -> %lld\n",
		     realm->ino, realm, realm->seq, le64_to_cpu(ri->seq));
		
		// flush caps... and data?

	} else
		dout(10, "update_snap_trace %llx %p seq %lld unchanged\n",
		     realm->ino, realm, realm->seq);

	invalidate += ceph_adjust_snaprealm_parent(client, realm,
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
	}

	ceph_put_snaprealm(realm);
	if (p < e)
		goto more;

	if (invalidate)
		ceph_rebuild_snaprealms(realm);

	return first;

bad:
	err = -EINVAL;
fail:
	derr(10, "update_snap_trace error %d\n", err);
	return 0;	
}



