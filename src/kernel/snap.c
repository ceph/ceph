
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
		kfree(realm->cached_snaps);
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
	int err = 0;
	int i;
	int num = realm->num_prior_parent_snaps + realm->num_snaps;

	if (parent) {
		if (!parent->cached_seq) {
			err = ceph_snaprealm_build_context(parent);
			if (err)
				goto fail;
		}
		num += parent->num_cached_snaps;  /* possible overestimate */
	}

	if (realm->cached_snaps)
		kfree(realm->cached_snaps);
	err = -ENOMEM;
	realm->cached_snaps = kmalloc(num * sizeof(u64), GFP_NOFS);
	if (!realm->cached_snaps)
		goto fail;

	/* build (reverse sorted) snap vector */
	num = 0;
	realm->cached_seq = realm->seq;
	if (parent) {
		for (i = 0; i < parent->num_cached_snaps; i++)
			if (parent->cached_snaps[i] >= realm->parent_since)
				realm->cached_snaps[num++] =
					parent->cached_snaps[i];
		if (parent->cached_seq > realm->cached_seq)
			realm->cached_seq = parent->cached_seq;
	}
	memcpy(realm->cached_snaps + num, realm->snaps,
	       sizeof(u64)*realm->num_snaps);
	num += realm->num_snaps;
	memcpy(realm->cached_snaps + num, realm->prior_parent_snaps,
	       sizeof(u64)*realm->num_prior_parent_snaps);
	num += realm->num_prior_parent_snaps;

	sort(realm->cached_snaps, num, sizeof(u64), cmpu64_rev, NULL);
	realm->num_cached_snaps = num;
	dout(10, "snaprealm_build_context %llx %p : seq %lld %d snaps\n",
	     realm->ino, realm, realm->cached_seq, realm->num_cached_snaps);
	return 0;

fail:
	if (realm->cached_snaps) {
		kfree(realm->cached_snaps);
		realm->cached_snaps = 0;
	}
	derr(0, "snaprealm_build_context %llx %p fail %d\n", realm->ino,
	     realm, err);
	return err;
}

void ceph_invalidate_snaprealm(struct ceph_snaprealm *realm)
{
	struct list_head *p;
	struct ceph_snaprealm *child;

	dout(10, "invalidate_snaprealm %llx %p\n", realm->ino, realm);
	realm->cached_seq = 0;
	
	list_for_each(p, &realm->children) {
		child = list_entry(p, struct ceph_snaprealm, child_item);
		ceph_invalidate_snaprealm(child);
	}
}


static int dup_array(u64 **dst, u64 *src, int num)
{
	if (*dst)
		kfree(*dst);
	if (num) {
		*dst = kmalloc(sizeof(u64) * num, GFP_NOFS);
		if (!*dst)
			return -1;
		memcpy(*dst, src, sizeof(u64) * num);
	} else
		*dst = 0;
	return 0;
}

u64 ceph_update_snap_trace(struct ceph_client *client,
			   void *p, void *e, int must_flush)
{
	struct ceph_mds_snap_realm *ri;
	int err = -ENOMEM;
	u64 first = 0;
	u64 *snaps;
	u64 *prior_parent_snaps;
	struct ceph_snaprealm *realm;
	int invalidate;

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
	if (!first)
		first = realm->ino;

	if (le64_to_cpu(ri->seq) > realm->seq) {
		dout(10, "update_snap_trace updating %llx %p %lld -> %lld\n",
		     realm->ino, realm, realm->seq, le64_to_cpu(ri->seq));
		
		// flush caps... and data?

	} else
		dout(10, "update_snap_trace %llx %p seq %lld unchanged\n",
		     realm->ino, realm, realm->seq);

	invalidate = ceph_adjust_snaprealm_parent(client, realm,
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

	if (invalidate)
		ceph_invalidate_snaprealm(realm);

	ceph_put_snaprealm(realm);
	if (p < e)
		goto more;
	return first;

bad:
	err = -EINVAL;
fail:
	derr(10, "update_snap_trace error %d\n", err);
	return 0;	
}



