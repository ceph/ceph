#include <linux/ctype.h>
#include <linux/kobject.h>

#include "super.h"

static struct kobject ceph_kobj;

/*
 * default kobject attribute operations.  duplicated here from
 * kobject.c
 */

#define DEF_ATTR_OP(name) \
static ssize_t name##_attr_show(struct kobject *kobj, struct attribute *attr,\
				char *buf)				\
{									\
	struct name##_attr *a =						\
		container_of(attr, struct name##_attr, attr);		\
	struct name *c = container_of(kobj, struct name, kobj);		\
	ssize_t ret = -EIO;						\
									\
	if (a->show)							\
		ret = a->show(c, a, buf);				\
	return ret;							\
}									\
									\
static ssize_t name##_attr_store(struct kobject *kobj, struct attribute *attr, \
				 const char *buf, size_t count)		\
{									\
	struct name##_attr *a = container_of(attr, struct name##_attr, attr); \
	struct name *c = container_of(kobj, struct name, kobj);		\
	ssize_t ret = -EIO;						\
									\
	if (a->store)							\
		ret = a->store(c, a, buf, count);			\
	return ret;							\
}									\
									\
static struct sysfs_ops name##_sysfs_ops = {				\
	.show	= name##_attr_show,					\
	.store	= name##_attr_store,					\
};									\
									\
static struct kobj_type name##_ops = {					\
	.sysfs_ops = &name##_sysfs_ops,					\
};

#define ADD_ENTITY_ATTR(ent, a, n, m, sh, st) \
	ent->a.attr.name = n; \
	ent->a.attr.mode = m; \
	ent->a.show = sh; \
	ent->a.store = st; \
	ret = sysfs_create_file(&ent->kobj, &ent->a.attr);



DEF_ATTR_OP(ceph_client)
DEF_ATTR_OP(ceph_mds_request)
DEF_ATTR_OP(ceph_osd_request)
DEF_ATTR_OP(ceph_mon_statfs_request)

/*
 * per-client attributes
 */
#define to_client(c) container_of(c, struct ceph_client, kobj)

static ssize_t fsid_show(struct ceph_client *client,
			 struct ceph_client_attr *attr, char *buf)
{
	return sprintf(buf, "%llx.%llx\n",
	       le64_to_cpu(__ceph_fsid_major(&client->monc.monmap->fsid)),
	       le64_to_cpu(__ceph_fsid_minor(&client->monc.monmap->fsid)));
}

static ssize_t monmap_show(struct ceph_client *client,
			   struct ceph_client_attr *attr, char *buf)
{
	int i, pos;

	if (client->monc.monmap == NULL)
		return 0;

	pos = sprintf(buf, "epoch %d\n", client->monc.monmap->epoch);
	for (i = 0; i < client->monc.monmap->num_mon; i++) {
		struct ceph_entity_inst *inst =
			&client->monc.monmap->mon_inst[i];

		if (pos > PAGE_SIZE - 128)
			break; /* be conservative */
		pos += sprintf(buf + pos, "\t%s%d\t%u.%u.%u.%u:%u\n",
			       ENTITY_NAME(inst->name),
			       IPQUADPORT(inst->addr.ipaddr));
	}
	return pos;
}

static ssize_t mdsmap_show(struct ceph_client *client,
			   struct ceph_client_attr *attr, char *buf)
{
	int i, pos;

	if (client->mdsc.mdsmap == NULL)
		return 0;
	pos = sprintf(buf, "epoch %d\n", client->mdsc.mdsmap->m_epoch);
	pos += sprintf(buf + pos, "root %d\n", client->mdsc.mdsmap->m_root);
	pos += sprintf(buf + pos, "session_timeout %d\n",
		       client->mdsc.mdsmap->m_session_timeout);
	pos += sprintf(buf + pos, "session_autoclose %d\n",
		       client->mdsc.mdsmap->m_session_autoclose);
	for (i = 0; i < client->mdsc.mdsmap->m_max_mds; i++) {
		struct ceph_entity_addr *addr = &client->mdsc.mdsmap->m_addr[i];
		int state = client->mdsc.mdsmap->m_state[i];

		if (pos > PAGE_SIZE - 128)
			break; /* be conservative */
		pos += sprintf(buf+pos, "\tmds%d\t%u.%u.%u.%u:%u\t(%s)\n",
			       i,
			       IPQUADPORT(addr->ipaddr),
			       ceph_mds_state_name(state));
	}
	return pos;
}

static ssize_t osdmap_show(struct ceph_client *client,
			   struct ceph_client_attr *attr, char *buf)
{
	int i, pos;

	if (client->osdc.osdmap == NULL)
		return 0;
	pos = sprintf(buf, "epoch %d\n", client->osdc.osdmap->epoch);
	pos += sprintf(buf + pos, "pg_num %d / %d\n",
		       client->osdc.osdmap->pg_num,
		       client->osdc.osdmap->pg_num_mask);
	pos += sprintf(buf + pos, "lpg_num %d / %d\n",
		       client->osdc.osdmap->lpg_num,
		       client->osdc.osdmap->lpg_num_mask);
	pos += sprintf(buf + pos, "flags%s%s\n",
		       (client->osdc.osdmap->flags & CEPH_OSDMAP_NEARFULL) ?
		       " NEARFULL":"",
		       (client->osdc.osdmap->flags & CEPH_OSDMAP_FULL) ?
		       " FULL":"");
	for (i = 0; i < client->osdc.osdmap->max_osd; i++) {
		struct ceph_entity_addr *addr =
			&client->osdc.osdmap->osd_addr[i];
		int state = client->osdc.osdmap->osd_state[i];
		char sb[64];

		if (pos > PAGE_SIZE - 128)
			break; /* be conservative */
		pos += sprintf(buf + pos,
		       "\tosd%d\t%u.%u.%u.%u:%u\t%3d%%\t(%s)\n",
		       i, IPQUADPORT(addr->ipaddr),
		       ((client->osdc.osdmap->osd_weight[i]*100) >> 16),
		       ceph_osdmap_state_str(sb, sizeof(sb), state));
	}
	return pos;
}

static ssize_t req_mon_want_osdmap_show(struct ceph_mon_client *monc,
			   struct ceph_mon_client_attr *attr, char *buf)
{
	return sprintf(buf, "%u\n", monc->want_osdmap);
}

static ssize_t req_mon_want_mdsmap_show(struct ceph_mon_client *monc,
			   struct ceph_mon_client_attr *attr, char *buf)
{
	return sprintf(buf, "%u\n", monc->want_mdsmap);
}

static struct kobj_type entity_ops = {
	.sysfs_ops = &ceph_client_sysfs_ops,
};


int ceph_sysfs_client_init(struct ceph_client *client)
{
	int ret = 0;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	ret = kobject_init_and_add(&client->kobj, &ceph_client_ops,
				   &ceph_kobj, "client%d", client->whoami);
	if (ret)
		goto out;

	ret = kobject_init_and_add(&client->mdsc.kobj, &entity_ops,
				   &client->kobj, "mdsc");
	if (ret)
		goto out;

	ret = kobject_init_and_add(&client->osdc.kobj, &entity_ops,
				   &client->kobj, "osdc");
	if (ret)
		goto out;

	ret = kobject_init_and_add(&client->monc.kobj, &entity_ops,
				   &client->kobj, "monc");
	if (ret)
		goto out;

	ADD_ENTITY_ATTR(client, k_fsid, "fsid", 0400, fsid_show, NULL);
	ADD_ENTITY_ATTR(client, k_monmap, "monmap", 0400, monmap_show, NULL);
	ADD_ENTITY_ATTR(client, k_mdsmap, "mdsmap", 0400, mdsmap_show, NULL);
	ADD_ENTITY_ATTR(client, k_osdmap, "osdmap", 0400, osdmap_show, NULL);
	ADD_ENTITY_ATTR((&client->monc), k_want_osdmap, "want_osdmap", 0400, req_mon_want_osdmap_show, NULL);
	ADD_ENTITY_ATTR((&client->monc), k_want_mdsmap, "want_mdsmap", 0400, req_mon_want_mdsmap_show, NULL);
	return 0;

out:
#endif
	return ret;
}

void ceph_sysfs_client_cleanup(struct ceph_client *client)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	kobject_del(&client->osdc.kobj);
	kobject_del(&client->mdsc.kobj);
	kobject_del(&client->kobj);
#endif	
}

static ssize_t req_mds_show(struct ceph_mds_request *req,
			   struct ceph_mds_request_attr *attr, char *buf)
{
	return sprintf(buf, "%u.%u.%u.%u:%u (%s%d)\n",
			IPQUADPORT(req->r_request->hdr.dst.addr.ipaddr),
			ENTITY_NAME(req->r_request->hdr.dst.name));
}

static ssize_t req_mds_op_show(struct ceph_mds_request *req,
			   struct ceph_mds_request_attr *attr, char *buf)
{
	int pos = 0, pathlen;
	u64 pathbase;

	char *path;

	pos += sprintf(buf, "%s", ceph_mds_op_name(req->r_op));

	if (req->r_dentry) {
		path = ceph_mdsc_build_path(req->r_dentry, &pathlen, &pathbase, -1);
		if (path) {
			pos += sprintf(buf+pos, " %s", path);
			kfree(path);
		}
	} else if (req->r_path1) {
		pos += sprintf(buf+pos, " %s", req->r_path1);
	}

	if (req->r_old_dentry) {
		path = ceph_mdsc_build_path(req->r_old_dentry, &pathlen, &pathbase, -1);
		if (path) {
			pos += sprintf(buf+pos, " %s", path);
			kfree(path);
		}
	} else if (req->r_path2 &&
		   req->r_op != CEPH_MDS_OP_FINDINODE) {
			pos += sprintf(buf+pos, " %s", req->r_path2);
	}

	pos += sprintf(buf+pos, "\n");

	return pos;
}

int ceph_sysfs_mds_req_init(struct ceph_mds_client *mdsc, struct ceph_mds_request *req)
{
	int ret = 0;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	ret = kobject_init_and_add(&req->kobj, &ceph_mds_request_ops,
				   &mdsc->kobj, "%d", req->r_tid);
	if (ret)
		goto out;

	ADD_ENTITY_ATTR(req, k_mds, "mds", 0400, req_mds_show, NULL);
	ADD_ENTITY_ATTR(req, k_op, "op", 0400, req_mds_op_show, NULL);

	return 0;
out:
#endif
	return ret;
}

void ceph_sysfs_mds_req_cleanup(struct ceph_mds_request *req)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	kobject_del(&req->kobj);
#endif
}

static ssize_t req_osd_show(struct ceph_osd_request *req,
			   struct ceph_osd_request_attr *attr, char *buf)
{
	return sprintf(buf, "%u.%u.%u.%u:%u (%s%d)\n",
			IPQUADPORT(req->r_request->hdr.dst.addr.ipaddr),
			ENTITY_NAME(req->r_request->hdr.dst.name));
}

static ssize_t req_osd_op_show(struct ceph_osd_request *req,
			   struct ceph_osd_request_attr *attr, char *buf)
{
	struct ceph_osd_request_head *head = req->r_request->front.iov_base;
	struct ceph_osd_op *op;
	int num_ops;
	int pos = 0;
	int opcode;
	int i;

	op = (void *)(head + 1);

	pos += sprintf(buf, "oid=%llx.%08x (snap=%lld)\n",
	     le64_to_cpu(head->oid.ino),
	     le32_to_cpu(head->oid.bno),
	     le64_to_cpu(head->oid.snap));

	num_ops = le16_to_cpu(head->num_ops);

	for (i=0; i<num_ops; i++) {
		opcode = le16_to_cpu(op->op);

		pos += sprintf(buf + pos, "%s\n", ceph_osd_op_name(opcode));
		op++;
	}

	return pos;
}

int ceph_sysfs_osd_req_init(struct ceph_osd_client *osdc, struct ceph_osd_request *req)
{
	int ret = 0;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	ret = kobject_init_and_add(&req->kobj, &ceph_osd_request_ops,
				   &osdc->kobj, "%d", req->r_tid);
	if (ret)
		goto out;

	ADD_ENTITY_ATTR(req, k_osd, "osd", 0400, req_osd_show, NULL);
	ADD_ENTITY_ATTR(req, k_op, "op", 0400, req_osd_op_show, NULL);

	return 0;
out:
#endif
	return ret;
}

void ceph_sysfs_osd_req_cleanup(struct ceph_osd_request *req)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	kobject_del(&req->kobj);
#endif
}

static ssize_t req_mon_show(struct ceph_mon_statfs_request *req,
			   struct ceph_mon_statfs_request_attr *attr, char *buf)
{
	return sprintf(buf, "%u.%u.%u.%u:%u (%s%d)\n",
			IPQUADPORT(attr->dst.addr.ipaddr),
			ENTITY_NAME(attr->dst.name));
}

static ssize_t req_mon_op_show(struct ceph_mon_statfs_request *req,
			   struct ceph_mon_statfs_request_attr *attr, char *buf)
{
	return sprintf(buf, "statfs\n");
}

int ceph_sysfs_mon_statfs_req_init(struct ceph_mon_client *monc, struct ceph_mon_statfs_request *req)
{
	int ret = 0;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	ret = kobject_init_and_add(&req->kobj, &ceph_mon_statfs_request_ops,
				   &monc->kobj, "%d", req->tid);
#endif
	memset(&req->k_mon.dst, 0, sizeof(req->k_mon.dst));
	ADD_ENTITY_ATTR(req, k_mon, "mon", 0400, req_mon_show, NULL);
	ADD_ENTITY_ATTR(req, k_op, "op", 0400, req_mon_op_show, NULL);

	return ret;
}

int ceph_sysfs_mon_statfs_req_set(struct ceph_mon_client *monc, struct ceph_mon_statfs_request *req,
				   struct ceph_msg *msg)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	req->k_mon.dst = msg->hdr.dst;
#endif
	return 0;
}

void ceph_sysfs_mon_statfs_req_cleanup(struct ceph_mon_statfs_request *req)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	kobject_del(&req->kobj);
#endif
}

/*
 * ceph attrs
 */
struct ceph_attr {
	struct attribute attr;
	ssize_t (*show)(struct ceph_attr *, char *);
	ssize_t (*store)(struct ceph_attr *, const char *, size_t);
	int *val;
};

static ssize_t ceph_attr_show(struct kobject *ko, struct attribute *a,
			      char *buf)
{
	struct ceph_attr *ca = container_of(a, struct ceph_attr, attr);
	return ca->show(ca, buf);
}

static ssize_t ceph_attr_store(struct kobject *ko, struct attribute *a,
			       const char *buf, size_t len)
{
	struct ceph_attr *ca = container_of(a, struct ceph_attr, attr);
	return ca->store(ca, buf, len);
}

static struct sysfs_ops ceph_sysfs_ops = {
	.show = ceph_attr_show,
	.store = ceph_attr_store,
};

static struct kobj_type ceph_type = {
	.sysfs_ops = &ceph_sysfs_ops,
};

int ceph_sysfs_init(void)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	int ret;

	ret = kobject_init_and_add(&ceph_kobj, &ceph_type, fs_kobj, "ceph");

	return ret;
#else
	return 0;
#endif
}

void ceph_sysfs_cleanup(void)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 25)
	kobject_del(&ceph_kobj);
#endif
}
