/*
 * Ceph string constants
 */
#include "ceph_strings.h"
#include "include/types.h"
#include "include/ceph_features.h"

const char *ceph_entity_type_name(int type)
{
	switch (type) {
	case CEPH_ENTITY_TYPE_MDS: return "mds";
	case CEPH_ENTITY_TYPE_OSD: return "osd";
	case CEPH_ENTITY_TYPE_MON: return "mon";
	case CEPH_ENTITY_TYPE_MGR: return "mgr";
	case CEPH_ENTITY_TYPE_CLIENT: return "client";
	case CEPH_ENTITY_TYPE_AUTH: return "auth";
	default: return "unknown";
	}
}

const char *ceph_con_mode_name(int con_mode)
{
	switch (con_mode) {
	case CEPH_CON_MODE_UNKNOWN: return "unknown";
	case CEPH_CON_MODE_CRC: return "crc";
	case CEPH_CON_MODE_SECURE: return "secure";
	default: return "???";
	}
}

const char *ceph_osd_op_name(int op)
{
	switch (op) {
#define GENERATE_CASE(op, opcode, str)	case CEPH_OSD_OP_##op: return (str);
__CEPH_FORALL_OSD_OPS(GENERATE_CASE)
#undef GENERATE_CASE
	default:
		return "???";
	}
}

const char *ceph_osd_state_name(int s)
{
	switch (s) {
	case CEPH_OSD_EXISTS:
		return "exists";
	case CEPH_OSD_UP:
		return "up";
	case CEPH_OSD_AUTOOUT:
		return "autoout";
	case CEPH_OSD_NEW:
		return "new";
	case CEPH_OSD_FULL:
		return "full";
	case CEPH_OSD_NEARFULL:
		return "nearfull";
	case CEPH_OSD_BACKFILLFULL:
		return "backfillfull";
        case CEPH_OSD_DESTROYED:
                return "destroyed";
        case CEPH_OSD_NOUP:
                return "noup";
        case CEPH_OSD_NODOWN:
                return "nodown";
        case CEPH_OSD_NOIN:
                return "noin";
        case CEPH_OSD_NOOUT:
                return "noout";
        case CEPH_OSD_STOP:
                return "stop";
	default:
		return "???";
	}
}

const char *ceph_release_name(int r)
{
	switch (r) {
	case CEPH_RELEASE_ARGONAUT:
		return "argonaut";
	case CEPH_RELEASE_BOBTAIL:
		return "bobtail";
	case CEPH_RELEASE_CUTTLEFISH:
		return "cuttlefish";
	case CEPH_RELEASE_DUMPLING:
		return "dumpling";
	case CEPH_RELEASE_EMPEROR:
		return "emperor";
	case CEPH_RELEASE_FIREFLY:
		return "firefly";
	case CEPH_RELEASE_GIANT:
		return "giant";
	case CEPH_RELEASE_HAMMER:
		return "hammer";
	case CEPH_RELEASE_INFERNALIS:
		return "infernalis";
	case CEPH_RELEASE_JEWEL:
		return "jewel";
	case CEPH_RELEASE_KRAKEN:
		return "kraken";
	case CEPH_RELEASE_LUMINOUS:
		return "luminous";
	case CEPH_RELEASE_MIMIC:
		return "mimic";
	case CEPH_RELEASE_NAUTILUS:
		return "nautilus";
	case CEPH_RELEASE_OCTOPUS:
		return "octopus";
	case CEPH_RELEASE_PACIFIC:
		return "pacific";
	case CEPH_RELEASE_QUINCY:
		return "quincy";
	case CEPH_RELEASE_REEF:
		return "reef";
	case CEPH_RELEASE_SQUID:
		return "squid";
	default:
		if (r < 0)
			return "unspecified";
		return "unknown";
	}
}

uint64_t ceph_release_features(int r)
{
	uint64_t req = 0;

	req |= CEPH_FEATURE_CRUSH_TUNABLES;
	if (r <= CEPH_RELEASE_CUTTLEFISH)
		return req;

	req |= CEPH_FEATURE_CRUSH_TUNABLES2 |
		CEPH_FEATURE_OSDHASHPSPOOL;
	if (r <= CEPH_RELEASE_EMPEROR)
		return req;

	req |= CEPH_FEATURE_CRUSH_TUNABLES3 |
		CEPH_FEATURE_OSD_PRIMARY_AFFINITY |
		CEPH_FEATURE_OSD_CACHEPOOL;
	if (r <= CEPH_RELEASE_GIANT)
		return req;

	req |= CEPH_FEATURE_CRUSH_V4;
	if (r <= CEPH_RELEASE_INFERNALIS)
		return req;

	req |= CEPH_FEATURE_CRUSH_TUNABLES5;
	if (r <= CEPH_RELEASE_JEWEL)
		return req;

	req |= CEPH_FEATURE_MSG_ADDR2;
	if (r <= CEPH_RELEASE_KRAKEN)
		return req;

	req |= CEPH_FEATUREMASK_CRUSH_CHOOSE_ARGS; // and overlaps
	if (r <= CEPH_RELEASE_LUMINOUS)
		return req;

	return req;
}

/* return oldest/first release that supports these features */
int ceph_release_from_features(uint64_t features)
{
	int r = 1;
	while (true) {
		uint64_t need = ceph_release_features(r);
		if ((need & features) != need ||
		    r == CEPH_RELEASE_MAX) {
			r--;
			need = ceph_release_features(r);
			/* we want the first release that looks like this */
			while (r > 1 && ceph_release_features(r - 1) == need) {
				r--;
			}
			break;
		}
		++r;
	}
	return r;
}

const char *ceph_osd_watch_op_name(int o)
{
	switch (o) {
	case CEPH_OSD_WATCH_OP_UNWATCH:
		return "unwatch";
	case CEPH_OSD_WATCH_OP_WATCH:
		return "watch";
	case CEPH_OSD_WATCH_OP_RECONNECT:
		return "reconnect";
	case CEPH_OSD_WATCH_OP_PING:
		return "ping";
	default:
		return "???";
	}
}

const char *ceph_osd_alloc_hint_flag_name(int f)
{
	switch (f) {
	case CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE:
		return "sequential_write";
	case CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_WRITE:
		return "random_write";
	case CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_READ:
		return "sequential_read";
	case CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_READ:
		return "random_read";
	case CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY:
		return "append_only";
	case CEPH_OSD_ALLOC_HINT_FLAG_IMMUTABLE:
		return "immutable";
	case CEPH_OSD_ALLOC_HINT_FLAG_SHORTLIVED:
		return "shortlived";
	case CEPH_OSD_ALLOC_HINT_FLAG_LONGLIVED:
		return "longlived";
	case CEPH_OSD_ALLOC_HINT_FLAG_COMPRESSIBLE:
		return "compressible";
	case CEPH_OSD_ALLOC_HINT_FLAG_INCOMPRESSIBLE:
		return "incompressible";
	default:
		return "???";
	}
}

const char *ceph_mds_state_name(int s)
{
	switch (s) {
		/* down and out */
	case CEPH_MDS_STATE_DNE:        return "down:dne";
	case CEPH_MDS_STATE_STOPPED:    return "down:stopped";
	case CEPH_MDS_STATE_DAMAGED:   return "down:damaged";
		/* up and out */
	case CEPH_MDS_STATE_BOOT:       return "up:boot";
	case CEPH_MDS_STATE_STANDBY:    return "up:standby";
	case CEPH_MDS_STATE_STANDBY_REPLAY:    return "up:standby-replay";
	case CEPH_MDS_STATE_REPLAYONCE: return "up:oneshot-replay";
	case CEPH_MDS_STATE_CREATING:   return "up:creating";
	case CEPH_MDS_STATE_STARTING:   return "up:starting";
		/* up and in */
	case CEPH_MDS_STATE_REPLAY:     return "up:replay";
	case CEPH_MDS_STATE_RESOLVE:    return "up:resolve";
	case CEPH_MDS_STATE_RECONNECT:  return "up:reconnect";
	case CEPH_MDS_STATE_REJOIN:     return "up:rejoin";
	case CEPH_MDS_STATE_CLIENTREPLAY: return "up:clientreplay";
	case CEPH_MDS_STATE_ACTIVE:     return "up:active";
	case CEPH_MDS_STATE_STOPPING:   return "up:stopping";
               /* misc */
	case CEPH_MDS_STATE_NULL:       return "null";
	}
	return "???";
}

const char *ceph_session_op_name(int op)
{
	switch (op) {
	case CEPH_SESSION_REQUEST_OPEN: return "request_open";
	case CEPH_SESSION_OPEN: return "open";
	case CEPH_SESSION_REQUEST_CLOSE: return "request_close";
	case CEPH_SESSION_CLOSE: return "close";
	case CEPH_SESSION_REQUEST_RENEWCAPS: return "request_renewcaps";
	case CEPH_SESSION_RENEWCAPS: return "renewcaps";
	case CEPH_SESSION_STALE: return "stale";
	case CEPH_SESSION_RECALL_STATE: return "recall_state";
	case CEPH_SESSION_FLUSHMSG: return "flushmsg";
	case CEPH_SESSION_FLUSHMSG_ACK: return "flushmsg_ack";
	case CEPH_SESSION_FORCE_RO: return "force_ro";
	case CEPH_SESSION_REJECT: return "reject";
	case CEPH_SESSION_REQUEST_FLUSH_MDLOG: return "request_flushmdlog";
	}
	return "???";
}

const char *ceph_mds_op_name(int op)
{
	switch (op) {
	case CEPH_MDS_OP_LOOKUP:  return "lookup";
	case CEPH_MDS_OP_LOOKUPHASH:  return "lookuphash";
	case CEPH_MDS_OP_LOOKUPPARENT:  return "lookupparent";
	case CEPH_MDS_OP_LOOKUPINO:  return "lookupino";
	case CEPH_MDS_OP_LOOKUPNAME:  return "lookupname";
	case CEPH_MDS_OP_GETATTR:  return "getattr";
	case CEPH_MDS_OP_DUMMY:  return "dummy";
	case CEPH_MDS_OP_SETXATTR: return "setxattr";
	case CEPH_MDS_OP_SETATTR: return "setattr";
	case CEPH_MDS_OP_RMXATTR: return "rmxattr";
	case CEPH_MDS_OP_SETLAYOUT: return "setlayou";
	case CEPH_MDS_OP_SETDIRLAYOUT: return "setdirlayout";
	case CEPH_MDS_OP_READDIR: return "readdir";
	case CEPH_MDS_OP_MKNOD: return "mknod";
	case CEPH_MDS_OP_LINK: return "link";
	case CEPH_MDS_OP_UNLINK: return "unlink";
	case CEPH_MDS_OP_RENAME: return "rename";
	case CEPH_MDS_OP_MKDIR: return "mkdir";
	case CEPH_MDS_OP_RMDIR: return "rmdir";
	case CEPH_MDS_OP_SYMLINK: return "symlink";
	case CEPH_MDS_OP_CREATE: return "create";
	case CEPH_MDS_OP_OPEN: return "open";
	case CEPH_MDS_OP_LOOKUPSNAP: return "lookupsnap";
	case CEPH_MDS_OP_LSSNAP: return "lssnap";
	case CEPH_MDS_OP_MKSNAP: return "mksnap";
	case CEPH_MDS_OP_RMSNAP: return "rmsnap";
	case CEPH_MDS_OP_RENAMESNAP: return "renamesnap";
	case CEPH_MDS_OP_READDIR_SNAPDIFF: return "readdir_snapdiff";
	case CEPH_MDS_OP_SETFILELOCK: return "setfilelock";
	case CEPH_MDS_OP_GETFILELOCK: return "getfilelock";
	case CEPH_MDS_OP_FRAGMENTDIR: return "fragmentdir";
	case CEPH_MDS_OP_EXPORTDIR: return "exportdir";
	case CEPH_MDS_OP_FLUSH: return "flush_path";
	case CEPH_MDS_OP_ENQUEUE_SCRUB: return "enqueue_scrub";
	case CEPH_MDS_OP_REPAIR_FRAGSTATS: return "repair_fragstats";
	case CEPH_MDS_OP_REPAIR_INODESTATS: return "repair_inodestats";
	case CEPH_MDS_OP_QUIESCE_SUBVOLUME: return "quiesce_subvolume";
	case CEPH_MDS_OP_QUIESCE_SUBVOLUME_INODE: return "quiesce_subvolume_inode";
	}
	return "???";
}

const char *ceph_cap_op_name(int op)
{
	switch (op) {
	case CEPH_CAP_OP_GRANT: return "grant";
	case CEPH_CAP_OP_REVOKE: return "revoke";
	case CEPH_CAP_OP_TRUNC: return "trunc";
	case CEPH_CAP_OP_EXPORT: return "export";
	case CEPH_CAP_OP_IMPORT: return "import";
	case CEPH_CAP_OP_UPDATE: return "update";
	case CEPH_CAP_OP_DROP: return "drop";
	case CEPH_CAP_OP_FLUSH: return "flush";
	case CEPH_CAP_OP_FLUSH_ACK: return "flush_ack";
	case CEPH_CAP_OP_FLUSHSNAP: return "flushsnap";
	case CEPH_CAP_OP_FLUSHSNAP_ACK: return "flushsnap_ack";
	case CEPH_CAP_OP_RELEASE: return "release";
	case CEPH_CAP_OP_RENEW: return "renew";
	}
	return "???";
}

const char *ceph_lease_op_name(int o)
{
	switch (o) {
	case CEPH_MDS_LEASE_REVOKE: return "revoke";
	case CEPH_MDS_LEASE_RELEASE: return "release";
	case CEPH_MDS_LEASE_RENEW: return "renew";
	case CEPH_MDS_LEASE_REVOKE_ACK: return "revoke_ack";
	}
	return "???";
}

const char *ceph_snap_op_name(int o)
{
	switch (o) {
	case CEPH_SNAP_OP_UPDATE: return "update";
	case CEPH_SNAP_OP_CREATE: return "create";
	case CEPH_SNAP_OP_DESTROY: return "destroy";
	case CEPH_SNAP_OP_SPLIT: return "split";
	}
	return "???";
}

const char *ceph_watch_event_name(int e)
{
	switch (e) {
	case CEPH_WATCH_EVENT_NOTIFY: return "notify";
	case CEPH_WATCH_EVENT_NOTIFY_COMPLETE: return "notify_complete";
	case CEPH_WATCH_EVENT_DISCONNECT: return "disconnect";
	}
	return "???";
}

const char *ceph_pool_op_name(int op)
{
	switch (op) {
	case POOL_OP_CREATE: return "create";
	case POOL_OP_DELETE: return "delete";
	case POOL_OP_AUID_CHANGE: return "auid change";  // (obsolete)
	case POOL_OP_CREATE_SNAP: return "create snap";
	case POOL_OP_DELETE_SNAP: return "delete snap";
	case POOL_OP_CREATE_UNMANAGED_SNAP: return "create unmanaged snap";
	case POOL_OP_DELETE_UNMANAGED_SNAP: return "delete unmanaged snap";
	}
	return "???";
}

const char *ceph_osd_backoff_op_name(int op)
{
	switch (op) {
	case CEPH_OSD_BACKOFF_OP_BLOCK: return "block";
	case CEPH_OSD_BACKOFF_OP_ACK_BLOCK: return "ack-block";
	case CEPH_OSD_BACKOFF_OP_UNBLOCK: return "unblock";
	}
	return "???";
}
