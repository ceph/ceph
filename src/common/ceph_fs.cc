/*
 * ceph_fs.cc - Some Ceph functions that are shared between kernel space and
 * user space.
 *
 */

/*
 * Some non-inline ceph helpers
 */

#include "include/ceph_fs.h"
#include "include/encoding.h"

void encode(const struct ceph_mds_request_head& h, ceph::buffer::list& bl) {
  using ceph::encode;
  encode(h.version, bl);
  encode(h.oldest_client_tid, bl);
  encode(h.mdsmap_epoch, bl);
  encode(h.flags, bl);

  // For old MDS daemons
  __u8 num_retry = __u32(h.ext_num_retry);
  __u8 num_fwd = __u32(h.ext_num_fwd);
  encode(num_retry, bl);
  encode(num_fwd, bl);

  encode(h.num_releases, bl);
  encode(h.op, bl);
  encode(h.caller_uid, bl);
  encode(h.caller_gid, bl);
  encode(h.ino, bl);
  bl.append((char*)&h.args, sizeof(h.args));

  if (h.version >= 2) {
    encode(h.ext_num_retry, bl);
    encode(h.ext_num_fwd, bl);
  }

  if (h.version >= 3) {
    __u32 struct_len = sizeof(struct ceph_mds_request_head);
    encode(struct_len, bl);
    encode(h.owner_uid, bl);
    encode(h.owner_gid, bl);

    /*
     * Please, add new fields handling here.
     * You don't need to check h.version as we do it
     * in decode(), because decode can properly skip
     * all unsupported fields if h.version >= 3.
     */
  }
}

void decode(struct ceph_mds_request_head& h, ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  unsigned struct_end = bl.get_off();

  decode(h.version, bl);
  decode(h.oldest_client_tid, bl);
  decode(h.mdsmap_epoch, bl);
  decode(h.flags, bl);
  decode(h.num_retry, bl);
  decode(h.num_fwd, bl);
  decode(h.num_releases, bl);
  decode(h.op, bl);
  decode(h.caller_uid, bl);
  decode(h.caller_gid, bl);
  decode(h.ino, bl);
  bl.copy(sizeof(h.args), (char*)&(h.args));

  if (h.version >= 2) {
    decode(h.ext_num_retry, bl);
    decode(h.ext_num_fwd, bl);
  } else {
    h.ext_num_retry = h.num_retry;
    h.ext_num_fwd = h.num_fwd;
  }

  if (h.version >= 3) {
    decode(h.struct_len, bl);
    struct_end += h.struct_len;

    decode(h.owner_uid, bl);
    decode(h.owner_gid, bl);
  } else {
    /*
     * client is old: let's take caller_{u,g}id as owner_{u,g}id
     * this is how it worked before adding of owner_{u,g}id fields.
     */
    h.owner_uid = h.caller_uid;
    h.owner_gid = h.caller_gid;
  }

  /* add new fields handling here */

  /*
   * From version 3 we have struct_len field.
   * It allows us to properly handle a case
   * when client send struct ceph_mds_request_head
   * bigger in size than MDS supports. In this
   * case we just want to skip all remaining bytes
   * at the end.
   *
   * See also DECODE_FINISH macro. Unfortunately,
   * we can't start using it right now as it will be
   * an incompatible protocol change.
   */
  if (h.version >= 3) {
    if (bl.get_off() > struct_end)
      throw ::ceph::buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__));
    if (bl.get_off() < struct_end)
      bl += struct_end - bl.get_off();
  }
}

int ceph_flags_to_mode(int flags)
{
	/* because CEPH_FILE_MODE_PIN is zero, so mode = -1 is error */
	int mode = -1;

	if ((flags & CEPH_O_DIRECTORY) == CEPH_O_DIRECTORY)
		return CEPH_FILE_MODE_PIN;

	switch (flags & O_ACCMODE) {
	case CEPH_O_WRONLY:
		mode = CEPH_FILE_MODE_WR;
		break;
	case CEPH_O_RDONLY:
		mode = CEPH_FILE_MODE_RD;
		break;
	case CEPH_O_RDWR:
	case O_ACCMODE: /* this is what the VFS does */
		mode = CEPH_FILE_MODE_RDWR;
		break;
	}

	if (flags & CEPH_O_LAZY)
		mode |= CEPH_FILE_MODE_LAZY;

	return mode;
}

int ceph_caps_for_mode(int mode)
{
	int caps = CEPH_CAP_PIN;

	if (mode & CEPH_FILE_MODE_RD)
		caps |= CEPH_CAP_FILE_SHARED |
			CEPH_CAP_FILE_RD | CEPH_CAP_FILE_CACHE;
	if (mode & CEPH_FILE_MODE_WR)
		caps |= CEPH_CAP_FILE_EXCL |
			CEPH_CAP_FILE_WR | CEPH_CAP_FILE_BUFFER |
			CEPH_CAP_AUTH_SHARED | CEPH_CAP_AUTH_EXCL |
			CEPH_CAP_XATTR_SHARED | CEPH_CAP_XATTR_EXCL;
	if (mode & CEPH_FILE_MODE_LAZY)
		caps |= CEPH_CAP_FILE_LAZYIO;

	return caps;
}

int ceph_flags_sys2wire(int flags)
{
       int wire_flags = 0;

       switch (flags & O_ACCMODE) {
       case O_RDONLY:
               wire_flags |= CEPH_O_RDONLY;
               break;
       case O_WRONLY:
               wire_flags |= CEPH_O_WRONLY;
               break;
       case O_RDWR:
               wire_flags |= CEPH_O_RDWR;
               break;
       }
       flags &= ~O_ACCMODE;

#define ceph_sys2wire(a) if (flags & a) { wire_flags |= CEPH_##a; flags &= ~a; }

       ceph_sys2wire(O_CREAT);
       ceph_sys2wire(O_EXCL);
       ceph_sys2wire(O_TRUNC);

       ceph_sys2wire(O_DIRECTORY);
       ceph_sys2wire(O_NOFOLLOW);

#undef ceph_sys2wire

       return wire_flags;
}
