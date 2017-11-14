// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "common/Clock.h"
#include "common/Timer.h"

#include "Client.h"
#include "Inode.h"
#include "Fh.h"
#include "Delegation.h"

class C_Deleg_Timeout : public Context {
  Delegation *deleg;
public:
  explicit C_Deleg_Timeout(Delegation *d) : deleg(d) {}
  void finish(int r) override {
    Inode *in = deleg->get_fh()->inode.get();
    Client *client = in->client;

    // Called back via Timer, which takes client_lock for us
    assert(client->client_lock.is_locked_by_me());

    lsubdout(client->cct, client, 0) << __func__ <<
	  ": delegation return timeout for inode 0x" <<
	  std::hex << in->ino << ". Forcibly unmounting client. "<<
	  client << std::dec << dendl;
    client->_unmount();
  }
};

/**
 * ceph_deleg_caps_for_type - what caps are necessary for a delegation?
 * @type: delegation request type
 *
 * Determine what caps are necessary in order to grant a delegation of a given
 * type. For read delegations, we need whatever we require in order to do
 * cached reads, plus AsLs to cover metadata changes that should trigger a
 * recall. We also grab Xs since changing xattrs usually alters the mtime and
 * so would trigger a recall.
 *
 * For write delegations, we need whatever read delegations need plus the
 * caps to allow writing to the file (Fbwx).
 */
int ceph_deleg_caps_for_type(unsigned type)
{
	int caps = CEPH_CAP_PIN;

	switch (type) {
	case CEPH_DELEGATION_WR:
		caps |= CEPH_CAP_FILE_EXCL |
			CEPH_CAP_FILE_WR | CEPH_CAP_FILE_BUFFER;
		/* Fallthrough */
	case CEPH_DELEGATION_RD:
		caps |= CEPH_CAP_FILE_SHARED |
			CEPH_CAP_FILE_RD | CEPH_CAP_FILE_CACHE |
			CEPH_CAP_XATTR_SHARED |
			CEPH_CAP_LINK_SHARED | CEPH_CAP_AUTH_SHARED;
		break;
	default:
		// Should never happen
		assert(false);
	}
	return caps;
}

/*
 * A delegation is a container for holding caps on behalf of a client that
 * wants to be able to rely on them until recalled.
 */
Delegation::Delegation(Fh *_fh, unsigned _type, ceph_deleg_cb_t _cb, void *_priv)
	: fh(_fh), priv(_priv), type(_type), recall_cb(_cb),
	  recall_time(utime_t()), timeout_event(nullptr)
{
  Inode *inode = _fh->inode.get();
  inode->client->get_cap_ref(inode, ceph_deleg_caps_for_type(_type));
};

Delegation::~Delegation()
{
  disarm_timeout();
  Inode *inode = fh->inode.get();
  inode->client->put_cap_ref(inode, ceph_deleg_caps_for_type(type));
}

void Delegation::reinit(unsigned _type, ceph_deleg_cb_t _recall_cb, void *_priv)
{
  /* update cap refs -- note that we do a get first to avoid any going to 0 */
  if (type != _type) {
    Inode *inode = fh->inode.get();

    inode->client->get_cap_ref(inode, ceph_deleg_caps_for_type(_type));
    inode->client->put_cap_ref(inode, ceph_deleg_caps_for_type(type));
    type = _type;
  }

  recall_cb = _recall_cb;
  priv = _priv;
}

void Delegation::arm_timeout()
{
  Client *client = fh->inode.get()->client;

  if (timeout_event)
    return;

  timeout_event = new C_Deleg_Timeout(this);
  client->timer.add_event_after(client->get_deleg_timeout(), timeout_event);
}

void Delegation::disarm_timeout()
{
  Client *client = fh->inode.get()->client;

  if (!timeout_event)
    return;

  client->timer.cancel_event(timeout_event);
  timeout_event = nullptr;
}

void Delegation::recall(bool skip_read)
{
  /* If skip_read is true, don't break read delegations */
  if (skip_read && type == CEPH_DELEGATION_RD)
    return;

  if (!is_recalled()) {
    recall_cb(fh, priv);
    recall_time = ceph_clock_now();
    arm_timeout();
  }
}
