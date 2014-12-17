// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "messages/MClientCapRelease.h"

#include "MetaSession.h"

#include "common/Formatter.h"

const char *MetaSession::get_state_name() const
{
  switch (state) {
  case STATE_NEW: return "new";
  case STATE_OPENING: return "opening";
  case STATE_OPEN: return "open";
  case STATE_CLOSING: return "closing";
  case STATE_CLOSED: return "closed";
  case STATE_STALE: return "stale";
  default: return "unknown";
  }
}

void MetaSession::dump(Formatter *f) const
{
  f->dump_int("mds", mds_num);
  f->dump_stream("addr") << inst.addr;
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("cap_gen", cap_gen);
  f->dump_stream("cap_ttl") << cap_ttl;
  f->dump_stream("last_cap_renew_request") << last_cap_renew_request;
  f->dump_unsigned("cap_renew_seq", cap_renew_seq);
  f->dump_int("num_caps", num_caps);
  f->dump_string("state", get_state_name());
}

MetaSession::~MetaSession()
{
  if (release)
    release->put();
}

void MetaSession::enqueue_cap_release(inodeno_t ino, uint64_t cap_id, ceph_seq_t iseq,
    ceph_seq_t mseq, epoch_t osd_barrier)
{
  if (!release) {
    release = new MClientCapRelease;
  }

  if (osd_barrier > release->osd_epoch_barrier) {
    release->osd_epoch_barrier = osd_barrier;
  }

  ceph_mds_cap_item i;
  i.ino = ino;
  i.cap_id = cap_id;
  i.seq = iseq;
  i.migrate_seq = mseq;
  release->caps.push_back(i);
}
