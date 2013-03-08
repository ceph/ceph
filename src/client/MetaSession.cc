// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include "MetaSession.h"

#include "common/Formatter.h"

const char *MetaSession::get_state_name() const
{
  switch (state) {
  case STATE_NEW: return "new";
  case STATE_OPENING: return "opening";
  case STATE_OPEN: return "open";
  case STATE_CLOSING: return "closing";
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
  f->dump_int("was_stale", (int)was_stale);
}

MetaSession::~MetaSession()
{
  if (release)
    release->put();
}
