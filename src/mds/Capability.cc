// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "Capability.h"

#include "common/Formatter.h"


/*
 * Capability::Export
 */

void Capability::Export::encode(bufferlist &bl) const
{
  __u8 struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(wanted, bl);
  ::encode(issued, bl);
  ::encode(pending, bl);
  ::encode(client_follows, bl);
  ::encode(mseq, bl);
  ::encode(last_issue_stamp, bl);
}

void Capability::Export::decode(bufferlist::iterator &p)
{
  __u8 struct_v;
  ::decode(struct_v, p);
  ::decode(wanted, p);
  ::decode(issued, p);
  ::decode(pending, p);
  ::decode(client_follows, p);
  ::decode(mseq, p);
  ::decode(last_issue_stamp, p);
}


/*
 * Capability::revoke_info
 */

void Capability::revoke_info::encode(bufferlist& bl) const
{
  __u8 struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(before, bl);
  ::encode(seq, bl);
  ::encode(last_issue, bl);
}

void Capability::revoke_info::decode(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  ::decode(before, bl);
  ::decode(seq, bl);
  ::decode(last_issue, bl);
}


/*
 * Capability
 */

void Capability::encode(bufferlist& bl) const
{
  __u8 struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(last_sent, bl);
  ::encode(last_issue_stamp, bl);

  ::encode(_wanted, bl);
  ::encode(_pending, bl);
  ::encode(_revokes, bl);
}

void Capability::decode(bufferlist::iterator &bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  ::decode(last_sent, bl);
  ::decode(last_issue_stamp, bl);

  ::decode(_wanted, bl);
  ::decode(_pending, bl);
  ::decode(_revokes, bl);
  
  _calc_issued();
}
