// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "common/ceph_context.h"
#include "include/types.h"
#include "rgw/rgw_acl.h"

#include <iostream>
#include <map>

#define dout_subsys ceph_subsys_rgw

int main(int argc, char **argv) {
  RGWACLXMLParser parser;

  if (!parser.init())
    exit(1);

  char buf[1024];

  for (;;) {
    int done;
    int len;

    len = fread(buf, 1, sizeof(buf), stdin);
    if (ferror(stdin)) {
      fprintf(stderr, "Read error\n");
      exit(-1);
    }
    done = feof(stdin);

    parser.parse(buf, len, done);

    if (done)
      break;
  }

  RGWAccessControlPolicy *policy = (RGWAccessControlPolicy *)parser.find_first("AccessControlPolicy");

  if (policy) {
    string id="79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be";
    dout(10) << hex << policy->get_perm(g_ceph_context, id, RGW_PERM_ALL) << dec << dendl;
    policy->to_xml(cout);
  }

  cout << parser.get_xml() << endl;

  bufferlist bl;
  policy->encode(bl);

  RGWAccessControlPolicy newpol;
  bufferlist::iterator iter = bl.begin();
  newpol.decode(iter);

  newpol.to_xml(cout);

  exit(0);
}

