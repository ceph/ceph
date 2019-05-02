// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "cls/queue/cls_queue_ops.h"

#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "include/utime.h"

void cls_create_queue_op::dump(Formatter *f) const
{
  head.dump(f);
}

void cls_create_queue_op::generate_test_instances(list<cls_create_queue_op*>& ls)
{
  ls.push_back(new cls_create_queue_op);
  ls.push_back(new cls_create_queue_op);
}