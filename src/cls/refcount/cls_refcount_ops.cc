// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls_refcount_ops.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

void cls_refcount_get_op::dump(ceph::Formatter *f) const
{
  f->dump_string("tag", tag);
  f->dump_int("implicit_ref", (int)implicit_ref);
}

void cls_refcount_get_op::generate_test_instances(list<cls_refcount_get_op*>& ls)
{
  ls.push_back(new cls_refcount_get_op);
  ls.push_back(new cls_refcount_get_op);
  ls.back()->tag = "foo";
  ls.back()->implicit_ref = true;
}


void cls_refcount_put_op::dump(ceph::Formatter *f) const
{
  f->dump_string("tag", tag);
  f->dump_int("implicit_ref", (int)implicit_ref);
}

void cls_refcount_put_op::generate_test_instances(list<cls_refcount_put_op*>& ls)
{
  ls.push_back(new cls_refcount_put_op);
  ls.push_back(new cls_refcount_put_op);
  ls.back()->tag = "foo";
  ls.back()->implicit_ref = true;
}



void cls_refcount_set_op::dump(ceph::Formatter *f) const
{
  encode_json("refs", refs, f);
}

void cls_refcount_set_op::generate_test_instances(list<cls_refcount_set_op*>& ls)
{
  ls.push_back(new cls_refcount_set_op);
  ls.push_back(new cls_refcount_set_op);
  ls.back()->refs.push_back("foo");
  ls.back()->refs.push_back("bar");
}


void cls_refcount_read_op::dump(ceph::Formatter *f) const
{
  f->dump_int("implicit_ref", (int)implicit_ref);
}

void cls_refcount_read_op::generate_test_instances(list<cls_refcount_read_op*>& ls)
{
  ls.push_back(new cls_refcount_read_op);
  ls.push_back(new cls_refcount_read_op);
  ls.back()->implicit_ref = true;
}


void cls_refcount_read_ret::dump(ceph::Formatter *f) const
{
  f->open_array_section("refs");
  for (list<string>::const_iterator p = refs.begin(); p != refs.end(); ++p)
    f->dump_string("ref", *p);
  f->close_section();
}

void cls_refcount_read_ret::generate_test_instances(list<cls_refcount_read_ret*>& ls)
{
  ls.push_back(new cls_refcount_read_ret);
  ls.push_back(new cls_refcount_read_ret);
  ls.back()->refs.push_back("foo");
  ls.back()->refs.push_back("bar");
}
