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

void obj_refcount::dump(ceph::Formatter *f) const
{
  f->open_array_section("refs");
  for (const auto &kv: refs) {
    f->open_object_section("ref");
    f->dump_string("oid", kv.first.c_str());
    f->dump_bool("active",kv.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("retired_refs");
  for (const auto& it: retired_refs)
    f->dump_string("ref", it.c_str());
  f->close_section();
}

void obj_refcount::generate_test_instances(list<obj_refcount*>& ls)
{
  ls.push_back(new obj_refcount);
  ls.back()->refs.emplace("foo",true);
  ls.back()->retired_refs.emplace("bar");
}
