// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "cls_refcount_ops.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

using std::list;

void cls_refcount_get_op::dump(ceph::Formatter *f) const
{
  f->dump_string("tag", tag);
  f->dump_int("implicit_ref", (int)implicit_ref);
}

list<cls_refcount_get_op> cls_refcount_get_op::generate_test_instances()
{
  list<cls_refcount_get_op> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().tag = "foo";
  ls.back().implicit_ref = true;
  return ls;
}


void cls_refcount_put_op::dump(ceph::Formatter *f) const
{
  f->dump_string("tag", tag);
  f->dump_int("implicit_ref", (int)implicit_ref);
}

list<cls_refcount_put_op> cls_refcount_put_op::generate_test_instances()
{
  list<cls_refcount_put_op> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().tag = "foo";
  ls.back().implicit_ref = true;
  return ls;
}



void cls_refcount_set_op::dump(ceph::Formatter *f) const
{
  encode_json("refs", refs, f);
}

list<cls_refcount_set_op> cls_refcount_set_op::generate_test_instances()
{
  list<cls_refcount_set_op> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().refs.push_back("foo");
  ls.back().refs.push_back("bar");
  return ls;
}


void cls_refcount_read_op::dump(ceph::Formatter *f) const
{
  f->dump_int("implicit_ref", (int)implicit_ref);
}

list<cls_refcount_read_op> cls_refcount_read_op::generate_test_instances()
{
  list<cls_refcount_read_op> ls;
  ls.emplace_back();
  ls.emplace_back();
  return ls;
}


void cls_refcount_read_ret::dump(ceph::Formatter *f) const
{
  f->open_array_section("refs");
  for (auto p = refs.begin(); p != refs.end(); ++p)
    f->dump_string("ref", *p);
  f->close_section();
}

list<cls_refcount_read_ret> cls_refcount_read_ret::generate_test_instances()
{
  list<cls_refcount_read_ret> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().refs.push_back("foo");
  ls.back().refs.push_back("bar");
  return ls;
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

list<obj_refcount> obj_refcount::generate_test_instances()
{
  list<obj_refcount> ls;
  ls.emplace_back();
  ls.back().refs.emplace("foo",true);
  ls.back().retired_refs.emplace("bar");
  return ls;
}
