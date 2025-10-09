// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "cls/user/cls_user_types.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "include/utime.h"

using std::list;
using std::string;

using ceph::Formatter;
using ceph::bufferlist;
using ceph::real_clock;

void cls_user_gen_test_bucket(cls_user_bucket *bucket, int i)
{
  char buf[16];
  snprintf(buf, sizeof(buf), ".%d", i);

  bucket->name = string("buck") + buf;
  bucket->marker = string("mark") + buf;
  bucket->bucket_id = string("bucket.id") + buf;
}

void cls_user_bucket::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("marker", marker,f);
  encode_json("bucket_id", bucket_id,f);
}

list<cls_user_bucket> cls_user_bucket::generate_test_instances()
{
  list<cls_user_bucket> ls;
  ls.emplace_back();
  cls_user_bucket b;
  cls_user_gen_test_bucket(&b, 0);
  ls.push_back(std::move(b));
  return ls;
}

void cls_user_bucket_entry::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("size", size, f);
  encode_json("size_rounded", size_rounded, f);
  encode_json("creation_time", utime_t(creation_time), f);
  encode_json("count", count, f);
  encode_json("user_stats_sync", user_stats_sync, f);
}

void cls_user_gen_test_bucket_entry(cls_user_bucket_entry *entry, int i)
{
  cls_user_gen_test_bucket(&entry->bucket, i);
  entry->size = i + 1;
  entry->size_rounded = i + 2;
  entry->creation_time = real_clock::from_time_t(i + 3);
  entry->count = i + 4;
  entry->user_stats_sync = true;
}

list<cls_user_bucket_entry> cls_user_bucket_entry::generate_test_instances()
{
  list<cls_user_bucket_entry> ls;
  ls.emplace_back();
  cls_user_bucket_entry entry;
  cls_user_gen_test_bucket_entry(&entry, 0);
  ls.push_back(std::move(entry));
  return ls;
}

void cls_user_gen_test_stats(cls_user_stats *s)
{
  s->total_entries = 1;
  s->total_bytes = 2;
  s->total_bytes_rounded = 3;
}

void cls_user_stats::dump(Formatter *f) const
{
  f->dump_int("total_entries", total_entries);
  f->dump_int("total_bytes", total_bytes);
  f->dump_int("total_bytes_rounded", total_bytes_rounded);
}

list<cls_user_stats> cls_user_stats::generate_test_instances()
{
  list<cls_user_stats> ls;
  ls.emplace_back();
  cls_user_stats s;
  cls_user_gen_test_stats(&s);
  ls.push_back(std::move(s));
  return ls;
}

void cls_user_gen_test_header(cls_user_header *h)
{
  cls_user_gen_test_stats(&h->stats);
  h->last_stats_sync = utime_t(1, 0).to_real_time();
  h->last_stats_update = utime_t(2, 0).to_real_time();
}
  
void cls_user_header::dump(Formatter *f) const
{
  encode_json("stats", stats, f);
  encode_json("last_stats_sync", utime_t(last_stats_sync), f);
  encode_json("last_stats_update", utime_t(last_stats_update), f);
}

list<cls_user_header> cls_user_header::generate_test_instances()
{
  list<cls_user_header> ls;
  ls.emplace_back();
  cls_user_header h;
  cls_user_gen_test_header(&h);
  ls.push_back(std::move(h));
  return ls;
}


void cls_user_account_header::dump(ceph::Formatter* f) const
{
  encode_json("count", count, f);
}

std::list<cls_user_account_header> cls_user_account_header::generate_test_instances()
{
  std::list<cls_user_account_header> ls;
  ls.emplace_back();
  return ls;
}

void cls_user_account_resource::dump(ceph::Formatter* f) const
{
  encode_json("name", name, f);
  encode_json("path", path, f);
  // skip metadata
}

void cls_user_gen_test_resource(cls_user_account_resource& r)
{
  r.name = "name";
  r.path = "path";
}

std::list<cls_user_account_resource> cls_user_account_resource::generate_test_instances()
{
  std::list<cls_user_account_resource> ls;
  ls.emplace_back();
  cls_user_account_resource p;
  cls_user_gen_test_resource(p);
  ls.push_back(std::move(p));
  return ls;
}
