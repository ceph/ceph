// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/user/cls_user_types.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

void cls_user_gen_test_bucket(cls_user_bucket *bucket, int i)
{
  char buf[16];
  snprintf(buf, sizeof(buf), ".%d", i);

  bucket->name = string("buck") + buf;
  bucket->data_pool = string(".data.pool") + buf;
  bucket->index_pool = string(".index.pool") + buf;
  bucket->marker = string("mark") + buf;
  bucket->bucket_id = string("bucket.id") + buf;
}

void cls_user_bucket::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("data_pool", data_pool,f);
  encode_json("data_extra_pool", data_extra_pool,f);
  encode_json("index_pool", index_pool,f);
  encode_json("marker", marker,f);
  encode_json("bucket_id", bucket_id,f);
}

void cls_user_bucket::generate_test_instances(list<cls_user_bucket*>& ls)
{
  ls.push_back(new cls_user_bucket);
  cls_user_bucket *b = new cls_user_bucket;
  cls_user_gen_test_bucket(b, 0);
  ls.push_back(b);
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

void cls_user_bucket_entry::generate_test_instances(list<cls_user_bucket_entry*>& ls)
{
  ls.push_back(new cls_user_bucket_entry);
  cls_user_bucket_entry *entry = new cls_user_bucket_entry;
  cls_user_gen_test_bucket_entry(entry, 0);
  ls.push_back(entry);
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

void cls_user_stats::generate_test_instances(list<cls_user_stats*>& ls)
{
  ls.push_back(new cls_user_stats);
  cls_user_stats *s = new cls_user_stats;
  cls_user_gen_test_stats(s);
  ls.push_back(s);
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

void cls_user_header::generate_test_instances(list<cls_user_header*>& ls)
{
  ls.push_back(new cls_user_header);
  cls_user_header *h = new cls_user_header;
  cls_user_gen_test_header(h);
  ls.push_back(h);
}
