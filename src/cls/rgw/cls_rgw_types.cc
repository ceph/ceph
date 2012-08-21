
#include "cls/rgw/cls_rgw_types.h"
#include "common/Formatter.h"


void rgw_bucket_pending_info::generate_test_instances(list<rgw_bucket_pending_info*>& o)
{
  rgw_bucket_pending_info *i = new rgw_bucket_pending_info;
  i->state = CLS_RGW_STATE_COMPLETE;
  i->op = CLS_RGW_OP_DEL;
  o.push_back(i);
  o.push_back(new rgw_bucket_pending_info);
}

void rgw_bucket_pending_info::dump(Formatter *f) const
{
  f->dump_int("state", (int)state);
  f->dump_stream("timestamp") << timestamp;
  f->dump_int("op", (int)op);
}

void rgw_bucket_dir_entry_meta::generate_test_instances(list<rgw_bucket_dir_entry_meta*>& o)
{
  rgw_bucket_dir_entry_meta *m = new rgw_bucket_dir_entry_meta;
  m->category = 1;
  m->size = 100;
  m->etag = "etag";
  m->owner = "owner";
  m->owner_display_name = "display name";
  m->tag = "tag";
  m->content_type = "content/type";
  o.push_back(m);
  o.push_back(new rgw_bucket_dir_entry_meta);
}

void rgw_bucket_dir_entry_meta::dump(Formatter *f) const
{
  f->dump_int("category", category);
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
  f->dump_string("etag", etag);
  f->dump_string("owner", owner);
  f->dump_string("owner_display_name", owner_display_name);
  f->dump_string("tag", tag);
  f->dump_string("content_type", content_type);
}

void rgw_bucket_dir_entry::generate_test_instances(list<rgw_bucket_dir_entry*>& o)
{
  list<rgw_bucket_dir_entry_meta *> l;
  rgw_bucket_dir_entry_meta::generate_test_instances(l);

  list<rgw_bucket_dir_entry_meta *>::iterator iter;
  for (iter = l.begin(); iter != l.end(); ++iter) {
    rgw_bucket_dir_entry_meta *m = *iter;
    rgw_bucket_dir_entry *e = new rgw_bucket_dir_entry;
    e->name = "name";
    e->epoch = 1234;
    e->locator = "locator";
    e->exists = true;
    e->meta = *m;

    o.push_back(e);

    delete m;
  }
  o.push_back(new rgw_bucket_dir_entry);
}

void rgw_bucket_dir_entry::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_unsigned("epoch", epoch);
  f->dump_string("locator", locator);
  f->dump_int("exists", (int)exists);
  f->open_object_section("meta");
  meta.dump(f);
  f->close_section();

  map<string, struct rgw_bucket_pending_info>::const_iterator iter = pending_map.begin();
  f->open_array_section("pending_map");
  for (; iter != pending_map.end(); ++iter) {
    f->dump_string("tag", iter->first);
    f->open_object_section("info");
    iter->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void rgw_bucket_category_stats::generate_test_instances(list<rgw_bucket_category_stats*>& o)
{
  rgw_bucket_category_stats *s = new rgw_bucket_category_stats;
  s->total_size = 1024;
  s->total_size_rounded = 4096;
  s->num_entries = 2;
  o.push_back(s);
  o.push_back(new rgw_bucket_category_stats);
}

void rgw_bucket_category_stats::dump(Formatter *f) const
{
  f->dump_unsigned("total_size", total_size);
  f->dump_unsigned("total_size_rounded", total_size_rounded);
  f->dump_unsigned("num_entries", num_entries);
}

void rgw_bucket_dir_header::generate_test_instances(list<rgw_bucket_dir_header*>& o)
{
  list<rgw_bucket_category_stats *> l;
  list<rgw_bucket_category_stats *>::iterator iter;
  rgw_bucket_category_stats::generate_test_instances(l);

  uint8_t i;
  for (i = 0, iter = l.begin(); iter != l.end(); ++iter, ++i) {
    rgw_bucket_dir_header *h = new rgw_bucket_dir_header;
    rgw_bucket_category_stats *s = *iter;
    h->stats[i] = *s;

    o.push_back(h);

    delete s;
  }

  o.push_back(new rgw_bucket_dir_header);
}

void rgw_bucket_dir_header::dump(Formatter *f) const
{
  map<uint8_t, struct rgw_bucket_category_stats>::const_iterator iter = stats.begin();
  f->open_array_section("stats");
  for (; iter != stats.end(); ++iter) {
    f->dump_int("category", (int)iter->first);
    f->open_object_section("category_stats");
    iter->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void rgw_bucket_dir::generate_test_instances(list<rgw_bucket_dir*>& o)
{
  list<rgw_bucket_dir_header *> l;
  list<rgw_bucket_dir_header *>::iterator iter;
  rgw_bucket_dir_header::generate_test_instances(l);

  uint8_t i;
  for (i = 0, iter = l.begin(); iter != l.end(); ++iter, ++i) {
    rgw_bucket_dir *d = new rgw_bucket_dir;
    rgw_bucket_dir_header *h = *iter;
    d->header = *h;

    list<rgw_bucket_dir_entry *> el;
    list<rgw_bucket_dir_entry *>::iterator eiter;
    for (eiter = el.begin(); eiter != el.end(); ++eiter) {
      rgw_bucket_dir_entry *e = *eiter;
      d->m[e->name] = *e;

      delete e;
    }

    o.push_back(d);

    delete h;
  }

  o.push_back(new rgw_bucket_dir);
}

void rgw_bucket_dir::dump(Formatter *f) const
{
  f->open_object_section("header");
  header.dump(f);
  f->close_section();
  map<string, struct rgw_bucket_dir_entry>::const_iterator iter = m.begin();
  f->open_array_section("map");
  for (; iter != m.end(); ++iter) {
    f->dump_string("obj", iter->first);
    f->open_object_section("dir_entry");
    iter->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

