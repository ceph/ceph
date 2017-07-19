
#include "cls/rgw/cls_rgw_ops.h"

#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "include/utime.h"

void rgw_cls_tag_timeout_op::dump(Formatter *f) const
{
  f->dump_int("tag_timeout", tag_timeout);
}

void rgw_cls_tag_timeout_op::generate_test_instances(list<rgw_cls_tag_timeout_op*>& ls)
{
  ls.push_back(new rgw_cls_tag_timeout_op);
  ls.push_back(new rgw_cls_tag_timeout_op);
  ls.back()->tag_timeout = 23323;
}

void cls_rgw_gc_set_entry_op::dump(Formatter *f) const
{
  f->dump_unsigned("expiration_secs", expiration_secs);
  f->open_object_section("obj_info");
  info.dump(f);
  f->close_section();
}

void cls_rgw_gc_set_entry_op::generate_test_instances(list<cls_rgw_gc_set_entry_op*>& ls)
{
  ls.push_back(new cls_rgw_gc_set_entry_op);
  ls.push_back(new cls_rgw_gc_set_entry_op);
  ls.back()->expiration_secs = 123;
}

void cls_rgw_gc_defer_entry_op::dump(Formatter *f) const
{
  f->dump_unsigned("expiration_secs", expiration_secs);
  f->dump_string("tag", tag);
}

void cls_rgw_gc_defer_entry_op::generate_test_instances(list<cls_rgw_gc_defer_entry_op*>& ls)
{
  ls.push_back(new cls_rgw_gc_defer_entry_op);
  ls.push_back(new cls_rgw_gc_defer_entry_op);
  ls.back()->expiration_secs = 123;
  ls.back()->tag = "footag";
}

void cls_rgw_gc_list_op::dump(Formatter *f) const
{
  f->dump_string("marker", marker);
  f->dump_unsigned("max", max);
  f->dump_bool("expired_only", expired_only);
}

void cls_rgw_gc_list_op::generate_test_instances(list<cls_rgw_gc_list_op*>& ls)
{
  ls.push_back(new cls_rgw_gc_list_op);
  ls.push_back(new cls_rgw_gc_list_op);
  ls.back()->marker = "mymarker";
  ls.back()->max = 2312;
}

void cls_rgw_gc_list_ret::dump(Formatter *f) const
{
  encode_json("entries", entries, f);
  f->dump_string("next_marker", next_marker);
  f->dump_int("truncated", (int)truncated);
}

void cls_rgw_gc_list_ret::generate_test_instances(list<cls_rgw_gc_list_ret*>& ls)
{
  ls.push_back(new cls_rgw_gc_list_ret);
  ls.push_back(new cls_rgw_gc_list_ret);
  ls.back()->entries.push_back(cls_rgw_gc_obj_info());
  ls.back()->truncated = true;
}


void cls_rgw_gc_remove_op::dump(Formatter *f) const
{
  encode_json("tags", tags, f);
}

void cls_rgw_gc_remove_op::generate_test_instances(list<cls_rgw_gc_remove_op*>& ls)
{
  ls.push_back(new cls_rgw_gc_remove_op);
  ls.push_back(new cls_rgw_gc_remove_op);
  ls.back()->tags.push_back("tag1");
  ls.back()->tags.push_back("tag2");
}

void rgw_cls_obj_prepare_op::generate_test_instances(list<rgw_cls_obj_prepare_op*>& o)
{
  rgw_cls_obj_prepare_op *op = new rgw_cls_obj_prepare_op;
  op->op = CLS_RGW_OP_ADD;
  op->key.name = "name";
  op->tag = "tag";
  op->locator = "locator";
  o.push_back(op);
  o.push_back(new rgw_cls_obj_prepare_op);
}

void rgw_cls_obj_prepare_op::dump(Formatter *f) const
{
  f->dump_int("op", op);
  f->dump_string("name", key.name);
  f->dump_string("tag", tag);
  f->dump_string("locator", locator);
  f->dump_bool("log_op", log_op);
  f->dump_int("bilog_flags", bilog_flags);
}

void rgw_cls_obj_complete_op::generate_test_instances(list<rgw_cls_obj_complete_op*>& o)
{
  rgw_cls_obj_complete_op *op = new rgw_cls_obj_complete_op;
  op->op = CLS_RGW_OP_DEL;
  op->key.name = "name";
  op->locator = "locator";
  op->ver.pool = 2;
  op->ver.epoch = 100;
  op->tag = "tag";

  list<rgw_bucket_dir_entry_meta *> l;
  rgw_bucket_dir_entry_meta::generate_test_instances(l);
  list<rgw_bucket_dir_entry_meta *>::iterator iter = l.begin();
  op->meta = *(*iter);

  o.push_back(op);

  o.push_back(new rgw_cls_obj_complete_op);
}

void rgw_cls_obj_complete_op::dump(Formatter *f) const
{
  f->dump_int("op", (int)op);
  f->dump_string("name", key.name);
  f->dump_string("instance", key.instance);
  f->dump_string("locator", locator);
  f->open_object_section("ver");
  ver.dump(f);
  f->close_section();
  f->open_object_section("meta");
  meta.dump(f);
  f->close_section();
  f->dump_string("tag", tag);
  f->dump_bool("log_op", log_op);
  f->dump_int("bilog_flags", bilog_flags);
}

void rgw_cls_link_olh_op::generate_test_instances(list<rgw_cls_link_olh_op*>& o)
{
  rgw_cls_link_olh_op *op = new rgw_cls_link_olh_op;
  op->key.name = "name";
  op->olh_tag = "olh_tag";
  op->delete_marker = true;
  op->op_tag = "op_tag";
  op->olh_epoch = 123;
  list<rgw_bucket_dir_entry_meta *> l;
  rgw_bucket_dir_entry_meta::generate_test_instances(l);
  list<rgw_bucket_dir_entry_meta *>::iterator iter = l.begin();
  op->meta = *(*iter);
  op->log_op = true;

  o.push_back(op);

  o.push_back(new rgw_cls_link_olh_op);
}

void rgw_cls_link_olh_op::dump(Formatter *f) const
{
  ::encode_json("key", key, f);
  ::encode_json("olh_tag", olh_tag, f);
  ::encode_json("delete_marker", delete_marker, f);
  ::encode_json("op_tag", op_tag, f);
  ::encode_json("meta", meta, f);
  ::encode_json("olh_epoch", olh_epoch, f);
  ::encode_json("log_op", log_op, f);
  ::encode_json("bilog_flags", (uint32_t)bilog_flags, f);
  utime_t ut(unmod_since);
  ::encode_json("unmod_since", ut, f);
  ::encode_json("high_precision_time", high_precision_time, f);
}

void rgw_cls_unlink_instance_op::generate_test_instances(list<rgw_cls_unlink_instance_op*>& o)
{
  rgw_cls_unlink_instance_op *op = new rgw_cls_unlink_instance_op;
  op->key.name = "name";
  op->op_tag = "op_tag";
  op->olh_epoch = 124;
  op->log_op = true;

  o.push_back(op);

  o.push_back(new rgw_cls_unlink_instance_op);
}

void rgw_cls_unlink_instance_op::dump(Formatter *f) const
{
  ::encode_json("key", key, f);
  ::encode_json("op_tag", op_tag, f);
  ::encode_json("olh_epoch", olh_epoch, f);
  ::encode_json("log_op", log_op, f);
  ::encode_json("bilog_flags", (uint32_t)bilog_flags, f);
}

void rgw_cls_read_olh_log_op::generate_test_instances(list<rgw_cls_read_olh_log_op*>& o)
{
  rgw_cls_read_olh_log_op *op = new rgw_cls_read_olh_log_op;
  op->olh.name = "name";
  op->ver_marker = 123;
  op->olh_tag = "olh_tag";

  o.push_back(op);

  o.push_back(new rgw_cls_read_olh_log_op);
}

void rgw_cls_read_olh_log_op::dump(Formatter *f) const
{
  ::encode_json("olh", olh, f);
  ::encode_json("ver_marker", ver_marker, f);
  ::encode_json("olh_tag", olh_tag, f);
}

void rgw_cls_read_olh_log_ret::generate_test_instances(list<rgw_cls_read_olh_log_ret*>& o)
{
  rgw_cls_read_olh_log_ret *r = new rgw_cls_read_olh_log_ret;
  r->is_truncated = true;
  list<rgw_bucket_olh_log_entry *> l;
  rgw_bucket_olh_log_entry::generate_test_instances(l);
  list<rgw_bucket_olh_log_entry *>::iterator iter = l.begin();
  r->log[1].push_back(*(*iter));

  o.push_back(r);

  o.push_back(new rgw_cls_read_olh_log_ret);
}

void rgw_cls_read_olh_log_ret::dump(Formatter *f) const
{
  ::encode_json("log", log, f);
  ::encode_json("is_truncated", is_truncated, f);
}

void rgw_cls_trim_olh_log_op::generate_test_instances(list<rgw_cls_trim_olh_log_op*>& o)
{
  rgw_cls_trim_olh_log_op *op = new rgw_cls_trim_olh_log_op;
  op->olh.name = "olh.name";
  op->ver = 100;
  op->olh_tag = "olh_tag";

  o.push_back(op);

  o.push_back(new rgw_cls_trim_olh_log_op);
}

void rgw_cls_trim_olh_log_op::dump(Formatter *f) const
{
  ::encode_json("olh", olh, f);
  ::encode_json("ver", ver, f);
  ::encode_json("olh_tag", olh_tag, f);
}

void rgw_cls_bucket_clear_olh_op::generate_test_instances(list<rgw_cls_bucket_clear_olh_op *>& o)
{

  rgw_cls_bucket_clear_olh_op *op = new rgw_cls_bucket_clear_olh_op;
  op->key.name = "key.name";
  op->olh_tag = "olh_tag";

  o.push_back(op);
  o.push_back(new rgw_cls_bucket_clear_olh_op);
}

void rgw_cls_bucket_clear_olh_op::dump(Formatter *f) const
{
  ::encode_json("key", key, f);
  ::encode_json("olh_tag", olh_tag, f);
}

void rgw_cls_list_op::generate_test_instances(list<rgw_cls_list_op*>& o)
{
  rgw_cls_list_op *op = new rgw_cls_list_op;
  op->start_obj.name = "start_obj";
  op->num_entries = 100;
  op->filter_prefix = "filter_prefix";
  o.push_back(op);
  o.push_back(new rgw_cls_list_op);
}

void rgw_cls_list_op::dump(Formatter *f) const
{
  f->dump_string("start_obj", start_obj.name);
  f->dump_unsigned("num_entries", num_entries);
}

void rgw_cls_list_ret::generate_test_instances(list<rgw_cls_list_ret*>& o)
{
 list<rgw_bucket_dir *> l;
  rgw_bucket_dir::generate_test_instances(l);
  list<rgw_bucket_dir *>::iterator iter;
  for (iter = l.begin(); iter != l.end(); ++iter) {
    rgw_bucket_dir *d = *iter;

    rgw_cls_list_ret *ret = new rgw_cls_list_ret;
    ret->dir = *d;
    ret->is_truncated = true;

    o.push_back(ret);

    delete d;
  }

  o.push_back(new rgw_cls_list_ret);
}

void rgw_cls_list_ret::dump(Formatter *f) const
{
  f->open_object_section("dir");
  dir.dump(f);
  f->close_section();
  f->dump_int("is_truncated", (int)is_truncated);
}

void rgw_cls_check_index_ret::generate_test_instances(list<rgw_cls_check_index_ret*>& o)
{
  list<rgw_bucket_dir_header *> h;
  rgw_bucket_dir_header::generate_test_instances(h);
  rgw_cls_check_index_ret *r = new rgw_cls_check_index_ret;
  r->existing_header = *(h.front());
  r->calculated_header = *(h.front());
  o.push_back(r);

  for (list<rgw_bucket_dir_header *>::iterator iter = h.begin(); iter != h.end(); ++iter) {
    delete *iter;
  }
  o.push_back(new rgw_cls_check_index_ret);
}

void rgw_cls_check_index_ret::dump(Formatter *f) const
{
  ::encode_json("existing_header", existing_header, f);
  ::encode_json("calculated_header", calculated_header, f);
}

void rgw_cls_bucket_update_stats_op::generate_test_instances(list<rgw_cls_bucket_update_stats_op*>& o)
{
  rgw_cls_bucket_update_stats_op *r = new rgw_cls_bucket_update_stats_op;
  r->absolute = true;
  rgw_bucket_category_stats& s = r->stats[0];
  s.total_size = 1;
  s.total_size_rounded = 4096;
  s.num_entries = 1;
  o.push_back(r);

  o.push_back(new rgw_cls_bucket_update_stats_op);
}

void rgw_cls_bucket_update_stats_op::dump(Formatter *f) const
{
  ::encode_json("absolute", absolute, f);
  map<int, rgw_bucket_category_stats> s;
  for (auto& entry : stats) {
    s[(int)entry.first] = entry.second;
  }
  ::encode_json("stats", s, f);
}

void cls_rgw_bi_log_list_op::dump(Formatter *f) const
{
  f->dump_string("marker", marker);
  f->dump_unsigned("max", max);
}

void cls_rgw_bi_log_list_op::generate_test_instances(list<cls_rgw_bi_log_list_op*>& ls)
{
  ls.push_back(new cls_rgw_bi_log_list_op);
  ls.push_back(new cls_rgw_bi_log_list_op);
  ls.back()->marker = "mark";
  ls.back()->max = 123;
}

void cls_rgw_bi_log_trim_op::dump(Formatter *f) const
{
  f->dump_string("start_marker", start_marker);
  f->dump_string("end_marker", end_marker);
}

void cls_rgw_bi_log_trim_op::generate_test_instances(list<cls_rgw_bi_log_trim_op*>& ls)
{
  ls.push_back(new cls_rgw_bi_log_trim_op);
  ls.push_back(new cls_rgw_bi_log_trim_op);
  ls.back()->start_marker = "foo";
  ls.back()->end_marker = "bar";
}

void cls_rgw_bi_log_list_ret::dump(Formatter *f) const
{
  encode_json("entries", entries, f);
  f->dump_unsigned("truncated", (int)truncated);
}

void cls_rgw_bi_log_list_ret::generate_test_instances(list<cls_rgw_bi_log_list_ret*>& ls)
{
  ls.push_back(new cls_rgw_bi_log_list_ret);
  ls.push_back(new cls_rgw_bi_log_list_ret);
  ls.back()->entries.push_back(rgw_bi_log_entry());
  ls.back()->truncated = true;
}
