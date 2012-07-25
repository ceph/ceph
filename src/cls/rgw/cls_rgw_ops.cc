
#include "cls/rgw/cls_rgw_ops.h"

#include "common/Formatter.h"


void rgw_cls_obj_prepare_op::generate_test_instances(list<rgw_cls_obj_prepare_op*>& o)
{
  rgw_cls_obj_prepare_op *op = new rgw_cls_obj_prepare_op;
  op->op = CLS_RGW_OP_ADD;
  op->name = "name";
  op->tag = "tag";
  op->locator = "locator";
  o.push_back(op);
  o.push_back(new rgw_cls_obj_prepare_op);
}

void rgw_cls_obj_prepare_op::dump(Formatter *f) const
{
  f->dump_int("op", op);
  f->dump_string("name", name);
  f->dump_string("tag", tag);
  f->dump_string("locator", locator);
}

void rgw_cls_obj_complete_op::generate_test_instances(list<rgw_cls_obj_complete_op*>& o)
{
  rgw_cls_obj_complete_op *op = new rgw_cls_obj_complete_op;
  op->op = CLS_RGW_OP_DEL;
  op->name = "name";
  op->locator = "locator";
  op->epoch = 100;
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
  f->dump_string("name", name);
  f->dump_string("locator", locator);
  f->dump_unsigned("epoch", epoch);
  f->open_object_section("meta");
  meta.dump(f);
  f->close_section();
  f->dump_string("tag", tag);
}

void rgw_cls_list_op::generate_test_instances(list<rgw_cls_list_op*>& o)
{
  rgw_cls_list_op *op = new rgw_cls_list_op;
  op->start_obj = "start_obj";
  op->num_entries = 100;
  op->filter_prefix = "filter_prefix";
  o.push_back(op);
  o.push_back(new rgw_cls_list_op);
}

void rgw_cls_list_op::dump(Formatter *f) const
{
  f->dump_string("start_obj", start_obj);
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

