/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "cls_replica_log_ops.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

void cls_replica_log_delete_marker_op::dump(Formatter *f) const
{
  f->dump_string("entity_id", entity_id);
}

void cls_replica_log_delete_marker_op::
generate_test_instances(std::list<cls_replica_log_delete_marker_op*>& ls)
{
  ls.push_back(new cls_replica_log_delete_marker_op);
  ls.push_back(new cls_replica_log_delete_marker_op);
  ls.back()->entity_id = "test_entity_1";
}

void cls_replica_log_set_marker_op::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
}

void cls_replica_log_set_marker_op::
generate_test_instances(std::list<cls_replica_log_set_marker_op*>& ls)
{
  std::list<cls_replica_log_progress_marker*> samples;
  cls_replica_log_progress_marker::generate_test_instances(samples);
  std::list<cls_replica_log_progress_marker*>::iterator i;
  for (i = samples.begin(); i != samples.end(); ++i) {
    ls.push_back(new cls_replica_log_set_marker_op(*(*i)));
  }
}

void cls_replica_log_get_bounds_op::dump(Formatter *f) const
{
  f->dump_string("contents", "empty");
}

void cls_replica_log_get_bounds_op::
generate_test_instances(std::list<cls_replica_log_get_bounds_op*>& ls)
{
  ls.push_back(new cls_replica_log_get_bounds_op);
}

void cls_replica_log_get_bounds_ret::dump(Formatter *f) const
{
  f->dump_string("position_marker", position_marker);
  oldest_time.gmtime(f->dump_stream("oldest_time"));
  encode_json("entity_markers", markers, f);
}

void cls_replica_log_get_bounds_ret::
generate_test_instances(std::list<cls_replica_log_get_bounds_ret*>& ls)
{
  std::list<cls_replica_log_progress_marker*> samples;
  cls_replica_log_progress_marker::generate_test_instances(samples);
  std::list<cls_replica_log_progress_marker> samples_whole;
  std::list<cls_replica_log_progress_marker*>::iterator i;
  int count = 0;
  for (i = samples.begin(); i != samples.end(); ++i) {
    ls.push_back(new cls_replica_log_get_bounds_ret());
    ls.back()->markers.push_back(*(*i));
    ls.back()->oldest_time.set_from_double(1000*count);
    ls.back()->position_marker = ls.back()->markers.front().position_marker;
    samples_whole.push_back(*(*i));
  }
  ls.push_back(new cls_replica_log_get_bounds_ret());
  ls.back()->markers = samples_whole;
  ls.back()->oldest_time = samples_whole.back().position_time;
  ls.back()->position_marker = samples_whole.back().position_marker;
}
