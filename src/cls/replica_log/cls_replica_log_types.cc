/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "cls_replica_log_types.h"

#include "common/Formatter.h"
#include "common/ceph_json.h"

void cls_replica_log_item_marker::dump(Formatter *f) const
{
  f->dump_string("name", item_name);
  f->dump_stream("timestamp") << item_timestamp;
}

void cls_replica_log_item_marker::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", item_name, obj);
  JSONDecoder::decode_json("timestamp", item_timestamp, obj);
}

void cls_replica_log_item_marker::
generate_test_instances(std::list<cls_replica_log_item_marker*>& ls)
{
  ls.push_back(new cls_replica_log_item_marker);
  ls.back()->item_name = "test_item_1";
  ls.back()->item_timestamp.set_from_double(0);
  ls.push_back(new cls_replica_log_item_marker);
  ls.back()->item_name = "test_item_2";
  ls.back()->item_timestamp.set_from_double(20);
}

void cls_replica_log_progress_marker::dump(Formatter *f) const
{
  encode_json("entity", entity_id, f);
  encode_json("position_marker", position_marker, f);
  encode_json("position_time", position_time, f);
  encode_json("items_in_progress", items, f);
}

void cls_replica_log_progress_marker::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("entity", entity_id, obj);
  JSONDecoder::decode_json("position_marker", position_marker, obj);
  JSONDecoder::decode_json("position_time", position_time, obj);
  JSONDecoder::decode_json("items_in_progress", items, obj);
}

void cls_replica_log_progress_marker::
generate_test_instances(std::list<cls_replica_log_progress_marker*>& ls)
{
  ls.push_back(new cls_replica_log_progress_marker);
  ls.push_back(new cls_replica_log_progress_marker);
  ls.back()->entity_id = "entity1";
  ls.back()->position_marker = "pos1";
  ls.back()->position_time.set_from_double(20);

  std::list<cls_replica_log_item_marker*> test_items;
  cls_replica_log_item_marker::generate_test_instances(test_items);
  std::list<cls_replica_log_item_marker*>::iterator i = test_items.begin();
  for ( ; i != test_items.end(); ++i) {
    ls.back()->items.push_back(*(*i));
  }
}

void cls_replica_log_bound::dump(Formatter *f) const
{
  encode_json("position_marker", position_marker, f);
  encode_json("position_time", position_time, f);
  encode_json("marker_exists", marker_exists, f);
  if (marker_exists) {
    encode_json("marker", marker, f); //progress marker
  }
}

void cls_replica_log_bound::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("position_marker", position_marker, obj);
  JSONDecoder::decode_json("position_time", position_time, obj);
  JSONDecoder::decode_json("marker_exists", marker_exists, obj);
  if (marker_exists) {
    JSONDecoder::decode_json("marker", marker, obj); //progress marker
  }
}

void cls_replica_log_bound::
generate_test_instances(std::list<cls_replica_log_bound*>& ls)
{
  ls.push_back(new cls_replica_log_bound);
  std::list<cls_replica_log_progress_marker*> marker_objects;
  cls_replica_log_progress_marker::generate_test_instances(marker_objects);
  std::list<cls_replica_log_progress_marker*>::iterator i =
	marker_objects.begin();
  ls.back()->update_marker(*(*i));
  ls.push_back(new cls_replica_log_bound);
  ++i;
  ls.back()->update_marker(*(*i));
}
