// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rgw/cls_rgw_types.h"
#include "common/ceph_json.h"
#include "include/utime.h"

using std::list;
using std::string;

using ceph::bufferlist;
using ceph::Formatter;

void rgw_zone_set_entry::from_str(const string& s)
{
  auto pos = s.find(':');
  if (pos == string::npos) {
    zone = s;
    location_key.reset();
  } else {
    zone = s.substr(0, pos);
    location_key = s.substr(pos + 1);
  }
}

string rgw_zone_set_entry::to_str() const
{
  string s = zone;
  if (location_key) {
    s = s + ":" + *location_key;
  }
  return s;
}

void rgw_zone_set_entry::encode(bufferlist &bl) const
{
  /* no ENCODE_START, ENCODE_END for backward compatibility */
  ceph::encode(to_str(), bl);
}

void rgw_zone_set_entry::decode(bufferlist::const_iterator &bl)
{
  /* no DECODE_START, DECODE_END for backward compatibility */
  string s;
  ceph::decode(s, bl);
  from_str(s);
}

void rgw_zone_set_entry::dump(Formatter *f) const
{
  encode_json("entry", to_str(), f);
}

void rgw_zone_set_entry::decode_json(JSONObj *obj) {
  string s;
  JSONDecoder::decode_json("entry", s, obj);
  from_str(s);
}
void rgw_zone_set::dump(Formatter *f) const
{
  encode_json("entries", entries, f);
}

void rgw_zone_set::generate_test_instances(list<rgw_zone_set*>& o)
{
  o.push_back(new rgw_zone_set);
  o.push_back(new rgw_zone_set);
  std::optional<string> loc_key = "loc_key";
  o.back()->insert("zone1", loc_key);
  o.back()->insert("zone2", loc_key);
  o.back()->insert("zone3", loc_key);
}

void rgw_zone_set::insert(const string& zone, std::optional<string> location_key)
{
  entries.insert(rgw_zone_set_entry(zone, location_key));
}

bool rgw_zone_set::exists(const string& zone, std::optional<string> location_key) const
{
  return entries.find(rgw_zone_set_entry(zone, location_key)) != entries.end();
}

void encode_json(const char *name, const rgw_zone_set& zs, ceph::Formatter *f)
{
  encode_json(name, zs.entries, f);
}

void decode_json_obj(rgw_zone_set& zs, JSONObj *obj)
{
  decode_json_obj(zs.entries, obj);
}

std::string_view to_string(RGWModifyOp op)
{
  switch (op) {
    case CLS_RGW_OP_ADD: return "write";
    case CLS_RGW_OP_DEL: return "del";
    case CLS_RGW_OP_CANCEL: return "cancel";
    case CLS_RGW_OP_LINK_OLH: return "link_olh";
    case CLS_RGW_OP_LINK_OLH_DM: return "link_olh_del";
    case CLS_RGW_OP_UNLINK_INSTANCE: return "unlink_instance";
    case CLS_RGW_OP_SYNCSTOP: return "syncstop";
    case CLS_RGW_OP_RESYNC: return "resync";
    default:
    case CLS_RGW_OP_UNKNOWN: return "unknown";
  }
}

RGWModifyOp parse_modify_op(std::string_view name)
{
  if (name == "write") {
    return CLS_RGW_OP_ADD;
  } else if (name == "del") {
    return CLS_RGW_OP_DEL;
  } else if (name == "cancel") {
    return CLS_RGW_OP_CANCEL;
  } else if (name == "link_olh") {
    return CLS_RGW_OP_LINK_OLH;
  } else if (name == "link_olh_del") {
    return CLS_RGW_OP_LINK_OLH_DM;
  } else if (name == "unlink_instance") {
    return CLS_RGW_OP_UNLINK_INSTANCE;
  } else if (name == "syncstop") {
    return CLS_RGW_OP_SYNCSTOP;
  } else if (name == "resync") {
    return CLS_RGW_OP_RESYNC;
  } else {
    return CLS_RGW_OP_UNKNOWN;
  }
}

std::string_view to_string(RGWObjCategory c)
{
  switch (c) {
    case RGWObjCategory::None: return "rgw.none";
    case RGWObjCategory::Main: return "rgw.main";
    case RGWObjCategory::Shadow: return "rgw.shadow";
    case RGWObjCategory::MultiMeta: return "rgw.multimeta";
    case RGWObjCategory::CloudTiered: return "rgw.cloudtiered";
    default: return "unknown";
  }
}

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
  encode_json("state", (int)state, f);
  utime_t ut(timestamp);
  encode_json("timestamp", ut, f);
  encode_json("op", (int)op, f);
}

void rgw_bucket_pending_info::decode_json(JSONObj *obj) {
  int val;
  JSONDecoder::decode_json("state", val, obj);
  state = (RGWPendingState)val;
  utime_t ut(timestamp);
  JSONDecoder::decode_json("timestamp", ut, obj);
  JSONDecoder::decode_json("op", val, obj);
  op = (uint8_t)val;
}

void cls_rgw_obj_key::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("instance", instance, obj);
}

void rgw_bucket_dir_entry_meta::generate_test_instances(list<rgw_bucket_dir_entry_meta*>& o)
{
  rgw_bucket_dir_entry_meta *m = new rgw_bucket_dir_entry_meta;
  m->category = RGWObjCategory::Main;
  m->size = 100;
  m->etag = "etag";
  m->owner = "owner";
  m->owner_display_name = "display name";
  m->content_type = "content/type";
  o.push_back(m);
  o.push_back(new rgw_bucket_dir_entry_meta);
}

void rgw_bucket_dir_entry_meta::dump(Formatter *f) const
{
  encode_json("category", (int)category, f);
  encode_json("size", size, f);
  utime_t ut(mtime);
  encode_json("mtime", ut, f);
  encode_json("etag", etag, f);
  encode_json("storage_class", storage_class, f);
  encode_json("owner", owner, f);
  encode_json("owner_display_name", owner_display_name, f);
  encode_json("content_type", content_type, f);
  encode_json("accounted_size", accounted_size, f);
  encode_json("user_data", user_data, f);
  encode_json("appendable", appendable, f);
}

void rgw_bucket_dir_entry_meta::decode_json(JSONObj *obj) {
  int val;
  JSONDecoder::decode_json("category", val, obj);
  category = static_cast<RGWObjCategory>(val);
  JSONDecoder::decode_json("size", size, obj);
  utime_t ut;
  JSONDecoder::decode_json("mtime", ut, obj);
  mtime = ut.to_real_time();
  JSONDecoder::decode_json("etag", etag, obj);
  JSONDecoder::decode_json("storage_class", storage_class, obj);
  JSONDecoder::decode_json("owner", owner, obj);
  JSONDecoder::decode_json("owner_display_name", owner_display_name, obj);
  JSONDecoder::decode_json("content_type", content_type, obj);
  JSONDecoder::decode_json("accounted_size", accounted_size, obj);
  JSONDecoder::decode_json("user_data", user_data, obj);
  JSONDecoder::decode_json("appendable", appendable, obj);
}

void rgw_bucket_dir_entry::generate_test_instances(list<rgw_bucket_dir_entry*>& o)
{
  list<rgw_bucket_dir_entry_meta *> l;
  rgw_bucket_dir_entry_meta::generate_test_instances(l);

  for (auto iter = l.begin(); iter != l.end(); ++iter) {
    rgw_bucket_dir_entry_meta *m = *iter;
    rgw_bucket_dir_entry *e = new rgw_bucket_dir_entry;
    e->key.name = "name";
    e->ver.pool = 1;
    e->ver.epoch = 1234;
    e->locator = "locator";
    e->exists = true;
    e->meta = *m;
    e->tag = "tag";

    o.push_back(e);

    delete m;
  }
  o.push_back(new rgw_bucket_dir_entry);
}

void rgw_bucket_entry_ver::dump(Formatter *f) const
{
  encode_json("pool", pool, f);
  encode_json("epoch", epoch, f);
}

void rgw_bucket_entry_ver::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("epoch", epoch, obj);
}

void rgw_bucket_entry_ver::generate_test_instances(list<rgw_bucket_entry_ver*>& ls)
{
  ls.push_back(new rgw_bucket_entry_ver);
  ls.push_back(new rgw_bucket_entry_ver);
  ls.back()->pool = 123;
  ls.back()->epoch = 12322;
}


void rgw_bucket_dir_entry::dump(Formatter *f) const
{
  encode_json("name", key.name, f);
  encode_json("instance", key.instance , f);
  encode_json("ver", ver , f);
  encode_json("locator", locator , f);
  encode_json("exists", exists , f);
  encode_json("meta", meta , f);
  encode_json("tag", tag , f);
  encode_json("flags", (int)flags , f);
  encode_json("pending_map", pending_map, f);
  encode_json("versioned_epoch", versioned_epoch , f);
}

void rgw_bucket_dir_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("name", key.name, obj);
  JSONDecoder::decode_json("instance", key.instance , obj);
  JSONDecoder::decode_json("ver", ver , obj);
  JSONDecoder::decode_json("locator", locator , obj);
  JSONDecoder::decode_json("exists", exists , obj);
  JSONDecoder::decode_json("meta", meta , obj);
  JSONDecoder::decode_json("tag", tag , obj);
  int val;
  JSONDecoder::decode_json("flags", val , obj);
  flags = (uint16_t)val;
  JSONDecoder::decode_json("pending_map", pending_map, obj);
  JSONDecoder::decode_json("versioned_epoch", versioned_epoch, obj);
}

static void dump_bi_entry(bufferlist bl, BIIndexType index_type, Formatter *formatter)
{
  auto iter = bl.cbegin();
  switch (index_type) {
    case BIIndexType::Plain:
    case BIIndexType::Instance:
      {
        rgw_bucket_dir_entry entry;
        decode(entry, iter);
        encode_json("entry", entry, formatter);
      }
      break;
    case BIIndexType::OLH:
      {
        rgw_bucket_olh_entry entry;
        decode(entry, iter);
        encode_json("entry", entry, formatter);
      }
      break;
    case BIIndexType::ReshardDeleted:
      {
        rgw_bucket_deleted_entry entry;
        decode(entry, iter);
        encode_json("entry", entry, formatter);
      }
      break;
    default:
      break;
  }
}

void rgw_cls_bi_entry::decode_json(JSONObj *obj, cls_rgw_obj_key *effective_key) {
  JSONDecoder::decode_json("idx", idx, obj);
  string s;
  JSONDecoder::decode_json("type", s, obj);
  if (s == "plain") {
    type = BIIndexType::Plain;
  } else if (s == "instance") {
    type = BIIndexType::Instance;
  } else if (s == "olh") {
    type = BIIndexType::OLH;
  } else if (s == "resharddeleted") {
    type = BIIndexType::ReshardDeleted;
  } else {
    type = BIIndexType::Invalid;
  }
  using ceph::encode;
  switch (type) {
    case BIIndexType::Plain:
    case BIIndexType::Instance:
      {
        rgw_bucket_dir_entry entry;
        JSONDecoder::decode_json("entry", entry, obj);
        encode(entry, data);

        if (effective_key) {
          *effective_key = entry.key;
        }
      }
      break;
    case BIIndexType::OLH:
      {
        rgw_bucket_olh_entry entry;
        JSONDecoder::decode_json("entry", entry, obj);
        encode(entry, data);

        if (effective_key) {
          *effective_key = entry.key;
        }
      }
      break;
      case BIIndexType::ReshardDeleted:
      {
        rgw_bucket_deleted_entry entry;
        JSONDecoder::decode_json("entry", entry, obj);
        encode(entry, data);

        if (effective_key) {
          *effective_key = entry.key;
        }
      }
      break;
    default:
      break;
  }
}

void rgw_cls_bi_entry::dump(Formatter *f) const
{
  string type_str;
  switch (type) {
  case BIIndexType::Plain:
    type_str = "plain";
    break;
  case BIIndexType::Instance:
    type_str = "instance";
    break;
  case BIIndexType::OLH:
    type_str = "olh";
    break;
  case BIIndexType::ReshardDeleted:
    type_str = "resharddeleted";
    break;
  default:
    type_str = "invalid";
  }
  encode_json("type", type_str, f);
  encode_json("idx", idx, f);
  dump_bi_entry(data, type, f);
}

bool rgw_cls_bi_entry::get_info(cls_rgw_obj_key *key,
                                RGWObjCategory *category,
                                rgw_bucket_category_stats *accounted_stats) const
{
  using ceph::decode;
  auto iter = data.cbegin();
  if (type == BIIndexType::OLH) {
    rgw_bucket_olh_entry entry;
    decode(entry, iter);
    *key = std::move(entry.key);
    return false;
  }
  if (type == BIIndexType::ReshardDeleted) {
    rgw_bucket_deleted_entry entry;
    decode(entry, iter);
    *key = std::move(entry.key);
    return false;
  }

  rgw_bucket_dir_entry entry;
  decode(entry, iter);
  *key = entry.key;
  *category = entry.meta.category;
  accounted_stats->num_entries++;
  accounted_stats->total_size += entry.meta.accounted_size;
  accounted_stats->total_size_rounded += cls_rgw_get_rounded_size(entry.meta.accounted_size);
  accounted_stats->actual_size += entry.meta.size;
  if (type == BIIndexType::Plain) {
    return entry.exists && entry.flags == 0;
  } else if (type == BIIndexType::Instance) {
    return entry.exists;
  }
  return false;
}

void rgw_cls_bi_entry::generate_test_instances(list<rgw_cls_bi_entry*>& o)
{
  using ceph::encode;
  rgw_cls_bi_entry *m = new rgw_cls_bi_entry;
  rgw_bucket_olh_entry entry;
  entry.delete_marker = true;
  entry.epoch = 1234;
  entry.tag = "tag";
  entry.key.name = "key.name";
  entry.key.instance = "key.instance";
  entry.exists = true;
  entry.pending_removal = true;
  m->type = BIIndexType::OLH;
  m->idx = "idx";
  encode(entry,m->data);
  o.push_back(m);
  o.push_back(new rgw_cls_bi_entry);
}

void rgw_bucket_olh_entry::dump(Formatter *f) const
{
  encode_json("key", key, f);
  encode_json("delete_marker", delete_marker, f);
  encode_json("epoch", epoch, f);
  encode_json("pending_log", pending_log, f);
  encode_json("tag", tag, f);
  encode_json("exists", exists, f);
  encode_json("pending_removal", pending_removal, f);
}

void rgw_bucket_olh_entry::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("delete_marker", delete_marker, obj);
  JSONDecoder::decode_json("epoch", epoch, obj);
  JSONDecoder::decode_json("pending_log", pending_log, obj);
  JSONDecoder::decode_json("tag", tag, obj);
  JSONDecoder::decode_json("exists", exists, obj);
  JSONDecoder::decode_json("pending_removal", pending_removal, obj);
}

void rgw_bucket_olh_entry::generate_test_instances(list<rgw_bucket_olh_entry*>& o)
{
  rgw_bucket_olh_entry *entry = new rgw_bucket_olh_entry;
  entry->delete_marker = true;
  entry->epoch = 1234;
  entry->tag = "tag";
  entry->key.name = "key.name";
  entry->key.instance = "key.instance";
  entry->exists = true;
  entry->pending_removal = true;
  o.push_back(entry);
  o.push_back(new rgw_bucket_olh_entry);
}

void rgw_bucket_deleted_entry::dump(Formatter *f) const
{
  encode_json("key", key, f);
}

void rgw_bucket_deleted_entry::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("key", key, obj);
}

void rgw_bucket_deleted_entry::generate_test_instances(list<rgw_bucket_deleted_entry*>& o)
{
  rgw_bucket_deleted_entry *entry = new rgw_bucket_deleted_entry;
  entry->key.name = "key.name";
  entry->key.instance = "key.instance";
  o.push_back(entry);
  o.push_back(new rgw_bucket_deleted_entry);
}

void rgw_bucket_olh_log_entry::generate_test_instances(list<rgw_bucket_olh_log_entry*>& o)
{
  rgw_bucket_olh_log_entry *entry = new rgw_bucket_olh_log_entry;
  entry->epoch = 1234;
  entry->op = CLS_RGW_OLH_OP_LINK_OLH;
  entry->op_tag = "op_tag";
  entry->key.name = "key.name";
  entry->key.instance = "key.instance";
  entry->delete_marker = true;
  o.push_back(entry);
  o.push_back(new rgw_bucket_olh_log_entry);
}

void rgw_bucket_olh_log_entry::dump(Formatter *f) const
{
  encode_json("epoch", epoch, f);
  const char *op_str;
  switch (op) {
    case CLS_RGW_OLH_OP_LINK_OLH:
      op_str = "link_olh";
      break;
    case CLS_RGW_OLH_OP_UNLINK_OLH:
      op_str = "unlink_olh";
      break;
    case CLS_RGW_OLH_OP_REMOVE_INSTANCE:
      op_str = "remove_instance";
      break;
    default:
      op_str = "unknown";
  }
  encode_json("op", op_str, f);
  encode_json("op_tag", op_tag, f);
  encode_json("key", key, f);
  encode_json("delete_marker", delete_marker, f);
}

void rgw_bucket_olh_log_entry::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("epoch", epoch, obj);
  string op_str;
  JSONDecoder::decode_json("op", op_str, obj);
  if (op_str == "link_olh") {
    op = CLS_RGW_OLH_OP_LINK_OLH;
  } else if (op_str == "unlink_olh") {
    op = CLS_RGW_OLH_OP_UNLINK_OLH;
  } else if (op_str == "remove_instance") {
    op = CLS_RGW_OLH_OP_REMOVE_INSTANCE;
  } else {
    op = CLS_RGW_OLH_OP_UNKNOWN;
  }
  JSONDecoder::decode_json("op_tag", op_tag, obj);
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("delete_marker", delete_marker, obj);
}

void rgw_bi_log_entry::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("op_id", id, obj);
  JSONDecoder::decode_json("op_tag", tag, obj);
  string op_str;
  JSONDecoder::decode_json("op", op_str, obj);
  op = parse_modify_op(op_str);
  JSONDecoder::decode_json("object", object, obj);
  JSONDecoder::decode_json("instance", instance, obj);
  string state_str;
  JSONDecoder::decode_json("state", state_str, obj);
  if (state_str == "pending") {
    state = CLS_RGW_STATE_PENDING_MODIFY;
  } else if (state_str == "complete") {
    state = CLS_RGW_STATE_COMPLETE;
  } else {
    state = CLS_RGW_STATE_UNKNOWN;
  }
  JSONDecoder::decode_json("index_ver", index_ver, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
  uint32_t f;
  JSONDecoder::decode_json("bilog_flags", f, obj);
  JSONDecoder::decode_json("ver", ver, obj);
  bilog_flags = (uint16_t)f;
  JSONDecoder::decode_json("owner", owner, obj);
  JSONDecoder::decode_json("owner_display_name", owner_display_name, obj);
  JSONDecoder::decode_json("zones_trace", zones_trace, obj);
}

void rgw_bi_log_entry::dump(Formatter *f) const
{
  f->dump_string("op_id", id);
  f->dump_string("op_tag", tag);
  f->dump_string("op", to_string(op));
  f->dump_string("object", object);
  f->dump_string("instance", instance);

  switch (state) {
    case CLS_RGW_STATE_PENDING_MODIFY:
      f->dump_string("state", "pending");
      break;
    case CLS_RGW_STATE_COMPLETE:
      f->dump_string("state", "complete");
      break;
    default:
      f->dump_string("state", "invalid");
      break;
  }

  f->dump_int("index_ver", index_ver);
  utime_t ut(timestamp);
  ut.gmtime_nsec(f->dump_stream("timestamp"));
  f->open_object_section("ver");
  ver.dump(f);
  f->close_section();
  f->dump_int("bilog_flags", bilog_flags);
  f->dump_bool("versioned", (bilog_flags & RGW_BILOG_FLAG_VERSIONED_OP) != 0);
  f->dump_string("owner", owner);
  f->dump_string("owner_display_name", owner_display_name);
  encode_json("zones_trace", zones_trace, f);
}

void rgw_bi_log_entry::generate_test_instances(list<rgw_bi_log_entry*>& ls)
{
  ls.push_back(new rgw_bi_log_entry);
  ls.push_back(new rgw_bi_log_entry);
  ls.back()->id = "midf";
  ls.back()->object = "obj";
  ls.back()->timestamp = ceph::real_clock::from_ceph_timespec({ceph_le32(2), ceph_le32(3)});
  ls.back()->index_ver = 4323;
  ls.back()->tag = "tagasdfds";
  ls.back()->op = CLS_RGW_OP_DEL;
  ls.back()->state = CLS_RGW_STATE_PENDING_MODIFY;
}

void rgw_bucket_category_stats::generate_test_instances(list<rgw_bucket_category_stats*>& o)
{
  rgw_bucket_category_stats *s = new rgw_bucket_category_stats;
  s->total_size = 1024;
  s->total_size_rounded = 4096;
  s->num_entries = 2;
  s->actual_size = 1024;
  o.push_back(s);
  o.push_back(new rgw_bucket_category_stats);
}

void rgw_bucket_category_stats::dump(Formatter *f) const
{
  f->dump_unsigned("total_size", total_size);
  f->dump_unsigned("total_size_rounded", total_size_rounded);
  f->dump_unsigned("num_entries", num_entries);
  f->dump_unsigned("actual_size", actual_size);
}

void rgw_bucket_dir_header::generate_test_instances(list<rgw_bucket_dir_header*>& o)
{
  list<rgw_bucket_category_stats *> l;
  rgw_bucket_category_stats::generate_test_instances(l);

  uint8_t i = 0;
  for (auto iter = l.begin(); iter != l.end(); ++iter, ++i) {
    RGWObjCategory c = static_cast<RGWObjCategory>(i);
    rgw_bucket_dir_header *h = new rgw_bucket_dir_header;
    rgw_bucket_category_stats *s = *iter;
    h->stats[c] = *s;

    o.push_back(h);

    delete s;
  }

  o.push_back(new rgw_bucket_dir_header);
}

void rgw_bucket_dir_header::dump(Formatter *f) const
{
  f->dump_int("ver", ver);
  f->dump_int("master_ver", master_ver);
  f->open_array_section("stats");
  for (auto iter = stats.begin(); iter != stats.end(); ++iter) {
    f->dump_int("category", int(iter->first));
    f->open_object_section("category_stats");
    iter->second.dump(f);
    f->close_section();
  }
  f->close_section();
  ::encode_json("new_instance", new_instance, f);
  f->dump_int("reshardlog_entries", reshardlog_entries);
}

void rgw_bucket_dir::generate_test_instances(list<rgw_bucket_dir*>& o)
{
  list<rgw_bucket_dir_header *> l;
  rgw_bucket_dir_header::generate_test_instances(l);

  uint8_t i = 0;
  for (auto iter = l.begin(); iter != l.end(); ++iter, ++i) {
    rgw_bucket_dir *d = new rgw_bucket_dir;
    rgw_bucket_dir_header *h = *iter;
    d->header = *h;

    list<rgw_bucket_dir_entry *> el;
    for (auto eiter = el.begin(); eiter != el.end(); ++eiter) {
      rgw_bucket_dir_entry *e = *eiter;
      d->m[e->key.name] = *e;

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
  if (header) {
    header->dump(f);
  }
  f->close_section();
  auto iter = m.cbegin();
  f->open_array_section("map");
  for (; iter != m.cend(); ++iter) {
    f->dump_string("key", iter->first);
    f->open_object_section("dir_entry");
    iter->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void rgw_s3select_usage_data::generate_test_instances(list<rgw_s3select_usage_data*>& o)
{
  rgw_s3select_usage_data *s = new rgw_s3select_usage_data;
  s->bytes_processed = 1024;
  s->bytes_returned = 512;
  o.push_back(s);
  o.push_back(new rgw_s3select_usage_data);
}

void rgw_s3select_usage_data::dump(Formatter *f) const
{
  f->dump_unsigned("bytes_processed", bytes_processed);
  f->dump_unsigned("bytes_returned", bytes_returned);
}

void rgw_usage_data::generate_test_instances(list<rgw_usage_data*>& o)
{
  rgw_usage_data *s = new rgw_usage_data;
  s->bytes_sent = 1024;
  s->bytes_received = 1024;
  s->ops = 2;
  s->successful_ops = 1;
  o.push_back(s);
  o.push_back(new rgw_usage_data);
}

void rgw_usage_data::dump(Formatter *f) const
{
  f->dump_int("bytes_sent", bytes_sent);
  f->dump_int("bytes_received", bytes_received);
  f->dump_int("ops", ops);
  f->dump_int("successful_ops", successful_ops);
}

void rgw_usage_log_info::generate_test_instances(list<rgw_usage_log_info*>& o)
{
  rgw_usage_log_info *s = new rgw_usage_log_info;
  std::string owner = "owner";
  std::string payer = "payer";
  std::string bucket = "bucket";

  rgw_usage_log_entry r(owner, payer, bucket);
  s->entries.push_back(r);
  o.push_back(s);
  o.push_back(new rgw_usage_log_info);
}

void rgw_usage_log_info::dump(Formatter *f) const
{
  encode_json("entries", entries, f);
}

void rgw_user_bucket::generate_test_instances(list<rgw_user_bucket*>& o)
{
  rgw_user_bucket *s = new rgw_user_bucket;
  s->user = "user";
  s->bucket = "bucket";
  o.push_back(s);
  o.push_back(new rgw_user_bucket);
}

void rgw_user_bucket::dump(Formatter *f) const
{
  f->dump_string("user", user);
  f->dump_string("bucket", bucket);
}

void rgw_usage_log_entry::dump(Formatter *f) const
{
  f->dump_string("owner", owner.to_str());
  f->dump_string("payer", payer.to_str());
  f->dump_string("bucket", bucket);
  f->dump_unsigned("epoch", epoch);

  f->open_object_section("total_usage");
  f->dump_unsigned("bytes_sent", total_usage.bytes_sent);
  f->dump_unsigned("bytes_received", total_usage.bytes_received);
  f->dump_unsigned("ops", total_usage.ops);
  f->dump_unsigned("successful_ops", total_usage.successful_ops);
  f->close_section();

  f->open_array_section("categories");
  if (usage_map.size() > 0) {
    for (auto it = usage_map.begin(); it != usage_map.end(); it++) {
      const rgw_usage_data& total_usage = it->second;
      f->open_object_section("entry");
      f->dump_string("category", it->first.c_str());
      f->dump_unsigned("bytes_sent", total_usage.bytes_sent);
      f->dump_unsigned("bytes_received", total_usage.bytes_received);
      f->dump_unsigned("ops", total_usage.ops);
      f->dump_unsigned("successful_ops", total_usage.successful_ops);
      f->close_section();
    }
  }
  f->close_section();

  f->open_object_section("s3select");
  f->dump_unsigned("bytes_processed", s3select_usage.bytes_processed);
  f->dump_unsigned("bytes_returned", s3select_usage.bytes_returned);
  f->close_section();
}

void rgw_usage_log_entry::generate_test_instances(list<rgw_usage_log_entry *> &o)
{
  rgw_usage_log_entry *entry = new rgw_usage_log_entry;
  rgw_usage_data usage_data{1024, 2048};
  rgw_s3select_usage_data s3select_usage_data{8192, 4096};
  entry->owner = rgw_user("owner");
  entry->payer = rgw_user("payer");
  entry->bucket = "bucket";
  entry->epoch = 1234;
  entry->total_usage.bytes_sent = usage_data.bytes_sent;
  entry->total_usage.bytes_received = usage_data.bytes_received;
  entry->total_usage.ops = usage_data.ops;
  entry->total_usage.successful_ops = usage_data.successful_ops;
  entry->usage_map["get_obj"] = usage_data;
  entry->s3select_usage = s3select_usage_data;
  o.push_back(entry);
  o.push_back(new rgw_usage_log_entry);
}

std::string to_string(cls_rgw_reshard_initiator i) {
  switch (i) {
  case cls_rgw_reshard_initiator::Unknown:
    return "unknown";
  case cls_rgw_reshard_initiator::Admin:
    return "administrator";
  case cls_rgw_reshard_initiator::Dynamic:
    return "dynamic resharding";
  default:
    return "error";
  }
}

void cls_rgw_reshard_entry::generate_key(const string& tenant, const string& bucket_name, string *key)
{
  *key = tenant + ":" + bucket_name;
}

void cls_rgw_reshard_entry::get_key(string *key) const
{
  generate_key(tenant, bucket_name, key);
}

void cls_rgw_reshard_entry::dump(Formatter *f) const
{
  utime_t ut(time);
  encode_json("time", ut, f);
  encode_json("tenant", tenant, f);
  encode_json("bucket_name", bucket_name, f);
  encode_json("bucket_id", bucket_id, f);
  encode_json("old_num_shards", old_num_shards, f);
  encode_json("tentative_new_num_shards", new_num_shards, f);
  encode_json("initiator", to_string(initiator), f);
}

void cls_rgw_reshard_entry::generate_test_instances(list<cls_rgw_reshard_entry*>& ls)
{
  ls.push_back(new cls_rgw_reshard_entry);
  ls.push_back(new cls_rgw_reshard_entry);
  ls.back()->time = ceph::real_clock::from_ceph_timespec({ceph_le32(2), ceph_le32(3)});
  ls.back()->tenant = "tenant";
  ls.back()->bucket_name = "bucket1""";
  ls.back()->bucket_id = "bucket_id";
  ls.back()->old_num_shards = 8;
  ls.back()->new_num_shards = 64;
}

void cls_rgw_bucket_instance_entry::dump(Formatter *f) const
{
  encode_json("reshard_status", to_string(reshard_status), f);
}

void cls_rgw_bucket_instance_entry::generate_test_instances(
list<cls_rgw_bucket_instance_entry*>& ls)
{
  ls.push_back(new cls_rgw_bucket_instance_entry);
  ls.push_back(new cls_rgw_bucket_instance_entry);
  ls.back()->reshard_status = RESHARD_STATUS::IN_PROGRESS;
}

void cls_rgw_lc_entry::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("start_time", start_time, f);
  encode_json("status", status, f);
}

void cls_rgw_lc_entry::generate_test_instances(list<cls_rgw_lc_entry*>& o)
{
  cls_rgw_lc_entry *s = new cls_rgw_lc_entry;
  s->bucket = "bucket";
  s->start_time = 10;
  s->status = 1;
  o.push_back(s);
  o.push_back(new cls_rgw_lc_entry);
}

void cls_rgw_lc_obj_head::dump(Formatter *f) const 
{
  encode_json("start_date", start_date, f);
  encode_json("marker", marker, f);
}

void cls_rgw_lc_obj_head::generate_test_instances(list<cls_rgw_lc_obj_head*>& ls)
{
}

std::ostream& operator<<(std::ostream& out, cls_rgw_reshard_status status) {
  switch (status) {
  case cls_rgw_reshard_status::NOT_RESHARDING:
    out << "NOT_RESHARDING";
    break;
  case cls_rgw_reshard_status::IN_LOGRECORD:
    out << "IN_LOGRECORD";
    break;
  case cls_rgw_reshard_status::IN_PROGRESS:
    out << "IN_PROGRESS";
    break;
  case cls_rgw_reshard_status::DONE:
    out << "DONE";
    break;
  default:
    out << "UNKNOWN_STATUS";
  }

  return out;
}
