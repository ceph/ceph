// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <string.h>

#include <algorithm>
#include <iostream>
#include <unordered_map>

#include "common/async/spawn_throttle.h"
#include "common/strtol.h" // for strict_strtoll()
#include "include/types.h"

#include "rgw_xml.h"
#include "rgw_multi_del.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

bool RGWMultiDelObject::xml_end(const char *el)
{
  RGWMultiDelKey *key_obj = static_cast<RGWMultiDelKey *>(find_first("Key"));
  RGWMultiDelVersionId *vid = static_cast<RGWMultiDelVersionId *>(find_first("VersionId"));
  XMLObj *etag_match = static_cast<XMLObj *>(find_first("ETag"));
  XMLObj *last_modified_time = static_cast<XMLObj *>(find_first("LastModifiedTime"));
  XMLObj *size = static_cast<XMLObj *>(find_first("Size"));

  if (!key_obj)
    return false;

  string s = key_obj->get_data();
  if (s.empty())
    return false;

  key = s;

  if (vid) {
    version_id = vid->get_data();
  }

  if (etag_match) {
    if_match = etag_match->get_data().c_str();
  }

  if(last_modified_time) {
    string last_modified_time_str = last_modified_time->get_data();
    if (last_modified_time_str.empty())
      return false;

    string last_modified_time_str_decoded = url_decode(last_modified_time_str);
    if (parse_time(last_modified_time_str_decoded.c_str(), &last_mod_time) < 0)
      return false;
  }

  if (size) {
    string err;
    long long size_tmp = strict_strtoll(size->get_data(), 10, &err);
    if (!err.empty()) {
      return false;
    }
    size_match = uint64_t(size_tmp);
  }

  return true;
}

bool RGWMultiDelDelete::xml_end(const char *el) {
  RGWMultiDelQuiet *quiet_set = static_cast<RGWMultiDelQuiet *>(find_first("Quiet"));
  if (quiet_set) {
    string quiet_val = quiet_set->get_data();
    quiet = (strcasecmp(quiet_val.c_str(), "true") == 0);
  }

  XMLObjIter iter = find("Object");
  RGWMultiDelObject *object = static_cast<RGWMultiDelObject *>(iter.get_next());
  while (object) {
    objects.push_back(*object);
    object = static_cast<RGWMultiDelObject *>(iter.get_next());
  }
  return true;
}

XMLObj *RGWMultiDelXMLParser::alloc_obj(const char *el) {
  XMLObj *obj = NULL;
  if (strcmp(el, "Delete") == 0) {
    obj = new RGWMultiDelDelete();
  } else if (strcmp(el, "Quiet") == 0) {
    obj = new RGWMultiDelQuiet();
  } else if (strcmp(el, "Object") == 0) {
    obj = new RGWMultiDelObject ();
  } else if (strcmp(el, "Key") == 0) {
    obj = new RGWMultiDelKey();
  } else if (strcmp(el, "VersionId") == 0) {
    obj = new RGWMultiDelVersionId();
  }

  return obj;
}

void rgw::multi_delete::dispatch(const std::vector<Item>& items,
                                 bool bucket_versioned,
                                 uint32_t max_aio,
                                 boost::asio::yield_context yield,
                                 Exec exec,
                                 OnDispatch on_dispatch)
{
  auto group = ceph::async::spawn_throttle{yield, std::max<uint32_t>(1, max_aio)};

  if (!bucket_versioned) {
    for (size_t i = 0; i < items.size(); ++i) {
      group.spawn([&exec, &items, i] (boost::asio::yield_context y) {
        exec(items[i], false, y);
      });
      if (on_dispatch) {
        on_dispatch();
      }
    }
    group.wait();
    return;
  }

  // Preserve first-seen order within each key group so callers can keep
  // request/result ordering stable while coalescing intermediate OLH updates.
  std::vector<std::vector<size_t>> grouped_items;
  grouped_items.reserve(items.size());
  std::unordered_map<std::string, size_t> group_index;
  group_index.reserve(items.size());

  for (size_t i = 0; i < items.size(); ++i) {
    const auto& name = items[i].key.name;
    auto [it, inserted] = group_index.emplace(name, grouped_items.size());
    if (inserted) {
      grouped_items.emplace_back();
    }
    grouped_items[it->second].push_back(i);
  }

  for (const auto& indexes : grouped_items) {
    for (size_t i = 0; i + 1 < indexes.size(); ++i) {
      const auto index = indexes[i];
      group.spawn([&exec, &items, index] (boost::asio::yield_context y) {
        exec(items[index], true, y);
      });
      if (on_dispatch) {
        on_dispatch();
      }
    }
  }
  group.wait();

  for (const auto& indexes : grouped_items) {
    const auto index = indexes.back();
    group.spawn([&exec, &items, index] (boost::asio::yield_context y) {
      exec(items[index], false, y);
    });
    if (on_dispatch) {
      on_dispatch();
    }
  }
  group.wait();
}
