// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "common/config.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "rgw_sal_rados.h"

class RGWRadosList {

  /*
   * process_t describes how to process a irectory, we will either
   * process the whole thing (entire_container == true) or a portion
   * of it (entire_container == false). When we only process a
   * portion, we will list the specific keys and/or specific lexical
   * prefixes.
   */
  struct process_t {
    bool entire_container;
    std::set<rgw_obj_key> filter_keys;
    std::set<std::string> prefixes;

    process_t() :
      entire_container(false)
    {}
  };

  std::map<std::string,process_t> bucket_process_map;
  std::set<std::string> visited_oids;

  void add_bucket_entire(const std::string& bucket_name) {
    auto p = bucket_process_map.emplace(std::make_pair(bucket_name,
						       process_t()));
    p.first->second.entire_container = true;
  }

  void add_bucket_prefix(const std::string& bucket_name,
			 const std::string& prefix) {
    auto p = bucket_process_map.emplace(std::make_pair(bucket_name,
						       process_t()));
    p.first->second.prefixes.insert(prefix);
  }

  void add_bucket_filter(const std::string& bucket_name,
			 const rgw_obj_key& obj_key) {
    auto p = bucket_process_map.emplace(std::make_pair(bucket_name,
						       process_t()));
    p.first->second.filter_keys.insert(obj_key);
  }

  rgw::sal::RadosStore* store;

  uint16_t max_concurrent_ios;
  uint64_t stale_secs;
  std::string tenant_name;

  bool include_rgw_obj_name;
  std::string field_separator;

  int handle_stat_result(const DoutPrefixProvider *dpp,
			 RGWRados::Object::Stat::Result& result,
			 const std::string& bucket_name,
			 const rgw_obj_key& obj_key,
			 std::set<std::string>& obj_oids);
  int pop_and_handle_stat_op(const DoutPrefixProvider *dpp,
                             RGWObjectCtx& obj_ctx,
			     std::deque<RGWRados::Object::Stat>& ops);

public:

  RGWRadosList(rgw::sal::RadosStore* _store,
	       int _max_ios,
	       uint64_t _stale_secs,
	       const std::string& _tenant_name) :
    store(_store),
    max_concurrent_ios(_max_ios),
    stale_secs(_stale_secs),
    tenant_name(_tenant_name),
    include_rgw_obj_name(false)
  {}

  int process_bucket(const DoutPrefixProvider *dpp,
                     const std::string& bucket_instance_id,
		     const std::string& prefix,
		     const std::set<rgw_obj_key>& entries_filter);

  int do_incomplete_multipart(const DoutPrefixProvider *dpp,
			      rgw::sal::Bucket* bucket);

  int build_linked_oids_index();

  int run(const DoutPrefixProvider *dpp,
	  const std::string& bucket_id,
	  const bool silent_indexless = false);
  int run(const DoutPrefixProvider *dpp,
	  const bool yes_i_really_mean_it = false);

  // if there's a non-empty field separator, that means we'll display
  // bucket and object names
  void set_field_separator(const std::string& fs) {
    field_separator = fs;
    include_rgw_obj_name = !field_separator.empty();
  }
}; // class RGWRadosList
