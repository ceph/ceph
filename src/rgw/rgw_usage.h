// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_USAGE_H
#define CEPH_RGW_USAGE_H

#include <string>
#include <map>
#include <unordered_set>

#include "common/Formatter.h"

#include "rgw_basic_types.h"
#include "rgw_formats.h"

class RGWRados;


class RGWUsage
{
public:
  static int show(RGWRados *store, const rgw_user& uid,
		  const std::string& bucket_name, uint64_t start_epoch,
		  uint64_t end_epoch, bool show_log_entries, bool show_log_sum,
		  std::unordered_set<std::string_view,
		                     std::hash<std::string_view>,
		                     std::equal_to<>> *categories,
		  RGWFormatterFlusher& flusher);

  static int trim(RGWRados *store, const rgw_user& uid, const string& bucket_name, uint64_t start_epoch,
	          uint64_t end_epoch);

  static int clear(RGWRados *store);
};


#endif
