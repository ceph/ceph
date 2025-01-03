// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <map>

#include "common/Formatter.h"
#include "common/dout.h"
#include "rgw_formats.h"
#include "rgw_user.h"
#include "rgw_sal_fwd.h"


class RGWUsage
{
public:
  static int show(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
		  rgw::sal::User* user , rgw::sal::Bucket* bucket,
		  uint64_t start_epoch, uint64_t end_epoch, bool show_log_entries,
		  bool show_log_sum,
		  std::map<std::string, bool> *categories, RGWFormatterFlusher& flusher);

  static int trim(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
		  rgw::sal::User* user , rgw::sal::Bucket* bucket,
		  uint64_t start_epoch, uint64_t end_epoch, optional_yield y);

  static int clear(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, optional_yield y);
};
