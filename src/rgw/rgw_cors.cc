// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include <string.h>

#include <iostream>
#include <map>

#include <boost/algorithm/string.hpp> 

#include "include/types.h"
#include "common/debug.h"
#include "include/str_list.h"
#include "common/Formatter.h"

#include "rgw_cors.h"

#define dout_subsys ceph_subsys_rgw
using namespace std;

void RGWCORSRule::dump_origins() {
  unsigned num_origins = allowed_origins.size();
  dout(10) << "Allowed origins : " << num_origins << dendl;
  for(set<string>::iterator it = allowed_origins.begin();
      it != allowed_origins.end(); 
      ++it) {
    dout(10) << *it << "," << dendl;
  }
}

void RGWCORSRule::erase_origin_if_present(string& origin, bool *rule_empty) {
  set<string>::iterator it = allowed_origins.find(origin);
  if (!rule_empty)
    return;
  *rule_empty = false;
  if (it != allowed_origins.end()) {
    dout(10) << "Found origin " << origin << ", set size:" << 
        allowed_origins.size() << dendl;
    allowed_origins.erase(it);
    *rule_empty = (allowed_origins.empty());
  }
}

static bool is_string_in_set(set<string>& s, string h) {
  if ((s.find("*") != s.end()) || 
          (s.find(h) != s.end())) {
    return true;
  }
  /* The header can be Content-*-type, or Content-* */
  for(set<string>::iterator it = s.begin();
      it != s.end(); ++it) {
    size_t off;
    if ((off = (*it).find("*"))!=string::npos) {
      list<string> ssplit;
      unsigned flen = 0;
      
      get_str_list((*it), "* \t", ssplit);
      if (off != 0) {
        string sl = ssplit.front();
        flen = sl.length();
        dout(10) << "Finding " << sl << ", in " << h << ", at offset 0" << dendl;
        if (!boost::algorithm::starts_with(h,sl))
          continue;
        ssplit.pop_front();
      }
      if (off != ((*it).length() - 1)) {
        string sl = ssplit.front();
        dout(10) << "Finding " << sl << ", in " << h 
          << ", at offset not less than " << flen << dendl;
        if (h.compare((h.size() - sl.size()), sl.size(), sl) != 0)
          continue;
        ssplit.pop_front();
      }
      if (!ssplit.empty())
        continue;
      return true;
    }
  }
  return false;
}

bool RGWCORSRule::is_origin_present(const char *o) {
  string origin = o;
  return is_string_in_set(allowed_origins, origin);
}

bool RGWCORSRule::is_header_allowed(const char *h, size_t len) {
  string hdr(h, len);
  return is_string_in_set(allowed_hdrs, hdr);
}

void RGWCORSRule::format_exp_headers(string& s) {
  s = "";
  for(list<string>::iterator it = exposable_hdrs.begin();
      it != exposable_hdrs.end(); ++it) {
      if (s.length() > 0)
        s.append(",");
      s.append((*it));
  }
}

RGWCORSRule * RGWCORSConfiguration::host_name_rule(const char *origin) {
  for(list<RGWCORSRule>::iterator it_r = rules.begin(); 
      it_r != rules.end(); ++it_r) {
    RGWCORSRule& r = (*it_r);
    if (r.is_origin_present(origin))
      return &r;
  }
  return NULL;
}

void RGWCORSConfiguration::erase_host_name_rule(string& origin) {
  bool rule_empty;
  unsigned loop = 0;
  /*Erase the host name from that rule*/
  dout(10) << "Num of rules : " << rules.size() << dendl;
  for(list<RGWCORSRule>::iterator it_r = rules.begin(); 
      it_r != rules.end(); ++it_r, loop++) {
    RGWCORSRule& r = (*it_r);
    r.erase_origin_if_present(origin, &rule_empty);
    dout(10) << "Origin:" << origin << ", rule num:" 
      << loop << ", emptying now:" << rule_empty << dendl;
    if (rule_empty) {
      rules.erase(it_r);
      break;
    }
  }
}

void RGWCORSConfiguration::dump() {
  unsigned loop = 1;
  unsigned num_rules = rules.size();
  dout(10) << "Number of rules: " << num_rules << dendl;
  for(list<RGWCORSRule>::iterator it = rules.begin();
      it!= rules.end(); ++it, loop++) {
    dout(10) << " <<<<<<< Rule " << loop << " >>>>>>> " << dendl;
    (*it).dump_origins();
  }
}
