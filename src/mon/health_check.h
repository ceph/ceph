// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <map>

#include "include/health.h"
#include "include/utime.h"
#include "common/Formatter.h"

struct health_check_t {
  health_status_t severity;
  std::string summary;
  std::list<std::string> detail;
  int64_t count = 0;

  DENC(health_check_t, v, p) {
    DENC_START(2, 1, p);
    denc(v.severity, p);
    denc(v.summary, p);
    denc(v.detail, p);
    if (struct_v >= 2) {
      denc(v.count, p);
    }
    DENC_FINISH(p);
  }

  friend bool operator==(const health_check_t& l,
			 const health_check_t& r) {
    return l.severity == r.severity &&
      l.summary == r.summary &&
      l.detail == r.detail &&
      l.count == r.count;
  }
  friend bool operator!=(const health_check_t& l,
			 const health_check_t& r) {
    return !(l == r);
  }

  void dump(ceph::Formatter *f, bool want_detail=true) const {
    f->dump_stream("severity") << severity;

    f->open_object_section("summary");
    f->dump_string("message", summary);
    f->dump_int("count", count);
    f->close_section();

    if (want_detail) {
      f->open_array_section("detail");
      for (auto& p : detail) {
	f->open_object_section("detail_item");
	f->dump_string("message", p);
	f->close_section();
      }
      f->close_section();
    }
  }

  static void generate_test_instances(std::list<health_check_t*>& ls) {
    ls.push_back(new health_check_t);
    ls.back()->severity = HEALTH_WARN;
    ls.push_back(new health_check_t);
    ls.back()->severity = HEALTH_ERR;
    ls.back()->summary = "summarization";
    ls.back()->detail = {"one", "two", "three"};
    ls.back()->count = 42;
  }
};
WRITE_CLASS_DENC(health_check_t)


struct health_mute_t {
  std::string code;
  utime_t ttl;
  bool sticky = false;
  std::string summary;
  int64_t count;

  DENC(health_mute_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.code, p);
    denc(v.ttl, p);
    denc(v.sticky, p);
    denc(v.summary, p);
    denc(v.count, p);
    DENC_FINISH(p);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_string("code", code);
    if (ttl != utime_t()) {
      f->dump_stream("ttl") << ttl;
    }
    f->dump_bool("sticky", sticky);
    f->dump_string("summary", summary);
    f->dump_int("count", count);
  }

  static void generate_test_instances(std::list<health_mute_t*>& ls) {
    ls.push_back(new health_mute_t);
    ls.push_back(new health_mute_t);
    ls.back()->code = "OSD_DOWN";
    ls.back()->ttl = utime_t(1, 2);
    ls.back()->sticky = true;
    ls.back()->summary = "foo bar";
    ls.back()->count = 2;
  }
};
WRITE_CLASS_DENC(health_mute_t)

struct health_check_map_t {
  std::map<std::string,health_check_t> checks;

  DENC(health_check_map_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.checks, p);
    DENC_FINISH(p);
  }

  void dump(ceph::Formatter *f) const {
    for (auto& [code, check] : checks) {
      f->dump_object(code, check);
    }
  }

  static void generate_test_instances(std::list<health_check_map_t*>& ls) {
    ls.push_back(new health_check_map_t);
    ls.push_back(new health_check_map_t);
    {
      auto& d = ls.back()->add("FOO", HEALTH_WARN, "foo", 2);
      d.detail.push_back("a");
      d.detail.push_back("b");
    }
    {
      auto& d = ls.back()->add("BAR", HEALTH_ERR, "bar!", 3);
      d.detail.push_back("c");
      d.detail.push_back("d");
      d.detail.push_back("e");
    }
  }

  void clear() {
    checks.clear();
  }
  bool empty() const {
    return checks.empty();
  }
  void swap(health_check_map_t& other) {
    checks.swap(other.checks);
  }

  health_check_t& add(const std::string& code,
		      health_status_t severity,
		      const std::string& summary,
		      int64_t count) {
    ceph_assert(checks.count(code) == 0);
    health_check_t& r = checks[code];
    r.severity = severity;
    r.summary = summary;
    r.count = count;
    return r;
  }
  health_check_t& get_or_add(const std::string& code,
			     health_status_t severity,
			     const std::string& summary,
			     int64_t count) {
    health_check_t& r = checks[code];
    r.severity = severity;
    r.summary = summary;
    r.count += count;
    return r;
  }

  void merge(const health_check_map_t& o) {
    for (auto& [code, check] : o.checks) {
      auto [it, new_check] = checks.try_emplace(code, check);
      if (!new_check) {
        // merge details, and hope the summary matches!
        it->second.detail.insert(
          it->second.detail.end(),
          check.detail.begin(),
          check.detail.end());
        it->second.count += check.count;
      }
    }
  }

  friend bool operator==(const health_check_map_t& l,
			 const health_check_map_t& r) {
    return l.checks == r.checks;
  }
  friend bool operator!=(const health_check_map_t& l,
			 const health_check_map_t& r) {
    return !(l == r);
  }
};
WRITE_CLASS_DENC(health_check_map_t)
