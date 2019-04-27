// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <map>

#include "include/health.h"
#include "common/Formatter.h"

struct health_check_t {
  health_status_t severity;
  std::string summary;
  std::list<std::string> detail;

  DENC(health_check_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.severity, p);
    denc(v.summary, p);
    denc(v.detail, p);
    DENC_FINISH(p);
  }

  friend bool operator==(const health_check_t& l,
			 const health_check_t& r) {
    return l.severity == r.severity &&
      l.summary == r.summary &&
      l.detail == r.detail;
  }
  friend bool operator!=(const health_check_t& l,
			 const health_check_t& r) {
    return !(l == r);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_stream("severity") << severity;

    f->open_object_section("summary");
    f->dump_string("message", summary);
    f->close_section();

    f->open_array_section("detail");
    for (auto& p : detail) {
      f->open_object_section("detail_item");
      f->dump_string("message", p);
      f->close_section();
    }
    f->close_section();
  }

  static void generate_test_instances(std::list<health_check_t*>& ls) {
    ls.push_back(new health_check_t);
    ls.push_back(new health_check_t);
    ls.back()->severity = HEALTH_ERR;
    ls.back()->summary = "summarization";
    ls.back()->detail = {"one", "two", "three"};
  }
};
WRITE_CLASS_DENC(health_check_t)


struct health_check_map_t {
  std::map<std::string,health_check_t> checks;

  DENC(health_check_map_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.checks, p);
    DENC_FINISH(p);
  }

  void dump(ceph::Formatter *f) const {
    for (auto& p : checks) {
      f->dump_object(p.first.c_str(), p.second);
    }
  }

  static void generate_test_instances(std::list<health_check_map_t*>& ls) {
    ls.push_back(new health_check_map_t);
    ls.push_back(new health_check_map_t);
    {
      auto& d = ls.back()->add("FOO", HEALTH_WARN, "foo");
      d.detail.push_back("a");
      d.detail.push_back("b");
    }
    {
      auto& d = ls.back()->add("BAR", HEALTH_ERR, "bar!");
      d.detail.push_back("c");
      d.detail.push_back("d");
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
		      const std::string& summary) {
    ceph_assert(checks.count(code) == 0);
    health_check_t& r = checks[code];
    r.severity = severity;
    r.summary = summary;
    return r;
  }
  health_check_t& get_or_add(const std::string& code,
			     health_status_t severity,
			     const std::string& summary) {
    health_check_t& r = checks[code];
    r.severity = severity;
    r.summary = summary;
    return r;
  }

  void merge(const health_check_map_t& o) {
    for (auto& p : o.checks) {
      auto q = checks.find(p.first);
      if (q == checks.end()) {
	// new check
	checks[p.first] = p.second;
      } else {
	// merge details, and hope the summary matches!
	q->second.detail.insert(
	  q->second.detail.end(),
	  p.second.detail.begin(),
	  p.second.detail.end());
      }
    }
  }

  health_status_t dump_summary(ceph::Formatter *f, std::string *plain,
			       const char *sep, bool detail) const {
    health_status_t r = HEALTH_OK;
    for (auto& p : checks) {
      if (r > p.second.severity) {
	r = p.second.severity;
      }
      if (f) {
	f->open_object_section(p.first.c_str());
	f->dump_stream("severity") << p.second.severity;

        f->open_object_section("summary");
        f->dump_string("message", p.second.summary);
        f->close_section();

	if (detail) {
	  f->open_array_section("detail");
	  for (auto& d : p.second.detail) {
            f->open_object_section("detail_item");
            f->dump_string("message", d);
            f->close_section();
	  }
	  f->close_section();
	}
	f->close_section();
      } else {
	if (!plain->empty()) {
	  *plain += sep;
	}
	*plain += p.second.summary;
      }
    }
    return r;
  }

  void dump_summary_compat(ceph::Formatter *f) const {
    for (auto& p : checks) {
      f->open_object_section("item");
      f->dump_stream("severity") << p.second.severity;
      f->dump_string("summary", p.second.summary);
      f->close_section();
    }
  }

  void dump_detail(std::string *plain) const {
    for (auto& p : checks) {
      *plain += p.first + " " + p.second.summary + "\n";
      for (auto& d : p.second.detail) {
        *plain += "    ";
        *plain += d;
        *plain += "\n";
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
