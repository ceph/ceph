// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "common/ceph_time.h"
#include "common/dout.h"
#include "common/Formatter.h"

namespace rgw { namespace sal { namespace nsfs {

/* What adoption found, written by each adoption pass to a file of its
 * own (DECISIONS.md 2026-10-30, amended 2026-11-01).
 *
 * A finding is its kind and the text the code that found it produced,
 * which names its subject.  What a kind does to the conversion and the
 * course of action proposed for it are fixed per kind, in kind_text(),
 * so every finding of one kind proposes the same. */
enum class FindingKind {
  store_unreadable,
  store_version,
  ldap_configured,
  collision,
  identity_failed,
  limit_raised,
  root_widened,
  user_drift,
  unresolved_name,
  unresolved_uid,
  trust_narrowed,
  bucket_failed,
  policy_refused,
  notification_not_carried,
  notification_needs_operator,
  links_not_served,
  abandoned,
};

struct FindingKindText {
  const char* name;
  const char* effect;
  const char* action;
};

const FindingKindText& kind_text(FindingKind kind);

struct Finding {
  FindingKind kind;
  std::string text;
};

/* The findings gathered since the last report was written.  Only
 * `automatic` adoption gathers:  a finding is kept only on a thread
 * holding a Gathering, which the queue's start, its workers and the
 * synchronous startup hold;  a command's findings are its log and its
 * response's, where the operator driving it reads.  The queue's
 * workers add concurrently. */
class AdoptionFindings {
public:
  class Gathering {
  public:
    Gathering();
    ~Gathering();
    Gathering(const Gathering&) = delete;
    Gathering& operator=(const Gathering&) = delete;
  private:
    bool was;
  };

  void add(FindingKind kind, std::string text);
  void add(FindingKind kind, const std::vector<std::string>& texts);
  /* Whether a finding of `kind` begins with `prefix`. */
  bool contains(FindingKind kind, const std::string& prefix);
  /* Drop the findings of `kind` that begin with `prefix`. */
  void remove(FindingKind kind, const std::string& prefix);
  /* What was gathered and when gathering began;  starts again empty. */
  std::vector<Finding> take(ceph::real_time& since);

private:
  void add_locked(FindingKind kind, const std::string& text);

  std::mutex mtx;
  ceph::real_time since{ceph::real_clock::now()};
  std::vector<Finding> findings;
};

struct AdoptionReport {
  /* "run" (the synchronous `automatic` startup) or "queue" (a drain
   * of the queue, or its start failing) */
  std::string pass;
  /* $cluster-$name */
  std::string gateway;
  std::string store;
  ceph::real_time since;
  ceph::real_time ended;
  bool complete{false};
  std::string why_not_complete;
  /* what the pass's caller is returned, where it has a caller */
  std::map<std::string, std::string> counts;
  std::vector<Finding> findings;
};

void dump_adoption_report(const AdoptionReport& r, ceph::Formatter* f);

/* Write `r` into `dir` as nsfs-adoption-<gateway>-<ended, UTC>.json.
 * Returns 0 and the file's path, or a negative errno. */
int write_adoption_report(const DoutPrefixProvider* dpp,
                          const std::string& dir, const AdoptionReport& r,
                          std::string& path);

} } } // namespace rgw::sal::nsfs
