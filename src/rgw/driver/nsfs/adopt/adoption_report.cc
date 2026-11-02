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

#include "adoption_report.h"

#include <algorithm>
#include <ctime>

#include "common/errno.h"
#include "common/JSONFormatter.h"
#include "common/iso_8601.h"
#include "common/safe_io.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

namespace {

/* In FindingKind's order. */
const FindingKindText kind_texts[] = {
  {"store_unreadable",
   "the record is not imported, and adoption cannot complete",
   "repair or remove the file, then run adoption again"},
  {"store_version",
   "adoption does not run on the NooBaa version that wrote the store",
   "upgrade NooBaa, or adopt from a store a supported version wrote"},
  {"ldap_configured",
   "NooBaa's LDAP-backed AssumeRoleWithWebIdentity is not carried:  a "
   "client holding an LDAP web identity token cannot get a session",
   "issue those clients tokens from an OIDC provider registered with RGW"},
  {"collision",
   "nothing is imported, and the pass stops",
   "remove one of the records that share the _id, then run adoption "
   "again"},
  {"identity_failed",
   "the identity or role is not written, and adoption cannot complete",
   "fix what the finding names and run adoption again, or abandon the "
   "record"},
  {"limit_raised",
   "the account's limit is above RGW's default",
   "none needed"},
  {"root_widened",
   "the account's root may bypass governance retention, which NooBaa "
   "refused it",
   "deny s3:BypassGovernanceRetention to the root by policy, if NooBaa's "
   "restriction should stand"},
  {"user_drift",
   "the IAM user runs as its account's POSIX identity, not its own",
   "set rgw_nsfs_user_identity_override, if it should run as its own"},
  {"unresolved_name",
   "the identity is refused until nsswitch resolves its name, as under "
   "NooBaa",
   "make the name resolve"},
  {"unresolved_uid",
   "the identity is refused until nsswitch knows its uid, as under NooBaa",
   "make the uid resolve"},
  {"trust_narrowed",
   "IAM users NooBaa's STS admitted by name can no longer assume the role",
   "name each such user in the trust policy by its ARN, if it should "
   "assume the role"},
  {"role_config_superseded",
   "the account's role_config role is not imported;  the ROLE record of "
   "its name is imported in its place",
   "none needed;  NooBaa's own migration does the same"},
  {"bucket_failed",
   "the bucket is not adopted, and adoption cannot complete",
   "fix what the finding names and run adoption again, or abandon the "
   "bucket"},
  {"policy_refused",
   "the bucket is served without its policy:  its grants do not apply, "
   "and its denials do not refuse",
   "write the policy with RGW principals (PutBucketPolicy)"},
  {"arn_matched",
   "the statement now applies to the identity the ARN names, which "
   "NooBaa NC 5.23 did not match in a bucket policy, nor at 5.23.0 in a "
   "trust policy",
   "none needed, if the statement should apply;  otherwise remove the "
   "principal"},
  {"notification_not_carried",
   "no event is sent for it",
   "recreate it with RGW's notification API"},
  {"notification_needs_operator",
   "it is carried, but it may not deliver, or it sends a credential in "
   "cleartext",
   "change the setting the finding names"},
  {"links_not_served",
   "the symbolic links NooBaa served are not served",
   "replace each link with the object it named, if it should still be "
   "served"},
  {"abandoned",
   "the record is frozen and readable, and not converted",
   "take the abandonment back to convert it"},
};

static_assert(std::size(kind_texts) ==
              static_cast<size_t>(FindingKind::abandoned) + 1);

/* 20261101T170219.123456Z:  a pass on one gateway is not two in one
 * microsecond */
std::string file_stamp(ceph::real_time t)
{
  const auto ts = ceph::real_clock::to_timespec(t);
  struct tm tm;
  gmtime_r(&ts.tv_sec, &tm);
  char buf[32];
  const size_t n = strftime(buf, sizeof(buf), "%Y%m%dT%H%M%S", &tm);
  snprintf(buf + n, sizeof(buf) - n, ".%06ldZ", ts.tv_nsec / 1000);
  return buf;
}

} // namespace

const FindingKindText& kind_text(FindingKind kind)
{
  return kind_texts[static_cast<size_t>(kind)];
}

static thread_local bool tl_gathering = false;

AdoptionFindings::Gathering::Gathering() : was(tl_gathering)
{
  tl_gathering = true;
}

AdoptionFindings::Gathering::~Gathering()
{
  tl_gathering = was;
}

/* Once each:  the identities are imported again by every pass, and
 * find what they found before. */
void AdoptionFindings::add_locked(FindingKind kind, const std::string& text)
{
  if (!tl_gathering) {
    return;
  }
  if (std::none_of(findings.begin(), findings.end(),
                   [&](const Finding& f) {
                     return f.kind == kind && f.text == text;
                   })) {
    findings.push_back({kind, text});
  }
}

void AdoptionFindings::add(FindingKind kind, std::string text)
{
  std::lock_guard l{mtx};
  add_locked(kind, text);
}

void AdoptionFindings::add(FindingKind kind,
                           const std::vector<std::string>& texts)
{
  std::lock_guard l{mtx};
  for (const auto& t : texts) {
    add_locked(kind, t);
  }
}

bool AdoptionFindings::contains(FindingKind kind, const std::string& prefix)
{
  std::lock_guard l{mtx};
  return std::any_of(findings.begin(), findings.end(),
                     [&](const Finding& f) {
                       return f.kind == kind && f.text.starts_with(prefix);
                     });
}

void AdoptionFindings::remove(FindingKind kind, const std::string& prefix)
{
  std::lock_guard l{mtx};
  std::erase_if(findings, [&](const Finding& f) {
    return f.kind == kind && f.text.starts_with(prefix);
  });
}

std::vector<Finding> AdoptionFindings::take(ceph::real_time& out_since)
{
  std::lock_guard l{mtx};
  out_since = since;
  since = ceph::real_clock::now();
  return std::exchange(findings, {});
}

void dump_adoption_report(const AdoptionReport& r, ceph::Formatter* f)
{
  f->open_object_section("adoption_report");
  f->dump_string("pass", r.pass);
  f->dump_string("gateway", r.gateway);
  f->dump_string("store", r.store);
  f->dump_string("since", to_iso_8601(r.since));
  f->dump_string("ended", to_iso_8601(r.ended));
  f->dump_bool("complete", r.complete);
  if (!r.complete) {
    f->dump_string("why_not_complete", r.why_not_complete);
  }
  f->open_object_section("counts");
  for (const auto& [k, v] : r.counts) {
    f->dump_string(k, v);
  }
  f->close_section();
  f->open_array_section("findings");
  for (const auto& fd : r.findings) {
    const auto& t = kind_text(fd.kind);
    f->open_object_section("finding");
    f->dump_string("kind", t.name);
    f->dump_string("text", fd.text);
    f->dump_string("effect", t.effect);
    f->dump_string("action", t.action);
    f->close_section();
  }
  f->close_section();
  f->close_section();
}

int write_adoption_report(const DoutPrefixProvider* dpp,
                          const std::string& dir, const AdoptionReport& r,
                          std::string& path)
{
  ceph::JSONFormatter f(true);
  dump_adoption_report(r, &f);
  bufferlist bl;
  f.flush(bl);

  const std::string name =
    "nsfs-adoption-" + r.gateway + "-" + file_stamp(r.ended) + ".json";
  path = dir + "/" + name;
  const int ret = safe_write_file(dir.c_str(), name.c_str(), bl.c_str(),
                                  bl.length(), 0640);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: adoption: writing the report " << path
      << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ldpp_dout(dpp, 1) << "nsfs: adoption report:  " << path << ":  "
    << r.findings.size() << " findings" << dendl;
  return 0;
}

} } } // namespace rgw::sal::nsfs
