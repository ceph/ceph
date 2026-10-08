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

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "include/scope_guard.h"

#include "noobaa_config_store.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

/* Their record whole, in one read.  No iostreams and no incremental
 * parse:  a record is small, and a cap keeps a corrupt or hostile file
 * from being read into memory whole. */
static constexpr size_t NB_RECORD_MAX = 1u << 20;

int read_record_file(const DoutPrefixProvider* dpp, const std::string& path,
		     std::string& out)
{
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return -errno;
  }
  auto close_fd = make_scope_guard([fd] { ::close(fd); });

  out.clear();
  char buf[8192];
  for (;;) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n < 0) {
      return -errno;
    }
    if (n == 0) {
      break;
    }
    if ((out.size() + n) > NB_RECORD_MAX) {
      ldpp_dout(dpp, 0) << "ERROR: " << path << " exceeds "
	<< NB_RECORD_MAX << " bytes" << dendl;
      return -EFBIG;
    }
    out.append(buf, n);
  }
  return 0;
}

size_t NooBaaStoreContents::accounts() const
{
  return std::count_if(identities.begin(), identities.end(),
		       [](const NooBaaIdentity& i) { return !i.is_iam_user(); });
}

size_t NooBaaStoreContents::iam_users() const
{
  return std::count_if(identities.begin(), identities.end(),
		       [](const NooBaaIdentity& i) { return i.is_iam_user(); });
}

std::vector<std::string> NooBaaStoreContents::md5_etag_divergence() const
{
  /* What the accounts say.  A deployment is uniform when every
   * account agrees, and a bucket diverges when it states something
   * else -- including stating the same thing an account left unsaid,
   * since `false` and unsaid inherit differently. */
  std::optional<bool> account_says;
  bool accounts_differ = false;
  for (const auto& id : identities) {
    if (!id.have_force_md5_etag) {
      continue;
    }
    if (!account_says) {
      account_says = id.force_md5_etag;
    } else if (*account_says != id.force_md5_etag) {
      accounts_differ = true;
    }
  }

  std::vector<std::string> out;
  if (accounts_differ) {
    out.push_back("the accounts do not agree on force_md5_etag");
  }
  for (const auto& b : buckets) {
    if (!b.force_md5_etag) {
      continue;
    }
    if (account_says && (*b.force_md5_etag == *account_says)) {
      continue;
    }
    out.push_back(b.name + " sets force_md5_etag=" +
		  (*b.force_md5_etag ? "true" : "false") +
		  (account_says
		     ? std::string(", against the account's ") +
		       (*account_says ? "true" : "false")
		     : std::string(", which no account states")));
  }
  return out;
}

namespace {

/* Sorted names of the entries under `dir`, with `.` and `..` dropped.
 *
 * Sorted because an import that reports what it found should report it
 * the same way twice;  readdir order is the filesystem's.  Serial, with
 * no concurrency anywhere in this file -- their own config-directory
 * restructure is the only prior art for walking one of these trees and
 * it is serial (ACCOUNT_IMPORT.md §6.4). */
int list_dir(const std::string& dir, std::vector<std::string>& out)
{
  DIR* d = ::opendir(dir.c_str());
  if (!d) {
    return -errno;
  }
  auto close_dir = make_scope_guard([d] { ::closedir(d); });

  out.clear();
  for (;;) {
    errno = 0;
    struct dirent* e = ::readdir(d);
    if (!e) {
      if (errno != 0) {
	return -errno;
      }
      break;
    }
    const std::string name(e->d_name);
    if ((name == ".") || (name == "..")) {
      continue;
    }
    out.push_back(name);
  }
  std::sort(out.begin(), out.end());
  return 0;
}

bool path_exists(const std::string& path)
{
  struct stat st;
  return ::stat(path.c_str(), &st) == 0;
}

/* Decode one identity out of a parsed record.
 *
 * Throws `JSONDecoder::err` on a value of the wrong shape, which the
 * caller turns into an `unreadable` entry.  Caught at the boundary
 * rather than per field so that a field added here later is covered by
 * having been written inside it. */
void decode_identity(JSONObj* obj, NooBaaIdentity& id)
{
  JSONDecoder::decode_json("_id", id.id, obj);
  JSONDecoder::decode_json("name", id.name, obj);
  JSONDecoder::decode_json("email", id.email, obj);
  JSONDecoder::decode_json("creation_date", id.creation_date, obj);
  JSONDecoder::decode_json("owner", id.owner, obj);
  JSONDecoder::decode_json("creator", id.creator, obj);
  JSONDecoder::decode_json("iam_path", id.iam_path, obj);
  JSONDecoder::decode_json("master_key_id", id.master_key_id, obj);
  JSONDecoder::decode_json("allow_bucket_creation", id.allow_bucket_creation, obj);
  JSONDecoder::decode_json("iam_operate_on_root_account",
			   id.iam_operate_on_root_account, obj);

  /* Presence, not value.  A record that does not mention the flag must
   * not read as disabling it:  `rgw_non_md5_etag` is the
   * deployment-wide control and X1 carries this without enforcing it
   * (ACCOUNT_IMPORT.md §6.1a). */
  if (JSONObj* f = obj->find_obj("force_md5_etag")) {
    id.have_force_md5_etag = true;
    JSONDecoder::decode_json("force_md5_etag", id.force_md5_etag, obj);
    (void)f;
  }

  if (JSONObj* keys = obj->find_obj("access_keys")) {
    for (auto i = keys->find_first(); !i.end(); ++i) {
      NooBaaAccessKey k;
      JSONDecoder::decode_json("access_key", k.access_key, *i);
      JSONDecoder::decode_json("encrypted_secret_key",
			       k.encrypted_secret_key, *i);
      JSONDecoder::decode_json("creation_date", k.creation_date, *i);
      JSONDecoder::decode_json("deactivated", k.deactivated, *i);
      id.access_keys.push_back(std::move(k));
    }
  }

  if (JSONObj* nc = obj->find_obj("nsfs_account_config")) {
    NooBaaPosixConfig& p = id.posix;

    /* Their schema is a `oneOf`:  uid and gid, or a distinguished
     * name, never both.  The two behave differently -- a
     * directory-backed identity resolves its groups from the directory
     * and a local one does not -- so which it was is kept rather than
     * flattened (§2.3). */
    if (nc->find_obj("distinguished_name")) {
      p.kind = NooBaaPosixConfig::Kind::DistinguishedName;
      JSONDecoder::decode_json("distinguished_name",
			       p.distinguished_name, nc);
    } else if (nc->find_obj("uid")) {
      p.kind = NooBaaPosixConfig::Kind::Ids;
      JSONDecoder::decode_json("uid", p.uid, nc);
      JSONDecoder::decode_json("gid", p.gid, nc);
    }

    JSONDecoder::decode_json("new_buckets_path", p.new_buckets_path, nc);
    JSONDecoder::decode_json("fs_backend", p.fs_backend, nc);
    JSONDecoder::decode_json("custom_bucket_path_allowed_list",
			     p.custom_bucket_path_allowed_list, nc);
    JSONDecoder::decode_json("allow_bypass_governance",
			     p.allow_bypass_governance, nc);

    /* An explicit list overrides the directory entirely, and an empty
     * list is not the same as no list:  empty means no supplementary
     * groups, which is a grant decision rather than a default.  So
     * presence is recorded separately from contents (§2.4). */
    if (nc->find_obj("supplemental_groups")) {
      p.have_supplemental_groups = true;
      JSONDecoder::decode_json("supplemental_groups",
			       p.supplemental_groups, nc);
    }
  }

  /* Kept as text.  Turning a trust policy into an RGW role is I4;  I1
   * only has to not lose it. */
  if (JSONObj* rc = obj->find_obj("role_config")) {
    id.role_config = rc->get_data();
  }
}

} // namespace

int NooBaaConfigStore::read(const DoutPrefixProvider* dpp,
			    NooBaaStoreContents& out) const
{
  if (root.empty()) {
    return -ENOENT;
  }
  struct stat st;
  if (::stat(root.c_str(), &st) != 0) {
    return -errno;
  }
  if (!S_ISDIR(st.st_mode)) {
    return -ENOTDIR;
  }

  auto unreadable = [&out](const std::string& path, std::string why) {
    out.unreadable.push_back(NooBaaUnreadable{path, std::move(why)});
  };

  /* config.json.  Read first, because what it says changes how every
   * identity below it behaves and a reader of the report should see it
   * before the identities it qualifies. */
  {
    const std::string path = root + "/config.json";
    std::string doc;
    int ret = read_record_file(dpp, path, doc);
    if (ret == 0) {
      out.settings.have_config_json = true;
      JSONParser p;
      if (!p.parse(doc.data(), doc.size())) {
	unreadable(path, "not JSON");
      } else {
	if (p.find_obj("NSFS_ENABLE_DYNAMIC_SUPPLEMENTAL_GROUPS")) {
	  try {
	    JSONDecoder::decode_json("NSFS_ENABLE_DYNAMIC_SUPPLEMENTAL_GROUPS",
				     out.settings.dynamic_supplemental_groups,
				     &p);
	    out.settings.have_dynamic_supplemental_groups = true;
	  } catch (const JSONDecoder::err& e) {
	    unreadable(path, std::string("NSFS_ENABLE_DYNAMIC_SUPPLEMENTAL_"
					 "GROUPS: ") + e.what());
	  }
	}
	/* Where the master keys live.  Absent means their defaults --
	 * `file`, and the file beside config.json. */
	try {
	  JSONDecoder::decode_json("NC_MASTER_KEYS_STORE_TYPE",
				   out.settings.master_keys_store_type, &p);
	  JSONDecoder::decode_json("NC_MASTER_KEYS_FILE_LOCATION",
				   out.settings.master_keys_file_location, &p);
	  JSONDecoder::decode_json("NC_MASTER_KEYS_GET_EXECUTABLE",
				   out.settings.master_keys_get_executable, &p);
	} catch (const JSONDecoder::err& e) {
	  unreadable(path, std::string("NC_MASTER_KEYS_*: ") + e.what());
	}
      }
    } else if (ret != -ENOENT) {
      unreadable(path, cpp_strerror(-ret));
    }
    /* Absent is not an error.  Their config.js supplies the defaults,
     * and the one setting that matters here defaults to false --
     * which is itself a permission fact, not an absence, so the
     * caller is told the file was missing rather than being handed a
     * false it cannot distinguish from a stored one. */
  }

  out.master_keys_present = path_exists(root + "/master_keys.json");

  /* identities/<id>/identity.json.  A directory per identity, not a
   * file per identity:  verified against a store their CLI wrote. */
  {
    const std::string dir = root + "/identities";
    std::vector<std::string> entries;
    int ret = list_dir(dir, entries);
    if (ret == -ENOENT) {
      /* no identities directory at all:  not a NooBaa store, or an
       * empty one.  Reported, because an import with nothing to import
       * is almost certainly pointed at the wrong directory. */
      unreadable(dir, "no identities directory");
    } else if (ret < 0) {
      unreadable(dir, cpp_strerror(-ret));
    } else {
      for (const auto& name : entries) {
	const std::string path = dir + "/" + name + "/identity.json";
	std::string doc;
	ret = read_record_file(dpp, path, doc);
	if (ret < 0) {
	  unreadable(path, cpp_strerror(-ret));
	  continue;
	}
	JSONParser p;
	if (!p.parse(doc.data(), doc.size())) {
	  unreadable(path, "not JSON");
	  continue;
	}
	NooBaaIdentity id;
	try {
	  decode_identity(&p, id);
	} catch (const JSONDecoder::err& e) {
	  unreadable(path, e.what());
	  continue;
	}
	/* The identifier is what everything else references (§3.2), so
	 * a record without one cannot be imported and must not be
	 * silently counted. */
	if (id.id.empty()) {
	  unreadable(path, "no _id");
	  continue;
	}
	out.identities.push_back(std::move(id));
      }
    }
  }

  /* buckets/<name>.json.  Which buckets exist, and that each record
   * parses.  Carrying the fields across is I5, and the request path
   * already reads one of these per bucket in `serve` mode. */
  {
    const std::string dir = root + "/buckets";
    std::vector<std::string> entries;
    int ret = list_dir(dir, entries);
    if (ret == -ENOENT) {
      /* a store with no buckets is ordinary */
    } else if (ret < 0) {
      unreadable(dir, cpp_strerror(-ret));
    } else {
      for (const auto& name : entries) {
	static const std::string suffix = ".json";
	if ((name.size() <= suffix.size()) ||
	    (name.compare(name.size() - suffix.size(), suffix.size(),
			  suffix) != 0)) {
	  /* `<name>.json.lock` sits beside each record;  it is their
	   * write path's and not a record. */
	  continue;
	}
	const std::string path = dir + "/" + name;
	std::string doc;
	ret = read_record_file(dpp, path, doc);
	if (ret < 0) {
	  unreadable(path, cpp_strerror(-ret));
	  continue;
	}
	JSONParser p;
	if (!p.parse(doc.data(), doc.size())) {
	  unreadable(path, "not JSON");
	  continue;
	}
	NooBaaBucketRecord rec;
	rec.name = name.substr(0, name.size() - suffix.size());
	/* Presence, then value.  Absent is a third state their cascade
	 * depends on, so it cannot be read as false. */
	if (p.find_obj("force_md5_etag")) {
	  try {
	    bool force = false;
	    JSONDecoder::decode_json("force_md5_etag", force, &p);
	    rec.force_md5_etag = force;
	  } catch (const JSONDecoder::err& e) {
	    unreadable(path, std::string("force_md5_etag: ") + e.what());
	    continue;
	  }
	}
	out.buckets.push_back(std::move(rec));
      }
    }
  }

  ldpp_dout(dpp, 4) << "nsfs: read NooBaa store " << root << ": "
    << out.accounts() << " accounts, " << out.iam_users() << " IAM users, "
    << out.buckets.size() << " buckets, "
    << (out.master_keys_present ? "master keys present" : "NO MASTER KEYS")
    << ", " << out.unreadable.size() << " unreadable" << dendl;

  return 0;
}

}}} // namespace rgw::sal::nsfs
