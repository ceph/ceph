// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/Utils.h"
#include "common/dout.h"
#include "common/errno.h"
#include <boost/lexical_cast.hpp>
#include <regex>

namespace librbd {
namespace migration {
namespace util {

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::util::" << __func__ << ": "

int parse_url(CephContext* cct, const std::string& url, UrlSpec* url_spec) {
  ldout(cct, 10) << "url=" << url << dendl;
  *url_spec = UrlSpec{};

  // parse the provided URL (scheme, user, password, host, port, path,
  // parameters, query, and fragment)
  std::regex url_regex(
    R"(^(?:([^:/]*)://)?(?:(\w+)(?::(\w+))?@)?([^/;\?:#]+)(?::([^/;\?#]+))?)"
    R"((?:/([^;\?#]*))?(?:;([^\?#]+))?(?:\?([^#]+))?(?:#(\w+))?$)");
  std::smatch match;
  if(!std::regex_match(url, match, url_regex)) {
    lderr(cct) << "invalid url: '" << url << "'" << dendl;
    return -EINVAL;
  }

  auto& scheme = match[1];
  if (scheme == "http" || scheme == "") {
    url_spec->scheme = URL_SCHEME_HTTP;
  } else if (scheme == "https") {
    url_spec->scheme = URL_SCHEME_HTTPS;
    url_spec->port = "443";
  } else {
    lderr(cct) << "invalid url scheme: '" << url << "'" << dendl;
    return -EINVAL;
  }

  url_spec->host = match[4];
  auto& port = match[5];
  if (port.matched) {
    try {
      boost::lexical_cast<uint16_t>(port);
    } catch (boost::bad_lexical_cast&) {
      lderr(cct) << "invalid url port: '" << url << "'" << dendl;
      return -EINVAL;
    }
    url_spec->port = port;
  }

  auto& path = match[6];
  if (path.matched) {
    url_spec->path += path;
  }
  return 0;
}

void zero_shrunk_snapshot(CephContext* cct, const io::Extents& image_extents,
                          uint64_t snap_id, uint64_t new_size,
                          std::optional<uint64_t> *previous_size,
                          io::SparseExtents* sparse_extents) {
  if (*previous_size && **previous_size > new_size) {
    ldout(cct, 20) << "snapshot resize " << **previous_size << " -> "
                   << new_size << dendl;
    interval_set<uint64_t> zero_interval;
    zero_interval.insert(new_size, **previous_size - new_size);

    for (auto& image_extent : image_extents) {
      interval_set<uint64_t> image_interval;
      image_interval.insert(image_extent.first, image_extent.second);

      image_interval.intersection_of(zero_interval);
      for (auto [image_offset, image_length] : image_interval) {
        ldout(cct, 20) << "zeroing extent " << image_offset << "~"
                       << image_length << " at snapshot " << snap_id << dendl;
        sparse_extents->insert(image_offset, image_length,
                               {io::SPARSE_EXTENT_STATE_ZEROED, image_length});
      }
    }
  }
  *previous_size = new_size;
}

void merge_snapshot_delta(const io::SnapIds& snap_ids,
                          io::SnapshotDelta* snapshot_delta) {
  io::SnapshotDelta orig_snapshot_delta = std::move(*snapshot_delta);
  snapshot_delta->clear();

  auto snap_id_it = snap_ids.begin();
  ceph_assert(snap_id_it != snap_ids.end());

  // merge any snapshot intervals that were not requested
  std::list<io::SparseExtents*> pending_sparse_extents;
  for (auto& [snap_key, sparse_extents] : orig_snapshot_delta) {
    // advance to next valid requested snap id
    while (snap_id_it != snap_ids.end() && *snap_id_it < snap_key.first) {
      ++snap_id_it;
    }
    if (snap_id_it == snap_ids.end()) {
      break;
    }

    // loop through older write/read snapshot sparse extents to remove any
    // overlaps with the current sparse extent
    for (auto prev_sparse_extents : pending_sparse_extents) {
      for (auto& sparse_extent : sparse_extents) {
        prev_sparse_extents->erase(sparse_extent.get_off(),
                                   sparse_extent.get_len());
      }
    }

    auto write_read_snap_ids = std::make_pair(*snap_id_it, snap_key.second);
    (*snapshot_delta)[write_read_snap_ids] = std::move(sparse_extents);

    if (write_read_snap_ids.first > snap_key.first) {
      // the current snapshot wasn't requested so it might need to get
      // merged with a later snapshot
      pending_sparse_extents.push_back(&(*snapshot_delta)[write_read_snap_ids]);
    } else {
      // we don't merge results passed a valid requested snapshot
      pending_sparse_extents.clear();
    }
  }
}

} // namespace util
} // namespace migration
} // namespace librbd
