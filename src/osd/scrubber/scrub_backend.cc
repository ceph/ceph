// -*- m_mode_desc:C++; tab-width:8; c-basic-offset:2; indent-tabs-m_mode_desc:t
// -*- vim: ts=2 sw=2 smarttab

#include "./scrub_backend.h"

#include <algorithm>

#include <fmt/ranges.h>

#include "common/debug.h"

#include "include/utime_fmt.h"
#include "messages/MOSDRepScrubMap.h"
#include "osd/ECUtil.h"
#include "osd/OSD.h"
#include "osd/PG.h"
#include "osd/PrimaryLogPG.h"
#include "osd/osd_types_fmt.h"

#include "pg_scrubber.h"

using std::set;
using std::stringstream;
using std::vector;
using namespace Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;

#define dout_context (m_scrubber.get_pg_cct())
#define dout_subsys ceph_subsys_osd
#undef dout_prefix

#define dout_prefix ScrubBackend::logger_prefix(_dout, this)

std::ostream& ScrubBackend::logger_prefix(std::ostream* out,
                                          const ScrubBackend* t)
{
  return t->m_scrubber.gen_prefix(*out) << " b.e.: ";
}

// ////////////////////////////////////////////////////////////////////////// //

// for a Primary
ScrubBackend::ScrubBackend(ScrubBeListener& scrubber,
                           PgScrubBeListener& pg,
                           pg_shard_t i_am,
                           bool repair,
                           scrub_level_t shallow_or_deep,
                           const std::set<pg_shard_t>& acting)
    : m_scrubber{scrubber}
    , m_pg{pg}
    , m_pg_whoami{i_am}
    , m_repair{repair}
    , m_depth{shallow_or_deep}
    , m_pg_id{scrubber.get_pgid()}
    , m_pool{m_pg.get_pgpool()}
    , m_incomplete_clones_allowed{m_pool.info.allow_incomplete_clones()}
    , m_conf{m_scrubber.get_pg_cct()->_conf}
    , clog{m_scrubber.get_logger()}
{
  m_formatted_id = m_pg_id.calc_name_sring();

  m_acting_but_me.reserve(acting.size());
  std::copy_if(acting.begin(),
               acting.end(),
               std::back_inserter(m_acting_but_me),
               [i_am](const pg_shard_t& shard) { return shard != i_am; });

  m_is_replicated = m_pool.info.is_replicated();
  m_mode_desc =
    (m_repair ? "repair"sv
              : (m_depth == scrub_level_t::deep ? "deep-scrub"sv : "scrub"sv));
}

// for a Replica
ScrubBackend::ScrubBackend(ScrubBeListener& scrubber,
                           PgScrubBeListener& pg,
                           pg_shard_t i_am,
                           bool repair,
                           scrub_level_t shallow_or_deep)
    : m_scrubber{scrubber}
    , m_pg{pg}
    , m_pg_whoami{i_am}
    , m_repair{repair}
    , m_depth{shallow_or_deep}
    , m_pg_id{scrubber.get_pgid()}
    , m_pool{m_pg.get_pgpool()}
    , m_conf{m_scrubber.get_pg_cct()->_conf}
    , clog{m_scrubber.get_logger()}
{
  m_formatted_id = m_pg_id.calc_name_sring();
  m_is_replicated = m_pool.info.is_replicated();
  m_mode_desc =
    (m_repair ? "repair"sv
              : (m_depth == scrub_level_t::deep ? "deep-scrub"sv : "scrub"sv));
}

uint64_t ScrubBackend::logical_to_ondisk_size(uint64_t logical_size) const
{
  return m_pg.logical_to_ondisk_size(logical_size);
}

void ScrubBackend::update_repair_status(bool should_repair)
{
  dout(15) << __func__
           << ": repair state set to :" << (should_repair ? "true" : "false")
           << dendl;
  m_repair = should_repair;
  m_mode_desc =
    (m_repair ? "repair"sv
              : (m_depth == scrub_level_t::deep ? "deep-scrub"sv : "scrub"sv));
}

void ScrubBackend::new_chunk()
{
  dout(15) << __func__ << dendl;
  this_chunk.emplace(m_pg_whoami);
}

ScrubMap& ScrubBackend::get_primary_scrubmap()
{
  return this_chunk->received_maps[m_pg_whoami];
}

void ScrubBackend::merge_to_authoritative_set()
{
  dout(15) << __func__ << dendl;
  ceph_assert(m_scrubber.is_primary());
  ceph_assert(this_chunk->authoritative_set.empty() &&
              "the scrubber-backend should be empty");

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 15>()) {
    for (const auto& rpl : m_acting_but_me) {
      dout(15) << fmt::format("{}: replica {} has {} items",
                              __func__,
                              rpl,
                              this_chunk->received_maps[rpl].objects.size())
               << dendl;
    }
  }

  // Construct the authoritative set of objects
  for (const auto& map : this_chunk->received_maps) {
    std::transform(map.second.objects.begin(),
                   map.second.objects.end(),
                   std::inserter(this_chunk->authoritative_set,
                                 this_chunk->authoritative_set.end()),
                   [](const auto& i) { return i.first; });
  }
}

ScrubMap& ScrubBackend::my_map()
{
  return this_chunk->received_maps[m_pg_whoami];
}

void ScrubBackend::decode_received_map(pg_shard_t from,
                                       const MOSDRepScrubMap& msg)
{
  auto p = const_cast<bufferlist&>(msg.get_data()).cbegin();
  this_chunk->received_maps[from].decode(p, m_pool.id);

  dout(15) << __func__ << ": decoded map from : " << from
           << ": versions: " << this_chunk->received_maps[from].valid_through
           << " / " << msg.get_map_epoch() << dendl;
}


std::vector<snap_mapper_fix_t> ScrubBackend::replica_clean_meta(
  ScrubMap& repl_map,
  bool max_reached,
  const hobject_t& start,
  SnapMapReaderI& snaps_getter)
{
  dout(15) << __func__ << ": REPL META # " << m_cleaned_meta_map.objects.size()
           << " objects" << dendl;
  ceph_assert(!m_cleaned_meta_map.objects.size());
  m_cleaned_meta_map.clear_from(start);  // RRR how can this be required?
  m_cleaned_meta_map.insert(repl_map);
  auto for_meta_scrub = clean_meta_map(m_cleaned_meta_map, max_reached);
  return scan_snaps(for_meta_scrub, snaps_getter);
}


// /////////////////////////////////////////////////////////////////////////////
//
//  comparing the maps
//
// /////////////////////////////////////////////////////////////////////////////

objs_fix_list_t ScrubBackend::scrub_compare_maps(
  bool max_reached,
  SnapMapReaderI& snaps_getter)
{
  dout(10) << __func__ << " has maps, analyzing" << dendl;
  ceph_assert(m_scrubber.is_primary());

  // construct authoritative scrub map for type-specific scrubbing

  m_cleaned_meta_map.insert(my_map());
  merge_to_authoritative_set();

  // collect some omap statistics into m_omap_stats
  omap_checks();

  update_authoritative();
  auto for_meta_scrub = clean_meta_map(m_cleaned_meta_map, max_reached);

  // ok, do the pg-type specific scrubbing

  // (Validates consistency of the object info and snap sets)
  scrub_snapshot_metadata(for_meta_scrub);

  return objs_fix_list_t{std::move(this_chunk->m_inconsistent_objs),
                         scan_snaps(for_meta_scrub, snaps_getter)};
}

void ScrubBackend::omap_checks()
{
  const bool needs_omap_check = std::any_of(
    this_chunk->received_maps.begin(),
    this_chunk->received_maps.end(),
    [](const auto& m) -> bool {
      return m.second.has_large_omap_object_errors || m.second.has_omap_keys;
    });

  if (!needs_omap_check) {
    return;  // Nothing to do
  }

  stringstream wss;

  // Iterate through objects and update omap stats
  for (const auto& ho : this_chunk->authoritative_set) {

    for (const auto& [srd, smap] : this_chunk->received_maps) {
      if (srd != m_pg_whoami) {
        // Only set omap stats for the primary
        continue;
      }

      auto it = smap.objects.find(ho);
      if (it == smap.objects.end()) {
        continue;
      }

      const ScrubMap::object& smap_obj = it->second;
      m_omap_stats.omap_bytes += smap_obj.object_omap_bytes;
      m_omap_stats.omap_keys += smap_obj.object_omap_keys;
      if (smap_obj.large_omap_object_found) {
        auto osdmap = m_scrubber.get_osdmap();
        pg_t pg;
        osdmap->map_to_pg(ho.pool, ho.oid.name, ho.get_key(), ho.nspace, &pg);
        pg_t mpg = osdmap->raw_pg_to_pg(pg);
        m_omap_stats.large_omap_objects++;
        wss << "Large omap object found. Object: " << ho << " PG: " << pg
            << " (" << mpg << ")"
            << " Key count: " << smap_obj.large_omap_object_key_count
            << " Size (bytes): " << smap_obj.large_omap_object_value_size
            << '\n';
        break;
      }
    }
  }

  if (!wss.str().empty()) {
    dout(5) << __func__ << ": " << wss.str() << dendl;
    clog.warn(wss);
  }
}

/*
 * update_authoritative() updates:
 *
 *  - m_auth_peers: adds obj-> list of pairs < scrub-map, shard>
 *
 *  - m_cleaned_meta_map: replaces [obj] entry with:
 *     the relevant object in the scrub-map of the "selected" (back-most) peer
 */
void ScrubBackend::update_authoritative()
{
  dout(10) << __func__ << dendl;

  if (m_acting_but_me.empty()) {
    return;
  }

  compare_smaps();  // note: might cluster-log errors

  // update the session-wide m_auth_peers with the list of good
  // peers for each object (i.e. the ones that are in this_chunks's auth list)
  for (auto& [obj, peers] : this_chunk->authoritative) {

    auth_peers_t good_peers;

    for (auto& peer : peers) {
      good_peers.emplace_back(this_chunk->received_maps[peer].objects[obj],
                              peer);
    }

    m_auth_peers.emplace(obj, std::move(good_peers));
  }

  for (const auto& [obj, peers] : this_chunk->authoritative) {
    m_cleaned_meta_map.objects.erase(obj);
    m_cleaned_meta_map.objects.insert(
      *(this_chunk->received_maps[peers.back()].objects.find(obj)));
  }
}

int ScrubBackend::scrub_process_inconsistent()
{
  dout(20) << fmt::format("{}: {} (m_repair:{}) good peers tbl #: {}",
                          __func__,
                          m_mode_desc,
                          m_repair,
                          m_auth_peers.size())
           << dendl;

  ceph_assert(!m_auth_peers.empty());
  // authoritative only store objects which are missing or inconsistent.

  // some tests expect an error message that does not contain the __func__ and
  // PG:
  auto err_msg = fmt::format("{} {} {} missing, {} inconsistent objects",
                             m_formatted_id,
                             m_mode_desc,
                             m_missing.size(),
                             m_inconsistent.size());

  dout(4) << err_msg << dendl;
  clog.error() << err_msg;

  ceph_assert(m_repair);
  int fixed_cnt{0};

  for (const auto& [hobj, shrd_list] : m_auth_peers) {

    auto missing_entry = m_missing.find(hobj);

    if (missing_entry != m_missing.end()) {
      repair_object(hobj, shrd_list, missing_entry->second);
      fixed_cnt += missing_entry->second.size();
    }

    if (m_inconsistent.count(hobj)) {
      repair_object(hobj, shrd_list, m_inconsistent[hobj]);
      fixed_cnt += m_inconsistent[hobj].size();
    }
  }
  return fixed_cnt;
}

void ScrubBackend::repair_object(const hobject_t& soid,
                                 const auth_peers_t& ok_peers,
                                 const set<pg_shard_t>& bad_peers)
{
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    // log the good peers
    set<pg_shard_t> ok_shards;  // the shards from the ok_peers list
    for (const auto& peer : ok_peers) {
      ok_shards.insert(peer.second);
    }
    dout(10) << fmt::format(
                  "repair_object {} bad_peers osd.{{{}}}, ok_peers osd.{{{}}}",
                  soid,
                  bad_peers,
                  ok_shards)
             << dendl;
  }

  const ScrubMap::object& po = ok_peers.back().first;

  object_info_t oi;
  try {
    bufferlist bv;
    if (po.attrs.count(OI_ATTR)) {
      bv.push_back(po.attrs.find(OI_ATTR)->second);
    }
    auto bliter = bv.cbegin();
    decode(oi, bliter);
  } catch (...) {
    dout(0) << __func__
            << ": Need version of replica, bad object_info_t: " << soid
            << dendl;
    ceph_abort();
  }

  if (bad_peers.count(m_pg.get_primary())) {
    // We should only be scrubbing if the PG is clean.
    ceph_assert(!m_pg.is_waiting_for_unreadable_object());
    dout(10) << __func__ << ": primary = " << m_pg.get_primary() << dendl;
  }

  // No need to pass ok_peers, they must not be missing the object, so
  // force_object_missing will add them to missing_loc anyway
  m_pg.force_object_missing(ScrubberPasskey{}, bad_peers, soid, oi.version);
}


// /////////////////////////////////////////////////////////////////////////////
//
// components formerly of PGBackend::be_compare_scrubmaps()
//
// /////////////////////////////////////////////////////////////////////////////

using usable_t = shard_as_auth_t::usable_t;


static inline int dcount(const object_info_t& oi)
{
  return (oi.is_data_digest() ? 1 : 0) + (oi.is_omap_digest() ? 1 : 0);
}

auth_selection_t ScrubBackend::select_auth_object(const hobject_t& ho,
                                                  stringstream& errstream)
{
  // Create a list of shards (with the Primary first, so that it will be
  // auth-copy, all other things being equal)

  /// \todo: consider sorting the candidate shards by the conditions for
  /// selecting best auth source below. Then - stopping on the first one
  /// that is auth eligible.
  /// This creates an issue with 'digest_match' that should be handled.
  std::list<pg_shard_t> shards;
  for (const auto& [srd, smap] : this_chunk->received_maps) {
    if (srd != m_pg_whoami) {
      shards.push_back(srd);
    }
  }
  shards.push_front(m_pg_whoami);

  auth_selection_t ret_auth;
  ret_auth.auth = this_chunk->received_maps.end();
  eversion_t auth_version;

  for (auto& l : shards) {

    auto shard_ret = possible_auth_shard(ho, l, ret_auth.shard_map);

    // digest_match will only be true if computed digests are the same
    if (auth_version != eversion_t() &&
        ret_auth.auth->second.objects[ho].digest_present &&
        shard_ret.digest.has_value() &&
        ret_auth.auth->second.objects[ho].digest != *shard_ret.digest) {

      ret_auth.digest_match = false;
      dout(10) << fmt::format(
                    "{}: digest_match = false, {} data_digest 0x{:x} != "
                    "data_digest 0x{:x}",
                    __func__,
                    ho,
                    ret_auth.auth->second.objects[ho].digest,
                    *shard_ret.digest)
               << dendl;
    }

    dout(20)
      << fmt::format("{}: {} shard {} got:{:D}", __func__, ho, l, shard_ret)
      << dendl;

    if (shard_ret.possible_auth == shard_as_auth_t::usable_t::not_usable) {

      // Don't use this particular shard due to previous errors
      // XXX: For now we can't pick one shard for repair and another's object
      // info or snapset

      ceph_assert(shard_ret.error_text.length());
      errstream << m_pg_id.pgid << " shard " << l << " soid " << ho << " : "
                << shard_ret.error_text << "\n";

    } else if (shard_ret.possible_auth ==
               shard_as_auth_t::usable_t::not_found) {

      // do not emit the returned error message to the log
      dout(15) << fmt::format("{}: {} not found on shard {}", __func__, ho, l)
               << dendl;
    } else {

      dout(30) << fmt::format("{}: consider using {} srv: {} oi soid: {}",
                              __func__,
                              l,
                              shard_ret.oi.version,
                              shard_ret.oi.soid)
               << dendl;

      // consider using this shard as authoritative. Is it more recent?

      if (auth_version == eversion_t() || shard_ret.oi.version > auth_version ||
          (shard_ret.oi.version == auth_version &&
           dcount(shard_ret.oi) > dcount(ret_auth.auth_oi))) {

        dout(20) << fmt::format("{}: using {} moved auth oi {:p} <-> {:p}",
                                __func__,
                                l,
                                (void*)&ret_auth.auth_oi,
                                (void*)&shard_ret.oi)
                 << dendl;

        ret_auth.auth = shard_ret.auth_iter;
        ret_auth.auth_shard = ret_auth.auth->first;
        ret_auth.auth_oi = shard_ret.oi;
        auth_version = shard_ret.oi.version;
        ret_auth.is_auth_available = true;
      }
    }
  }

  dout(10) << fmt::format("{}: selecting osd {} for obj {} with oi {}",
                          __func__,
                          ret_auth.auth_shard,
                          ho,
                          ret_auth.auth_oi)
           << dendl;

  return ret_auth;
}

using set_sinfo_err_t = void (shard_info_wrapper::*)();

inline static const char* sep(bool& prev_err)
{
  if (prev_err) {
    return ", ";
  } else {
    prev_err = true;
    return "";
  }
}

// retval: should we continue with the tests
static inline bool dup_error_cond(bool& prev_err,
                                  bool continue_on_err,
                                  bool pred,
                                  shard_info_wrapper& si,
                                  set_sinfo_err_t sete,
                                  std::string_view msg,
                                  stringstream& errstream)
{
  if (pred) {
    (si.*sete)();
    errstream << sep(prev_err) << msg;
    return continue_on_err;
  }
  return true;
}

/**
 * calls a shard_info_wrapper function, but only if the error predicate is
 * true.
 * Returns a copy of the error status.
 */
static inline bool test_error_cond(bool error_pred,
                                   shard_info_wrapper& si,
                                   set_sinfo_err_t sete)
{
  if (error_pred) {
    (si.*sete)();
  }
  return error_pred;
}

shard_as_auth_t ScrubBackend::possible_auth_shard(const hobject_t& obj,
                                                  const pg_shard_t& srd,
                                                  shard_info_map_t& shard_map)
{
  //  'maps' (originally called with this_chunk->maps): this_chunk->maps
  //  'auth_oi' (called with 'auth_oi', which wasn't initialized at call site)
  //     - create and return
  //  'shard_map' - the one created in select_auth_object()
  //     - used to access the 'shard_info'

  const auto j = this_chunk->received_maps.find(srd);
  const auto& j_shard = j->first;
  const auto& j_smap = j->second;
  auto i = j_smap.objects.find(obj);
  if (i == j_smap.objects.end()) {
    return shard_as_auth_t{};
  }
  const auto& smap_obj = i->second;

  auto& shard_info = shard_map[j_shard];
  if (j_shard == m_pg_whoami) {
    shard_info.primary = true;
  }

  stringstream errstream;  // for this shard

  bool err{false};
  dup_error_cond(err,
                 true,
                 smap_obj.read_error,
                 shard_info,
                 &shard_info_wrapper::set_read_error,
                 "candidate had a read error"sv,
                 errstream);
  dup_error_cond(err,
                 true,
                 smap_obj.ec_hash_mismatch,
                 shard_info,
                 &shard_info_wrapper::set_ec_hash_mismatch,
                 "candidate had an ec hash mismatch"sv,
                 errstream);
  dup_error_cond(err,
                 true,
                 smap_obj.ec_size_mismatch,
                 shard_info,
                 &shard_info_wrapper::set_ec_size_mismatch,
                 "candidate had an ec size mismatch"sv,
                 errstream);

  if (!dup_error_cond(err,
                      false,
                      smap_obj.stat_error,
                      shard_info,
                      &shard_info_wrapper::set_stat_error,
                      "candidate had a stat error"sv,
                      errstream)) {
    // With stat_error no further checking
    // We don't need to also see a missing_object_info_attr
    return shard_as_auth_t{errstream.str()};
  }

  // We won't pick an auth copy if the snapset is missing or won't decode.
  ceph_assert(!obj.is_snapdir());

  if (obj.is_head()) {
    auto k = smap_obj.attrs.find(SS_ATTR);
    if (dup_error_cond(err,
                       false,
                       (k == smap_obj.attrs.end()),
                       shard_info,
                       &shard_info_wrapper::set_snapset_missing,
                       "candidate had a missing snapset key"sv,
                       errstream)) {
      bufferlist ss_bl;
      SnapSet snapset;
      ss_bl.push_back(k->second);
      try {
        auto bliter = ss_bl.cbegin();
        decode(snapset, bliter);
      } catch (...) {
        // invalid snapset, probably corrupt
        dup_error_cond(err,
                       false,
                       true,
                       shard_info,
                       &shard_info_wrapper::set_snapset_corrupted,
                       "candidate had a corrupt snapset"sv,
                       errstream);
      }
    } else {
      // debug@dev only
      dout(30) << fmt::format(
                    "{} missing snap addr: {:p} shard_info: {:p} er: {:x}",
                    __func__,
                    (void*)&smap_obj,
                    (void*)&shard_info,
                    shard_info.errors)
               << dendl;
    }
  }

  if (!m_is_replicated) {
    auto k = smap_obj.attrs.find(ECUtil::get_hinfo_key());
    if (dup_error_cond(err,
                       false,
                       (k == smap_obj.attrs.end()),
                       shard_info,
                       &shard_info_wrapper::set_hinfo_missing,
                       "candidate had a missing hinfo key"sv,
                       errstream)) {
      bufferlist hk_bl;
      ECUtil::HashInfo hi;
      hk_bl.push_back(k->second);
      try {
        auto bliter = hk_bl.cbegin();
        decode(hi, bliter);
      } catch (...) {
        dup_error_cond(err,
                       false,
                       true,
                       shard_info,
                       &shard_info_wrapper::set_hinfo_corrupted,
                       "candidate had a corrupt hinfo"sv,
                       errstream);
      }
    }
  }

  object_info_t oi;

  {
    auto k = smap_obj.attrs.find(OI_ATTR);
    if (!dup_error_cond(err,
                        false,
                        (k == smap_obj.attrs.end()),
                        shard_info,
                        &shard_info_wrapper::set_info_missing,
                        "candidate had a missing info key"sv,
                        errstream)) {
      // no object info on object, probably corrupt
      return shard_as_auth_t{errstream.str()};
    }

    bufferlist bl;
    bl.push_back(k->second);
    try {
      auto bliter = bl.cbegin();
      decode(oi, bliter);
    } catch (...) {
      // invalid object info, probably corrupt
      if (!dup_error_cond(err,
                          false,
                          true,
                          shard_info,
                          &shard_info_wrapper::set_info_corrupted,
                          "candidate had a corrupt info"sv,
                          errstream)) {
        return shard_as_auth_t{errstream.str()};
      }
    }
  }

  // This is automatically corrected in repair_oinfo_oid()
  ceph_assert(oi.soid == obj);

  if (test_error_cond(smap_obj.size != logical_to_ondisk_size(oi.size),
                      shard_info,
                      &shard_info_wrapper::set_obj_size_info_mismatch)) {

    errstream << sep(err) << "candidate size " << smap_obj.size << " info size "
              << logical_to_ondisk_size(oi.size) << " mismatch";
  }

  std::optional<uint32_t> digest;
  if (smap_obj.digest_present) {
    digest = smap_obj.digest;
  }

  if (shard_info.errors) {
    ceph_assert(err);
    return shard_as_auth_t{errstream.str(), digest};
  }

  ceph_assert(!err);
  // note that the error text is made available to the caller, even
  // for a successful shard selection
  return shard_as_auth_t{oi, j, errstream.str(), digest};
}

// re-implementation of PGBackend::be_compare_scrubmaps()
void ScrubBackend::compare_smaps()
{
  dout(10) << __func__
           << ": authoritative-set #: " << this_chunk->authoritative_set.size()
           << dendl;

  std::for_each(this_chunk->authoritative_set.begin(),
                this_chunk->authoritative_set.end(),
                [this](const auto& ho) {
                  if (auto maybe_clust_err = compare_obj_in_maps(ho);
                      maybe_clust_err) {
                    clog.error() << *maybe_clust_err;
                  }
                });
}

std::optional<std::string> ScrubBackend::compare_obj_in_maps(
  const hobject_t& ho)
{
  // clear per-object data:
  this_chunk->cur_inconsistent.clear();
  this_chunk->cur_missing.clear();
  this_chunk->fix_digest = false;

  stringstream candidates_errors;
  auto auth_res = select_auth_object(ho, candidates_errors);
  if (candidates_errors.str().size()) {
    // a collection of shard-specific errors detected while
    // finding the best shard to serve as authoritative
    clog.error() << candidates_errors.str();
  }

  inconsistent_obj_wrapper object_error{ho};
  if (!auth_res.is_auth_available) {
    // no auth selected
    object_error.set_version(0);
    object_error.set_auth_missing(ho,
                                  this_chunk->received_maps,
                                  auth_res.shard_map,
                                  this_chunk->m_error_counts.shallow_errors,
                                  this_chunk->m_error_counts.deep_errors,
                                  m_pg_whoami);

    if (object_error.has_deep_errors()) {
      this_chunk->m_error_counts.deep_errors++;
    } else if (object_error.has_shallow_errors()) {
      this_chunk->m_error_counts.shallow_errors++;
    }

    this_chunk->m_inconsistent_objs.push_back(std::move(object_error));
    return fmt::format("{} soid {} : failed to pick suitable object info\n",
                       m_scrubber.get_pgid().pgid,
                       ho);
  }

  stringstream errstream;
  auto& auth = auth_res.auth;

  // an auth source was selected

  object_error.set_version(auth_res.auth_oi.user_version);
  ScrubMap::object& auth_object = auth->second.objects[ho];
  ceph_assert(!this_chunk->fix_digest);

  auto [auths, objerrs] =
    match_in_shards(ho, auth_res, object_error, errstream);

  auto opt_ers =
    for_empty_auth_list(std::move(auths),
                        std::move(objerrs),
                        auth,
                        ho,
                        errstream);

  if (opt_ers.has_value()) {

    // At this point auth_list is populated, so we add the object error
    // shards as inconsistent.
    inconsistents(ho,
                  auth_object,
                  auth_res.auth_oi,
                  std::move(*opt_ers),
                  errstream);
  } else {

    // both the auth & errs containers are empty
    errstream << m_pg_id << " soid " << ho << " : empty auth list\n";
  }

  if (object_error.has_deep_errors()) {
    this_chunk->m_error_counts.deep_errors++;
  } else if (object_error.has_shallow_errors()) {
    this_chunk->m_error_counts.shallow_errors++;
  }

  if (object_error.errors || object_error.union_shards.errors) {
    this_chunk->m_inconsistent_objs.push_back(std::move(object_error));
  }

  if (errstream.str().empty()) {
    return std::nullopt;
  } else {
    return errstream.str();
  }
}


std::optional<ScrubBackend::auth_and_obj_errs_t>
ScrubBackend::for_empty_auth_list(std::list<pg_shard_t>&& auths,
                                  std::set<pg_shard_t>&& obj_errors,
                                  shard_to_scrubmap_t::iterator auth,
                                  const hobject_t& ho,
                                  stringstream& errstream)
{
  if (auths.empty()) {
    if (obj_errors.empty()) {
      errstream << m_pg_id << " soid " << ho
                << " : failed to pick suitable auth object\n";
      return std::nullopt;
    }
    // Object errors exist and nothing in auth_list
    // Prefer the auth shard, otherwise take first from list.
    pg_shard_t shard;
    if (obj_errors.count(auth->first)) {
      shard = auth->first;
    } else {
      shard = *(obj_errors.begin());
    }

    auths.push_back(shard);
    obj_errors.erase(shard);
  }

  return ScrubBackend::auth_and_obj_errs_t{std::move(auths),
                                           std::move(obj_errors)};
}


/// \todo replace the errstream with a member of this_chunk. Better be a
///  fmt::buffer. Then - we can use it directly in should_fix_digest()
void ScrubBackend::inconsistents(const hobject_t& ho,
                                 ScrubMap::object& auth_object,
                                 object_info_t& auth_oi,
                                 auth_and_obj_errs_t&& auth_n_errs,
                                 stringstream& errstream)
{
  auto& object_errors = auth_n_errs.object_errors;
  auto& auth_list = auth_n_errs.auth_list;

  this_chunk->cur_inconsistent.insert(object_errors.begin(),
                                      object_errors.end());  // merge?

  dout(15) << fmt::format(
                "{}: object errors #: {}  auth list #: {}  cur_missing #: {}  "
                "cur_incon #: {}",
                __func__,
                object_errors.size(),
                auth_list.size(),
                this_chunk->cur_missing.size(),
                this_chunk->cur_inconsistent.size())
           << dendl;


  if (!this_chunk->cur_missing.empty()) {
    m_missing[ho] = this_chunk->cur_missing;
  }
  if (!this_chunk->cur_inconsistent.empty()) {
    m_inconsistent[ho] = this_chunk->cur_inconsistent;
  }

  if (this_chunk->fix_digest) {

    ceph_assert(auth_object.digest_present);
    std::optional<uint32_t> data_digest{auth_object.digest};

    std::optional<uint32_t> omap_digest;
    if (auth_object.omap_digest_present) {
      omap_digest = auth_object.omap_digest;
    }
    this_chunk->missing_digest.push_back(
      make_pair(ho, make_pair(data_digest, omap_digest)));
  }

  if (!this_chunk->cur_inconsistent.empty() ||
      !this_chunk->cur_missing.empty()) {

    this_chunk->authoritative[ho] = auth_list;

  } else if (!this_chunk->fix_digest && m_is_replicated) {

    auto is_to_fix =
      should_fix_digest(ho, auth_object, auth_oi, m_repair, errstream);

    switch (is_to_fix) {

      case digest_fixing_t::no:
        break;

      case digest_fixing_t::if_aged: {
        utime_t age = this_chunk->started - auth_oi.local_mtime;

        // \todo find out 'age_limit' only once
        const auto age_limit = m_conf->osd_deep_scrub_update_digest_min_age;

        if (age <= age_limit) {
          dout(20) << __func__ << ": missing digest but age (" << age
                   << ") < conf (" << age_limit << ") on " << ho << dendl;
          break;
        }
      }

        [[fallthrough]];

      case digest_fixing_t::force:

        std::optional<uint32_t> data_digest;
        if (auth_object.digest_present) {
          data_digest = auth_object.digest;
          dout(20) << __func__ << ": will update data digest on " << ho
                   << dendl;
        }

        std::optional<uint32_t> omap_digest;
        if (auth_object.omap_digest_present) {
          omap_digest = auth_object.omap_digest;
          dout(20) << __func__ << ": will update omap digest on " << ho
                   << dendl;
        }
        this_chunk->missing_digest.push_back(
          make_pair(ho, make_pair(data_digest, omap_digest)));
        break;
    }
  }
}

/// \todo consider changing to use format() and to return the strings
ScrubBackend::digest_fixing_t ScrubBackend::should_fix_digest(
  const hobject_t& ho,
  const ScrubMap::object& auth_object,
  const object_info_t& auth_oi,
  bool repair_flag,
  stringstream& errstream)
{
  digest_fixing_t update{digest_fixing_t::no};

  if (auth_object.digest_present && !auth_oi.is_data_digest()) {
    dout(15) << __func__ << " missing data digest on " << ho << dendl;
    update = digest_fixing_t::if_aged;
  }

  if (auth_object.omap_digest_present && !auth_oi.is_omap_digest()) {
    dout(15) << __func__ << " missing omap digest on " << ho << dendl;
    update = digest_fixing_t::if_aged;
  }

  // recorded digest != actual digest?
  if (auth_oi.is_data_digest() && auth_object.digest_present &&
      auth_oi.data_digest != auth_object.digest) {
    errstream << m_pg_id << " recorded data digest 0x" << std::hex
              << auth_oi.data_digest << " != on disk 0x" << auth_object.digest
              << std::dec << " on " << auth_oi.soid << "\n";
    if (repair_flag)
      update = digest_fixing_t::force;
  }

  if (auth_oi.is_omap_digest() && auth_object.omap_digest_present &&
      auth_oi.omap_digest != auth_object.omap_digest) {
    errstream << m_pg_id << " recorded omap digest 0x" << std::hex
              << auth_oi.omap_digest << " != on disk 0x"
              << auth_object.omap_digest << std::dec << " on " << auth_oi.soid
              << "\n";
    if (repair_flag)
      update = digest_fixing_t::force;
  }

  return update;
}

ScrubBackend::auth_and_obj_errs_t ScrubBackend::match_in_shards(
  const hobject_t& ho,
  auth_selection_t& auth_sel,
  inconsistent_obj_wrapper& obj_result,
  stringstream& errstream)
{
  std::list<pg_shard_t> auth_list;     // out "param" to
  std::set<pg_shard_t> object_errors;  // be returned

  for (auto& [srd, smap] : this_chunk->received_maps) {

    if (srd == auth_sel.auth_shard) {
      auth_sel.shard_map[auth_sel.auth_shard].selected_oi = true;
    }

    if (smap.objects.count(ho)) {

      // the scrub-map has our object
      auth_sel.shard_map[srd].set_object(smap.objects[ho]);

      // Compare
      stringstream ss;
      const auto& auth_object = auth_sel.auth->second.objects[ho];
      const bool discrep_found = compare_obj_details(auth_sel.auth_shard,
                                                     auth_object,
                                                     auth_sel.auth_oi,
                                                     smap.objects[ho],
                                                     auth_sel.shard_map[srd],
                                                     obj_result,
                                                     ss,
                                                     ho.has_snapset());

      dout(20) << fmt::format(
		    "{}: {}{} <{}:{}> shards: {} {} {}", __func__,
		    (m_repair ? "repair " : ""),
		    (m_is_replicated ? "replicated " : ""), srd,
		    (srd == auth_sel.auth_shard ? "auth" : "-"),
		    auth_sel.shard_map.size(),
		    (auth_sel.digest_match ? " digest_match " : " "),
		    (auth_sel.shard_map[srd].only_data_digest_mismatch_info()
		       ? "'info mismatch info'"
		       : ""))
	       << dendl;
      if (discrep_found) {
	dout(10) << fmt::format(
		      "{}: <{}> auth:{} ({}/{}) vs {} ({}/{}) {}", __func__, ho,
		      auth_sel.auth_shard, auth_object.omap_digest_present,
		      auth_object.omap_digest, srd,
		      smap.objects[ho].omap_digest_present ? true : false,
		      smap.objects[ho].omap_digest, ss.str())
		 << dendl;
      }

      // If all replicas match, but they don't match object_info we can
      // repair it by using missing_digest mechanism
      if (m_repair && m_is_replicated && (srd == auth_sel.auth_shard) &&
          auth_sel.shard_map.size() > 1 && auth_sel.digest_match &&
          auth_sel.shard_map[srd].only_data_digest_mismatch_info() &&
          auth_object.digest_present) {
        // Set in missing_digests
        this_chunk->fix_digest = true;
        // Clear the error
        auth_sel.shard_map[srd].clear_data_digest_mismatch_info();
        errstream << m_pg_id << " soid " << ho
                  << " : repairing object info data_digest"
                  << "\n";
      }

      // Some errors might have already been set in select_auth_object()
      if (auth_sel.shard_map[srd].errors != 0) {

        this_chunk->cur_inconsistent.insert(srd);
        if (auth_sel.shard_map[srd].has_deep_errors()) {
          this_chunk->m_error_counts.deep_errors++;
        } else {
          this_chunk->m_error_counts.shallow_errors++;
        }

        if (discrep_found) {
          // Only true if compare_obj_details() found errors and put something
          // in ss
          errstream << m_pg_id << " shard " << srd << " soid " << ho << " : "
                    << ss.str() << "\n";
        }

      } else if (discrep_found) {

        // Track possible shards to use as authoritative, if needed

        // There are errors, without identifying the shard
        object_errors.insert(srd);
        errstream << m_pg_id << " soid " << ho << " : " << ss.str() << "\n";

      } else {

        // XXX: The auth shard might get here that we don't know
        // that it has the "correct" data.
        auth_list.push_back(srd);
      }

    } else {

      this_chunk->cur_missing.insert(srd);
      auth_sel.shard_map[srd].set_missing();
      auth_sel.shard_map[srd].primary = (srd == m_pg_whoami);

      // Can't have any other errors if there is no information available
      this_chunk->m_error_counts.shallow_errors++;
      errstream << m_pg_id << " shard " << srd << " " << ho << " : missing\n";
    }
    obj_result.add_shard(srd, auth_sel.shard_map[srd]);

    dout(20) << __func__ << ": (debug) soid " << ho << " : " << errstream.str()
             << dendl;
  }

  dout(15) << fmt::format("{}: auth_list: {} #: {}; obj-errs#: {}",
                          __func__,
                          auth_list,
                          auth_list.size(),
                          object_errors.size())
           << dendl;
  return {auth_list, object_errors};
}

// == PGBackend::be_compare_scrub_objects()
bool ScrubBackend::compare_obj_details(pg_shard_t auth_shard,
                                       const ScrubMap::object& auth,
                                       const object_info_t& auth_oi,
                                       const ScrubMap::object& candidate,
                                       shard_info_wrapper& shard_result,
                                       inconsistent_obj_wrapper& obj_result,
                                       stringstream& errstream,
                                       bool has_snapset)
{
  fmt::memory_buffer out;
  bool error{false};

  // ------------------------------------------------------------------------

  if (auth.digest_present && candidate.digest_present &&
      auth.digest != candidate.digest) {
    fmt::format_to(std::back_inserter(out),
                   "data_digest {:#x} != data_digest {:#x} from shard {}",
                   candidate.digest,
                   auth.digest,
                   auth_shard);
    error = true;
    obj_result.set_data_digest_mismatch();
  }

  if (auth.omap_digest_present && candidate.omap_digest_present &&
      auth.omap_digest != candidate.omap_digest) {
    fmt::format_to(std::back_inserter(out),
                   "{}omap_digest {:#x} != omap_digest {:#x} from shard {}",
                   sep(error),
                   candidate.omap_digest,
                   auth.omap_digest,
                   auth_shard);
    obj_result.set_omap_digest_mismatch();
  }

  // for replicated:
  if (m_is_replicated) {
    if (auth_oi.is_data_digest() && candidate.digest_present &&
        auth_oi.data_digest != candidate.digest) {
      fmt::format_to(std::back_inserter(out),
                     "{}data_digest {:#x} != data_digest {:#x} from auth oi {}",
                     sep(error),
                     candidate.digest,
                     auth_oi.data_digest,
                     auth_oi);
      shard_result.set_data_digest_mismatch_info();
    }

    // for replicated:
    if (auth_oi.is_omap_digest() && candidate.omap_digest_present &&
        auth_oi.omap_digest != candidate.omap_digest) {
      fmt::format_to(std::back_inserter(out),
                     "{}omap_digest {:#x} != omap_digest {:#x} from auth oi {}",
                     sep(error),
                     candidate.omap_digest,
                     auth_oi.omap_digest,
                     auth_oi);
      shard_result.set_omap_digest_mismatch_info();
    }
  }

  // ------------------------------------------------------------------------

  if (candidate.stat_error) {
    if (error) {
      errstream << fmt::to_string(out);
    }
    return error;
  }

  // ------------------------------------------------------------------------

  if (!shard_result.has_info_missing() && !shard_result.has_info_corrupted()) {

    auto can_attr = candidate.attrs.find(OI_ATTR);
    ceph_assert(can_attr != candidate.attrs.end());
    bufferlist can_bl;
    can_bl.push_back(can_attr->second);

    auto auth_attr = auth.attrs.find(OI_ATTR);
    ceph_assert(auth_attr != auth.attrs.end());
    bufferlist auth_bl;
    auth_bl.push_back(auth_attr->second);

    if (!can_bl.contents_equal(auth_bl)) {
      fmt::format_to(std::back_inserter(out),
		     "{}object info inconsistent ",
		     sep(error));
      obj_result.set_object_info_inconsistency();
    }
  }

  if (has_snapset) {
    if (!shard_result.has_snapset_missing() &&
        !shard_result.has_snapset_corrupted()) {

      auto can_attr = candidate.attrs.find(SS_ATTR);
      ceph_assert(can_attr != candidate.attrs.end());
      bufferlist can_bl;
      can_bl.push_back(can_attr->second);

      auto auth_attr = auth.attrs.find(SS_ATTR);
      ceph_assert(auth_attr != auth.attrs.end());
      bufferlist auth_bl;
      auth_bl.push_back(auth_attr->second);

      if (!can_bl.contents_equal(auth_bl)) {
        fmt::format_to(std::back_inserter(out),
		       "{}snapset inconsistent ",
		       sep(error));
        obj_result.set_snapset_inconsistency();
      }
    }
  }

  // ------------------------------------------------------------------------

  if (!m_is_replicated) {
    if (!shard_result.has_hinfo_missing() &&
        !shard_result.has_hinfo_corrupted()) {

      auto can_hi = candidate.attrs.find(ECUtil::get_hinfo_key());
      ceph_assert(can_hi != candidate.attrs.end());
      bufferlist can_bl;
      can_bl.push_back(can_hi->second);

      auto auth_hi = auth.attrs.find(ECUtil::get_hinfo_key());
      ceph_assert(auth_hi != auth.attrs.end());
      bufferlist auth_bl;
      auth_bl.push_back(auth_hi->second);

      if (!can_bl.contents_equal(auth_bl)) {
        fmt::format_to(std::back_inserter(out),
		       "{}hinfo inconsistent ",
		       sep(error));
        obj_result.set_hinfo_inconsistency();
      }
    }
  }

  // ------------------------------------------------------------------------

  // sizes:

  uint64_t oi_size = logical_to_ondisk_size(auth_oi.size);
  if (oi_size != candidate.size) {
    fmt::format_to(std::back_inserter(out),
                   "{}size {} != size {} from auth oi {}",
                   sep(error),
                   candidate.size,
                   oi_size,
                   auth_oi);
    shard_result.set_size_mismatch_info();
  }

  if (auth.size != candidate.size) {
    fmt::format_to(std::back_inserter(out),
                   "{}size {} != size {} from shard {}",
                   sep(error),
                   candidate.size,
                   auth.size,
                   auth_shard);
    obj_result.set_size_mismatch();
  }

  // If the replica is too large and we didn't already count it for this object

  if (candidate.size > m_conf->osd_max_object_size &&
      !obj_result.has_size_too_large()) {

    fmt::format_to(std::back_inserter(out),
                   "{}size {} > {} is too large",
                   sep(error),
                   candidate.size,
                   m_conf->osd_max_object_size);
    obj_result.set_size_too_large();
  }

  // ------------------------------------------------------------------------

  // comparing the attributes:

  for (const auto& [k, v] : auth.attrs) {
    if (k == OI_ATTR || k[0] != '_') {
      // We check system keys separately
      continue;
    }

    auto cand = candidate.attrs.find(k);
    if (cand == candidate.attrs.end()) {
      fmt::format_to(std::back_inserter(out),
		     "{}attr name mismatch '{}'",
		     sep(error),
		     k);
      obj_result.set_attr_name_mismatch();
    } else if (cand->second.cmp(v)) {
      fmt::format_to(std::back_inserter(out),
		     "{}attr value mismatch '{}'",
		     sep(error),
		     k);
      obj_result.set_attr_value_mismatch();
    }
  }

  for (const auto& [k, v] : candidate.attrs) {
    if (k == OI_ATTR || k[0] != '_') {
      // We check system keys separately
      continue;
    }

    auto in_auth = auth.attrs.find(k);
    if (in_auth == auth.attrs.end()) {
      fmt::format_to(std::back_inserter(out),
		     "{}attr name mismatch '{}'",
		     sep(error),
		     k);
      obj_result.set_attr_name_mismatch();
    }
  }

  if (error) {
    errstream << fmt::to_string(out);
  }
  return error;
}

static inline bool doing_clones(
  const std::optional<SnapSet>& snapset,
  const vector<snapid_t>::reverse_iterator& curclone)
{
  return snapset && curclone != snapset->clones.rend();
}

// /////////////////////////////////////////////////////////////////////////////
//
// final checking & fixing - scrub_snapshot_metadata()
//
// /////////////////////////////////////////////////////////////////////////////

/*
 * Validate consistency of the object info and snap sets.
 *
 * We are sort of comparing 2 lists. The main loop is on objmap.objects. But
 * the comparison of the objects is against multiple snapset.clones. There are
 * multiple clone lists and in between lists we expect head.
 *
 * Example
 *
 * objects              expected
 * =======              =======
 * obj1 snap 1          head, unexpected obj1 snap 1
 * obj2 head            head, match
 *              [SnapSet clones 6 4 2 1]
 * obj2 snap 7          obj2 snap 6, unexpected obj2 snap 7
 * obj2 snap 6          obj2 snap 6, match
 * obj2 snap 4          obj2 snap 4, match
 * obj3 head            obj2 snap 2 (expected), obj2 snap 1 (expected), match
 *              [Snapset clones 3 1]
 * obj3 snap 3          obj3 snap 3 match
 * obj3 snap 1          obj3 snap 1 match
 * obj4 head            head, match
 *              [Snapset clones 4]
 * EOL                  obj4 snap 4, (expected)
 */
void ScrubBackend::scrub_snapshot_metadata(ScrubMap& map)
{
  dout(10) << __func__ << " num stat obj "
	   << m_pg.get_pg_info(ScrubberPasskey{}).stats.stats.sum.num_objects
	   << dendl;

  std::optional<snapid_t> all_clones;  // Unspecified snapid_t or std::nullopt

  // traverse in reverse order.
  std::optional<hobject_t> head;
  std::optional<SnapSet> snapset;  // If initialized so will head (above)
  vector<snapid_t>::reverse_iterator
    curclone;  // Defined only if snapset initialized
  int missing = 0;
  inconsistent_snapset_wrapper soid_error, head_error;
  int soid_error_count = 0;

  for (auto p = map.objects.rbegin(); p != map.objects.rend(); ++p) {

    const hobject_t& soid = p->first;
    ceph_assert(!soid.is_snapdir());
    soid_error = inconsistent_snapset_wrapper{soid};
    object_stat_sum_t stat;

    stat.num_objects++;

    if (soid.nspace == m_conf->osd_hit_set_namespace)
      stat.num_objects_hit_set_archive++;

    if (soid.is_snap()) {
      // it's a clone
      stat.num_object_clones++;
    }

    // basic checks.
    std::optional<object_info_t> oi;
    if (!p->second.attrs.count(OI_ATTR)) {
      oi = std::nullopt;
      clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                    << " : no '" << OI_ATTR << "' attr";
      this_chunk->m_error_counts.shallow_errors++;
      soid_error.set_info_missing();
    } else {
      bufferlist bv;
      bv.push_back(p->second.attrs[OI_ATTR]);
      try {
        oi = object_info_t(bv);
      } catch (ceph::buffer::error& e) {
        oi = std::nullopt;
        clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                      << " : can't decode '" << OI_ATTR << "' attr "
                      << e.what();
        this_chunk->m_error_counts.shallow_errors++;
        soid_error.set_info_corrupted();
        soid_error.set_info_missing();  // Not available too
      }
    }

    if (oi) {
      if (logical_to_ondisk_size(oi->size) != p->second.size) {
        clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                      << " : on disk size (" << p->second.size
                      << ") does not match object info size (" << oi->size
                      << ") adjusted for ondisk to ("
                      << logical_to_ondisk_size(oi->size) << ")";
        soid_error.set_size_mismatch();
        this_chunk->m_error_counts.shallow_errors++;
      }

      dout(20) << m_mode_desc << "  " << soid << " " << *oi << dendl;

      // A clone num_bytes will be added later when we have snapset
      if (!soid.is_snap()) {
        stat.num_bytes += oi->size;
      }
      if (soid.nspace == m_conf->osd_hit_set_namespace)
        stat.num_bytes_hit_set_archive += oi->size;

      if (oi->is_dirty())
        ++stat.num_objects_dirty;
      if (oi->is_whiteout())
        ++stat.num_whiteouts;
      if (oi->is_omap())
        ++stat.num_objects_omap;
      if (oi->is_cache_pinned())
        ++stat.num_objects_pinned;
      if (oi->has_manifest())
        ++stat.num_objects_manifest;
    }

    // Check for any problems while processing clones
    if (doing_clones(snapset, curclone)) {
      std::optional<snapid_t> target;
      // Expecting an object with snap for current head
      if (soid.has_snapset() || soid.get_head() != head->get_head()) {

        dout(10) << __func__ << " " << m_mode_desc << " " << m_pg_id
                 << " new object " << soid << " while processing " << *head
                 << dendl;

        target = all_clones;
      } else {
        ceph_assert(soid.is_snap());
        target = soid.snap;
      }

      // Log any clones we were expecting to be there up to target
      // This will set missing, but will be a no-op if snap.soid == *curclone.
      missing +=
        process_clones_to(head, snapset, target, &curclone, head_error);
    }

    bool expected;
    // Check doing_clones() again in case we ran process_clones_to()
    if (doing_clones(snapset, curclone)) {
      // A head would have processed all clones above
      // or all greater than *curclone.
      ceph_assert(soid.is_snap() && *curclone <= soid.snap);

      // After processing above clone snap should match the expected curclone
      expected = (*curclone == soid.snap);
    } else {
      // If we aren't doing clones any longer, then expecting head
      expected = soid.has_snapset();
    }
    if (!expected) {
      // If we couldn't read the head's snapset, just ignore clones
      if (head && !snapset) {
	clog.error() << m_mode_desc << " " << m_pg_id
		     << " " << soid
		     << " : clone ignored due to missing snapset";
      } else {
        clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                      << " : is an unexpected clone";
      }
      this_chunk->m_error_counts.shallow_errors++;
      soid_error.set_headless();
      this_chunk->m_inconsistent_objs.push_back(std::move(soid_error));
      ++soid_error_count;
      if (head && soid.get_head() == head->get_head())
        head_error.set_clone(soid.snap);
      continue;
    }

    // new snapset?
    if (soid.has_snapset()) {

      if (missing) {
        log_missing(missing, head, __func__);
      }

      // Save previous head error information
      if (head && (head_error.errors || soid_error_count)) {
        this_chunk->m_inconsistent_objs.push_back(std::move(head_error));
      }

      // Set this as a new head object
      head = soid;
      missing = 0;
      head_error = soid_error;
      soid_error_count = 0;

      dout(20) << __func__ << " " << m_mode_desc << " new head " << head
               << dendl;

      if (p->second.attrs.count(SS_ATTR) == 0) {
        clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                      << " : no '" << SS_ATTR << "' attr";
        this_chunk->m_error_counts.shallow_errors++;
        snapset = std::nullopt;
        head_error.set_snapset_missing();
      } else {
        bufferlist bl;
        bl.push_back(p->second.attrs[SS_ATTR]);
        auto blp = bl.cbegin();
        try {
          snapset = SnapSet();  // Initialize optional<> before decoding into it
          decode(*snapset, blp);
          head_error.ss_bl.push_back(p->second.attrs[SS_ATTR]);
        } catch (ceph::buffer::error& e) {
          snapset = std::nullopt;
          clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                        << " : can't decode '" << SS_ATTR << "' attr "
                        << e.what();
          this_chunk->m_error_counts.shallow_errors++;
          head_error.set_snapset_corrupted();
        }
      }

      if (snapset) {
        // what will be next?
        curclone = snapset->clones.rbegin();

        if (!snapset->clones.empty()) {
          dout(20) << "  snapset " << *snapset << dendl;
          if (snapset->seq == 0) {
            clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                          << " : snaps.seq not set";
            this_chunk->m_error_counts.shallow_errors++;
            head_error.set_snapset_error();
          }
        }
      }
    } else {
      ceph_assert(soid.is_snap());
      ceph_assert(head);
      ceph_assert(snapset);
      ceph_assert(soid.snap == *curclone);

      dout(20) << __func__ << " " << m_mode_desc << " matched clone " << soid
               << dendl;

      if (snapset->clone_size.count(soid.snap) == 0) {
        clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                      << " : is missing in clone_size";
        this_chunk->m_error_counts.shallow_errors++;
        soid_error.set_size_mismatch();
      } else {
        if (oi && oi->size != snapset->clone_size[soid.snap]) {
          clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                        << " : size " << oi->size << " != clone_size "
                        << snapset->clone_size[*curclone];
          this_chunk->m_error_counts.shallow_errors++;
          soid_error.set_size_mismatch();
        }

        if (snapset->clone_overlap.count(soid.snap) == 0) {
          clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                        << " : is missing in clone_overlap";
          this_chunk->m_error_counts.shallow_errors++;
          soid_error.set_size_mismatch();
        } else {
          // This checking is based on get_clone_bytes().  The first 2 asserts
          // can't happen because we know we have a clone_size and
          // a clone_overlap.  Now we check that the interval_set won't
          // cause the last assert.
          uint64_t size = snapset->clone_size.find(soid.snap)->second;
          const interval_set<uint64_t>& overlap =
            snapset->clone_overlap.find(soid.snap)->second;
          bool bad_interval_set = false;
          for (interval_set<uint64_t>::const_iterator i = overlap.begin();
               i != overlap.end();
               ++i) {
            if (size < i.get_len()) {
              bad_interval_set = true;
              break;
            }
            size -= i.get_len();
          }

          if (bad_interval_set) {
            clog.error() << m_mode_desc << " " << m_pg_id << " " << soid
                          << " : bad interval_set in clone_overlap";
            this_chunk->m_error_counts.shallow_errors++;
            soid_error.set_size_mismatch();
          } else {
            stat.num_bytes += snapset->get_clone_bytes(soid.snap);
          }
        }
      }

      // what's next?
      ++curclone;
      if (soid_error.errors) {
        this_chunk->m_inconsistent_objs.push_back(std::move(soid_error));
        ++soid_error_count;
      }
    }
    m_scrubber.add_to_stats(stat);
  }

  if (doing_clones(snapset, curclone)) {
    dout(10) << __func__ << " " << m_mode_desc << " " << m_pg_id
             << " No more objects while processing " << *head << dendl;

    missing +=
      process_clones_to(head, snapset, all_clones, &curclone, head_error);
  }

  // There could be missing found by the test above or even
  // before dropping out of the loop for the last head.

  if (missing) {
    log_missing(missing, head, __func__);
  }
  if (head && (head_error.errors || soid_error_count)) {
    this_chunk->m_inconsistent_objs.push_back(std::move(head_error));
  }

  // fix data/omap digests
  m_scrubber.submit_digest_fixes(this_chunk->missing_digest);

  dout(10) << __func__ << " (" << m_mode_desc << ") finish" << dendl;
}

int ScrubBackend::process_clones_to(
  const std::optional<hobject_t>& head,
  const std::optional<SnapSet>& snapset,
  std::optional<snapid_t> target,
  vector<snapid_t>::reverse_iterator* curclone,
  inconsistent_snapset_wrapper& e)
{
  ceph_assert(head);
  ceph_assert(snapset);
  int missing_count = 0;

  // NOTE: clones are in descending order, thus **curclone > target test here
  hobject_t next_clone(*head);
  while (doing_clones(snapset, *curclone) &&
         (!target || **curclone > *target)) {

    ++missing_count;
    // it is okay to be missing one or more clones in a cache tier.
    // skip higher-numbered clones in the list.
    if (!m_incomplete_clones_allowed) {
      next_clone.snap = **curclone;
      clog.error() << m_mode_desc << " " << m_pg_id << " " << *head
                    << " : expected clone " << next_clone << " " << m_missing
                    << " missing";
      this_chunk->m_error_counts.shallow_errors++;
      e.set_clone_missing(next_clone.snap);
    }
    // Clones are descending
    ++(*curclone);
  }
  return missing_count;
}

void ScrubBackend::log_missing(int missing,
                               const std::optional<hobject_t>& head,
                               const char* logged_func_name)
{
  ceph_assert(head);
  if (m_incomplete_clones_allowed) {
    dout(20) << logged_func_name << " " << m_mode_desc << " " << m_pg_id << " "
             << *head << " skipped " << missing << " clone(s) in cache tier"
             << dendl;
  } else {
    clog.info() << m_mode_desc << " " << m_pg_id << " " << *head << " : "
                 << missing << " missing clone(s)";
  }
}


// ////////////////////////////////////////////////////////////////////////////////

std::vector<snap_mapper_fix_t> ScrubBackend::scan_snaps(
  ScrubMap& smap,
  SnapMapReaderI& snaps_getter)
{
  std::vector<snap_mapper_fix_t> out_orders;
  hobject_t head;
  SnapSet snapset;

  // Test qa/standalone/scrub/osd-scrub-snaps.sh greps for the strings
  // in this function
  dout(15) << "_scan_snaps starts" << dendl;

  for (auto i = smap.objects.rbegin(); i != smap.objects.rend(); ++i) {

    const hobject_t& hoid = i->first;
    ScrubMap::object& o = i->second;

    dout(20) << __func__ << " " << hoid << dendl;

    ceph_assert(!hoid.is_snapdir());

    if (hoid.is_head()) {
      // parse the SnapSet
      bufferlist bl;
      if (o.attrs.find(SS_ATTR) == o.attrs.end()) {
	// no snaps for this head
	continue;
      }
      bl.push_back(o.attrs[SS_ATTR]);
      auto p = bl.cbegin();
      try {
	decode(snapset, p);
      } catch (...) {
	dout(20) << fmt::format("{}: failed to decode the snapset ({})",
				__func__,
				hoid)
		 << dendl;
	continue;
      }
      head = hoid.get_head();
      continue;
    }

    /// \todo document why guaranteed to have initialized 'head' at this point

    if (hoid.snap < CEPH_MAXSNAP) {

      if (hoid.get_head() != head) {
        derr << __func__ << " no head for " << hoid << " (have " << head << ")"
             << dendl;
        continue;
      }

      // the 'hoid' is a clone hoid at this point. The 'snapset' below was taken
      // from the corresponding head hoid.
      auto maybe_fix_order = scan_object_snaps(hoid, snapset, snaps_getter);
      if (maybe_fix_order) {
        out_orders.push_back(std::move(*maybe_fix_order));
      }
    }
  }

  dout(15) << __func__ << " " << out_orders.size() << " fix orders" << dendl;
  return out_orders;
}

std::optional<snap_mapper_fix_t> ScrubBackend::scan_object_snaps(
  const hobject_t& hoid,
  const SnapSet& snapset,
  SnapMapReaderI& snaps_getter)
{
  using result_t = Scrub::SnapMapReaderI::result_t;
  dout(15) << fmt::format("{}: obj:{} snapset:{}", __func__, hoid, snapset)
	   << dendl;

  auto p = snapset.clone_snaps.find(hoid.snap);
  if (p == snapset.clone_snaps.end()) {
    derr << __func__ << " no clone_snaps for " << hoid << " in " << snapset
         << dendl;
    return std::nullopt;
  }
  set<snapid_t> obj_snaps{p->second.begin(), p->second.end()};

  // clang-format off

  // validate both that the mapper contains the correct snaps for the object
  // and that it is internally consistent.
  // possible outcomes:
  //
  // Error scenarios:
  // - SnapMapper index of object snaps does not match that stored in head
  //   object snapset attribute:
  //    we should delete the snapmapper entry and re-add it.
  // - no mapping found for the object's snaps:
  //    we should add the missing mapper entries.
  // - the snapmapper set for this object is internally inconsistent (e.g.
  //    the OBJ_ entries do not match the SNA_ entries). We remove
  //    whatever entries are there, and redo the DB content for this object.
  //
  // And
  // There is the "happy path": cur_snaps == obj_snaps. Nothing to do there.

  // clang-format on

  auto cur_snaps = snaps_getter.get_snaps_check_consistency(hoid);
  if (!cur_snaps) {
    switch (auto e = cur_snaps.error(); e.code) {
      case result_t::code_t::backend_error:
	derr << __func__ << ": get_snaps returned "
	     << cpp_strerror(e.backend_error) << " for " << hoid << dendl;
	ceph_abort();
      case result_t::code_t::not_found:
	dout(10) << __func__ << ": no snaps for " << hoid << ". Adding."
		 << dendl;
	return snap_mapper_fix_t{snap_mapper_op_t::add, hoid, obj_snaps, {}};
      case result_t::code_t::inconsistent:
	dout(10) << __func__ << ": inconsistent snapmapper data for " << hoid
		 << ". Recreating." << dendl;
	return snap_mapper_fix_t{
	  snap_mapper_op_t::overwrite, hoid, obj_snaps, {}};
      default:
	dout(10) << __func__ << ": error (" << cpp_strerror(e.backend_error)
		 << ") fetching snapmapper data for " << hoid << ". Recreating."
		 << dendl;
	return snap_mapper_fix_t{
	  snap_mapper_op_t::overwrite, hoid, obj_snaps, {}};
    }
    __builtin_unreachable();
  }

  if (*cur_snaps == obj_snaps) {
    dout(20) << fmt::format(
		  "{}: {}: snapset match SnapMapper's ({})", __func__, hoid,
		  obj_snaps)
	     << dendl;
    return std::nullopt;
  }

  // add this object to the list of snapsets that needs fixing. Note
  // that we also collect the existing (bogus) list, for logging purposes
  dout(20) << fmt::format(
		"{}: obj {}: was: {} updating to: {}", __func__, hoid,
		*cur_snaps, obj_snaps)
	   << dendl;
  return snap_mapper_fix_t{
    snap_mapper_op_t::update, hoid, obj_snaps, *cur_snaps};
}

/*
 * Building a map of objects suitable for snapshot validation.
 *
 * We are moving all "full" clone sets, i.e. the head and (preceding it, as
 * snapshots precede the head entry) the clone entries, into 'for_meta_scrub'.
 * That collection, not containing partial items, will be scrubbed by
 * scrub_snapshot_metadata().
 *
 * What's left in m_cleaned_meta_map is the leftover partial items that need to
 * be completed before they can be processed.
 */
ScrubMap ScrubBackend::clean_meta_map(ScrubMap& cleaned, bool max_reached)
{
  ScrubMap for_meta_scrub;

  if (max_reached || cleaned.objects.empty()) {
    cleaned.swap(for_meta_scrub);
  } else {
    auto iter = cleaned.objects.end();
    --iter;  // not empty, see 'if' clause
    auto begin = cleaned.objects.begin();
    if (iter->first.has_snapset()) {
      ++iter;
    } else {
      while (iter != begin) {
        auto next = iter--;
        if (next->first.get_head() != iter->first.get_head()) {
          ++iter;
          break;
        }
      }
    }
    for_meta_scrub.objects.insert(begin, iter);
    cleaned.objects.erase(begin, iter);
  }

  return for_meta_scrub;
}
