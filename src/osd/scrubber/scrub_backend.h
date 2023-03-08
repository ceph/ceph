// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

// clang-format off
/*
      +------------------------+
      |                        |
      |   PgScrubber           |
      |                        |-----------------------------+
      |                        |                             |
      +------------------------+                             | ownes & uses
      |   PrimaryLogScrub      |                             |
      +------------------------+                             |
                                                             |
                                                             |
                                                             v
                                  +-------------------------------------------+
                                  |ScrubBackend                               |
        +----------------+        |============                               |
        |  this_chunk    |        |                                           |
        | (scrub_chunk_t)|<-------| + decode_received_map()                   |
        +----------------+        | + scrub_compare_maps()                    |
                                  | + scan_snaps()                            |
                                  | .....                                     |
                                  |                                           |
                                  |                                           |
                                  +--------------------/-------------\--------+
                                         --/          /               \
                                      --/             |               |
                                   --/               /                 \
                                 -/            uses  |            uses |
                        uses  --/                   /                   \
                           --/                     /                    |
                        --/                        |                     \
                       v                           v                     v
                  PgBackend             PG/PrimaryLogPG          OSD Services


*/
// clang-format on

#include <fmt/core.h>
#include <fmt/format.h>

#include <string_view>

#include "common/LogClient.h"
#include "osd/OSDMap.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"
#include "osd/SnapMapReaderI.h"

struct ScrubMap;

class PG;
class PgScrubber;
struct PGPool;
using Scrub::PgScrubBeListener;

using data_omap_digests_t =
  std::pair<std::optional<uint32_t>, std::optional<uint32_t>>;

/// a list of fixes to be performed on objects' digests
using digests_fixes_t = std::vector<std::pair<hobject_t, data_omap_digests_t>>;

using shard_info_map_t = std::map<pg_shard_t, shard_info_wrapper>;
using shard_to_scrubmap_t = std::map<pg_shard_t, ScrubMap>;

using auth_peers_t = std::vector<std::pair<ScrubMap::object, pg_shard_t>>;

using wrapped_err_t =
  std::variant<inconsistent_obj_wrapper, inconsistent_snapset_wrapper>;
using inconsistent_objs_t = std::vector<wrapped_err_t>;

/// omap-specific stats
struct omap_stat_t {
  int large_omap_objects{0};
  int64_t omap_bytes{0};
  int64_t omap_keys{0};
};

struct error_counters_t {
  int shallow_errors{0};
  int deep_errors{0};
};

// the PgScrubber services used by the backend
struct ScrubBeListener {
  virtual std::ostream& gen_prefix(std::ostream& out) const = 0;
  virtual CephContext* get_pg_cct() const = 0;
  virtual LoggerSinkSet& get_logger() const = 0;
  virtual bool is_primary() const = 0;
  virtual spg_t get_pgid() const = 0;
  virtual const OSDMapRef& get_osdmap() const = 0;
  virtual void add_to_stats(const object_stat_sum_t& stat) = 0;
  virtual void submit_digest_fixes(const digests_fixes_t& fixes) = 0;
  virtual ~ScrubBeListener() = default;
};

// As the main scrub-backend entry point - scrub_compare_maps() - must
// be able to return both a list of snap fixes and a list of inconsistent
// objects:
struct objs_fix_list_t {
  inconsistent_objs_t inconsistent_objs;
  std::vector<Scrub::snap_mapper_fix_t> snap_fix_list;
};

/**
 * A structure used internally by select_auth_object()
 *
 * Conveys the usability of a specific shard as an auth source.
 */
struct shard_as_auth_t {
  // note: 'not_found' differs from 'not_usable' in that 'not_found'
  // does not carry an error message to be cluster-logged.
  enum class usable_t : uint8_t { not_usable, not_found, usable };

  // the ctor used when the shard should not be considered as auth
  explicit shard_as_auth_t(std::string err_msg)
      : possible_auth{usable_t::not_usable}
      , error_text{err_msg}
      , oi{}
      , auth_iter{}
      , digest{std::nullopt}
  {}

  // the object cannot be found on the shard
  explicit shard_as_auth_t()
      : possible_auth{usable_t::not_found}
      , error_text{}
      , oi{}
      , auth_iter{}
      , digest{std::nullopt}
  {}

  shard_as_auth_t(std::string err_msg, std::optional<uint32_t> data_digest)
      : possible_auth{usable_t::not_usable}
      , error_text{err_msg}
      , oi{}
      , auth_iter{}
      , digest{data_digest}
  {}

  // possible auth candidate
  shard_as_auth_t(const object_info_t& anoi,
                  shard_to_scrubmap_t::iterator it,
                  std::string err_msg,
                  std::optional<uint32_t> data_digest)
      : possible_auth{usable_t::usable}
      , error_text{err_msg}
      , oi{anoi}
      , auth_iter{it}
      , digest{data_digest}
  {}


  usable_t possible_auth;
  std::string error_text;
  object_info_t oi;
  shard_to_scrubmap_t::iterator auth_iter;
  std::optional<uint32_t> digest;
  // when used for Crimson, we'll probably want to return 'digest_match' (and
  // other in/out arguments) via this struct
};

// the format specifier {D} is used to request debug output
template <>
struct fmt::formatter<shard_as_auth_t> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end()) {
      debug_log = (*it++) == 'D';
    }
    return it;
  }
  template <typename FormatContext>
  auto format(shard_as_auth_t const& as_auth, FormatContext& ctx)
  {
    if (debug_log) {
      // note: 'if' chain, as hard to consistently (on all compilers) avoid some
      // warnings for a switch plus multiple return paths
      if (as_auth.possible_auth == shard_as_auth_t::usable_t::not_usable) {
        return fmt::format_to(ctx.out(),
                              "{{shard-not-usable:{}}}",
                              as_auth.error_text);
      }
      if (as_auth.possible_auth == shard_as_auth_t::usable_t::not_found) {
        return fmt::format_to(ctx.out(), "{{shard-not-found}}");
      }
      return fmt::format_to(ctx.out(),
                            "{{shard-usable: soid:{} {{txt:{}}} }}",
                            as_auth.oi.soid,
                            as_auth.error_text);

    } else {
      return fmt::format_to(
        ctx.out(),
        "usable:{} soid:{} {{txt:{}}}",
        (as_auth.possible_auth == shard_as_auth_t::usable_t::usable) ? "yes"
                                                                     : "no",
        as_auth.oi.soid,
        as_auth.error_text);
    }
  }

  bool debug_log{false};
};

struct auth_selection_t {
  shard_to_scrubmap_t::iterator auth;  ///< an iter into one of this_chunk->maps
  pg_shard_t auth_shard;               // set to auth->first
  object_info_t auth_oi;
  shard_info_map_t shard_map;
  bool is_auth_available{false};  ///< managed to select an auth' source?
  bool digest_match{true};        ///< do all (existing) digests match?
};

// note: some scrub tests are sensitive to the specific format of
// auth_selection_t listing in the logs
template <>
struct fmt::formatter<auth_selection_t> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(auth_selection_t const& aus, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(),
                          " {{AU-S: {}->{:x} OI({:x}:{}) {} dm:{}}} ",
                          aus.auth->first,
                          (uint64_t)(&aus.auth->second),
                          (uint64_t)(&aus.auth_oi),
                          aus.auth_oi,
                          aus.shard_map.size(),
                          aus.digest_match);
  }
};

/**
 * the back-end data that is per-chunk
 *
 * Created by the Scrubber after all replicas' maps have arrived.
 */
struct scrub_chunk_t {

  explicit scrub_chunk_t(pg_shard_t i_am) { received_maps[i_am] = ScrubMap{}; }

  /// the working set of scrub maps: the received maps, plus
  /// Primary's own map.
  std::map<pg_shard_t, ScrubMap> received_maps;

  /// a collection of all objs mentioned in the maps
  std::set<hobject_t> authoritative_set;

  utime_t started{ceph_clock_now()};

  digests_fixes_t missing_digest;

  /// Map from object with errors to good peers
  std::map<hobject_t, std::list<pg_shard_t>> authoritative;

  inconsistent_objs_t m_inconsistent_objs;

  /// shallow/deep error counters
  error_counters_t m_error_counts;

  // these must be reset for each element:

  std::set<pg_shard_t> cur_missing;
  std::set<pg_shard_t> cur_inconsistent;
  bool fix_digest{false};
};


/**
 * ScrubBackend wraps the data and operations required for the back-end part of
 * the scrubbing (i.e. for comparing the maps and fixing objects).
 *
 * Created anew upon each initiation of a scrub session.
 */
class ScrubBackend {
 public:
  // Primary constructor
  ScrubBackend(ScrubBeListener& scrubber,
               PgScrubBeListener& pg,
               pg_shard_t i_am,
               bool repair,
               scrub_level_t shallow_or_deep,
               const std::set<pg_shard_t>& acting);

  // Replica constructor: no primary map
  ScrubBackend(ScrubBeListener& scrubber,
               PgScrubBeListener& pg,
               pg_shard_t i_am,
               bool repair,
               scrub_level_t shallow_or_deep);

  friend class PgScrubber;
  friend class TestScrubBackend;

  /**
   * reset the per-chunk data structure (scrub_chunk_t).
   * Create an empty scrub-map for this shard, and place it
   * in the appropriate entry in 'received_maps'.
   *
   * @returns a pointer to the newly created ScrubMap.
   */
  void new_chunk();

  ScrubMap& get_primary_scrubmap();

  /**
   * sets Backend's m_repair flag (setting m_mode_desc to a corresponding
   * string)
   */
  void update_repair_status(bool should_repair);

  std::vector<Scrub::snap_mapper_fix_t> replica_clean_meta(
    ScrubMap& smap,
    bool max_reached,
    const hobject_t& start,
    Scrub::SnapMapReaderI& snaps_getter);

  /**
   * decode the arriving MOSDRepScrubMap message, placing the replica's
   * scrub-map into received_maps[from].
   *
   * @param from replica
   */
  void decode_received_map(pg_shard_t from, const MOSDRepScrubMap& msg);

  objs_fix_list_t scrub_compare_maps(bool max_reached,
				     Scrub::SnapMapReaderI& snaps_getter);

  int scrub_process_inconsistent();

  const omap_stat_t& this_scrub_omapstats() const { return m_omap_stats; }

  int authoritative_peers_count() const { return m_auth_peers.size(); };

  std::ostream& logger_prefix(std::ostream* _dout, const ScrubBackend* t);

 private:
  // set/constructed at the ctor():
  ScrubBeListener& m_scrubber;
  Scrub::PgScrubBeListener& m_pg;
  const pg_shard_t m_pg_whoami;
  bool m_repair;
  const scrub_level_t m_depth;
  const spg_t m_pg_id;
  std::vector<pg_shard_t> m_acting_but_me;  // primary only
  bool m_is_replicated{true};
  std::string_view m_mode_desc;
  std::string m_formatted_id;
  const PGPool& m_pool;
  bool m_incomplete_clones_allowed{false};

  /// collecting some scrub-session-wide omap stats
  omap_stat_t m_omap_stats;

  /// Mapping from object with errors to good peers
  std::map<hobject_t, auth_peers_t> m_auth_peers;

  // shorthands:
  ConfigProxy& m_conf;
  LoggerSinkSet& clog;

 private:

  struct auth_and_obj_errs_t {
    std::list<pg_shard_t> auth_list;
    std::set<pg_shard_t> object_errors;
  };

  std::optional<scrub_chunk_t> this_chunk;

  /// Maps from objects with errors to missing peers
  HobjToShardSetMapping m_missing;  // used by scrub_process_inconsistent()

  /// Maps from objects with errors to inconsistent peers
  HobjToShardSetMapping m_inconsistent;  // used by scrub_process_inconsistent()

  /// Cleaned std::map pending snap metadata scrub
  ScrubMap m_cleaned_meta_map{};

  /// a reference to the primary map
  ScrubMap& my_map();

  /// shallow/deep error counters
  error_counters_t get_error_counts() const { return this_chunk->m_error_counts; }

  /**
   *  merge_to_authoritative_set() updates
   *   - this_chunk->maps[from] with the replicas' scrub-maps;
   *   - this_chunk->authoritative_set as a union of all the maps' objects;
   */
  void merge_to_authoritative_set();

  // note: used by both Primary & replicas
  static ScrubMap clean_meta_map(ScrubMap& cleaned, bool max_reached);

  void compare_smaps();

  /// might return error messages to be cluster-logged
  std::optional<std::string> compare_obj_in_maps(const hobject_t& ho);

  void omap_checks();

  std::optional<auth_and_obj_errs_t> for_empty_auth_list(
    std::list<pg_shard_t>&& auths,
    std::set<pg_shard_t>&& obj_errors,
    shard_to_scrubmap_t::iterator auth,
    const hobject_t& ho,
    std::stringstream& errstream);

  auth_and_obj_errs_t match_in_shards(const hobject_t& ho,
                                      auth_selection_t& auth_sel,
                                      inconsistent_obj_wrapper& obj_result,
                                      std::stringstream& errstream);

  // returns: true if a discrepancy was found
  bool compare_obj_details(pg_shard_t auth_shard,
                           const ScrubMap::object& auth,
                           const object_info_t& auth_oi,
                           const ScrubMap::object& candidate,
                           shard_info_wrapper& shard_result,
                           inconsistent_obj_wrapper& obj_result,
                           std::stringstream& errorstream,
                           bool has_snapset);

  void repair_object(const hobject_t& soid,
                     const auth_peers_t& ok_peers,
                     const std::set<pg_shard_t>& bad_peers);

  /**
   * An auxiliary used by select_auth_object() to test a specific shard
   * as a possible auth candidate.
   * @param ho        the hobject for which we are looking for an auth source
   * @param srd       the candidate shard
   * @param shard_map [out] a collection of shard_info-s per shard.
   * possible_auth_shard() might set error flags in the relevant (this shard's)
   * entry.
   */
  shard_as_auth_t possible_auth_shard(const hobject_t& ho,
                                      const pg_shard_t& srd,
                                      shard_info_map_t& shard_map);

  auth_selection_t select_auth_object(const hobject_t& ho,
                                      std::stringstream& errstream);


  enum class digest_fixing_t { no, if_aged, force };

  /*
   *  an aux used by inconsistents() to determine whether to fix the digest
   */
  [[nodiscard]] digest_fixing_t should_fix_digest(
    const hobject_t& ho,
    const ScrubMap::object& auth_object,
    const object_info_t& auth_oi,
    bool repair_flag,
    std::stringstream& errstream);

  void inconsistents(const hobject_t& ho,
                     ScrubMap::object& auth_object,
                     object_info_t& auth_oi,  // consider moving to object
                     auth_and_obj_errs_t&& auth_n_errs,
                     std::stringstream& errstream);

  int process_clones_to(const std::optional<hobject_t>& head,
                        const std::optional<SnapSet>& snapset,
                        std::optional<snapid_t> target,
                        std::vector<snapid_t>::reverse_iterator* curclone,
                        inconsistent_snapset_wrapper& e);

  /**
   * Validate consistency of the object info and snap sets.
   */
  void scrub_snapshot_metadata(ScrubMap& map);

  /**
   *  Updates the "global" (i.e. - not 'per-chunk') databases:
   *   - in m_authoritative: a list of good peers for each "problem" object in
   *     the current chunk;
   *   - in m_cleaned_meta_map: a "cleaned" version of the object (the one from
   *     the selected shard).
   */
  void update_authoritative();

  void log_missing(int missing,
                   const std::optional<hobject_t>& head,
                   const char* logged_func_name);

  /**
   * returns a list of snaps "fix orders"
   */
  std::vector<Scrub::snap_mapper_fix_t> scan_snaps(
    ScrubMap& smap,
    Scrub::SnapMapReaderI& snaps_getter);

  /**
   * an aux used by scan_snaps(), possibly returning a fix-order
   * for a specific hobject.
   */
  std::optional<Scrub::snap_mapper_fix_t> scan_object_snaps(
    const hobject_t& hoid,
    const SnapSet& snapset,
    Scrub::SnapMapReaderI& snaps_getter);

  // accessing the PG backend for this translation service
  uint64_t logical_to_ondisk_size(uint64_t logical_size) const;
};

template <>
struct fmt::formatter<data_omap_digests_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const data_omap_digests_t& dg, FormatContext& ctx)
  {
    // can't use value_or() due to different output types
    if (std::get<0>(dg).has_value()) {
      fmt::format_to(ctx.out(), "[{:#x}/", std::get<0>(dg).value());
    } else {
      fmt::format_to(ctx.out(), "[---/");
    }
    if (std::get<1>(dg).has_value()) {
      return fmt::format_to(ctx.out(), "{:#x}]", std::get<1>(dg).value());
    } else {
      return fmt::format_to(ctx.out(), "---]");
    }
  }
};

template <>
struct fmt::formatter<std::pair<hobject_t, data_omap_digests_t>> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const std::pair<hobject_t, data_omap_digests_t>& x,
	      FormatContext& ctx) const
  {
    return fmt::format_to(ctx.out(),
			  "{{ {} - {} }}",
			  std::get<0>(x),
			  std::get<1>(x));
  }
};
