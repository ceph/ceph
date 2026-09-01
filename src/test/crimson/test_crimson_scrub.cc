// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/filter_iterator.hpp>

#include <fmt/ranges.h>

#include <seastar/core/sleep.hh>

#include "test/crimson/gtest_seastar.h"

#include "include/rados/rados_types.hpp"
#include "common/scrub_types.h"
#include "crimson/common/interruptible_future.h"
#include "crimson/osd/scrub/scrub_machine.h"
#include "crimson/osd/scrub/scrub_validator.h"

#include "osd/osd_types_fmt.h"

constexpr static size_t TEST_MAX_OBJECT_SIZE = 128<<20;
constexpr static std::string_view TEST_INTERNAL_NAMESPACE = ".internal";
constexpr static uint64_t TEST_OMAP_KEY_LIMIT = 200000;
constexpr static size_t TEST_OMAP_BYTES_LIMIT = 1<<30;

void so_set_attr_len(ScrubMap::object &obj, const std::string &name, size_t len)
{
  obj.attrs[name] = bufferlist();
  obj.attrs[name].push_back(buffer::ptr(len));
}

void so_set_attr(ScrubMap::object &obj, const std::string &name, bufferlist bl)
{
  bl.rebuild();
  obj.attrs[name] = bl;
}

std::optional<bufferlist> so_get_attr(
  ScrubMap::object &obj, const std::string &name)
{
  if (obj.attrs.count(name)) {
    return obj.attrs[name];
  } else {
    return std::nullopt;
  }
}

template <typename T>
void so_set_attr_type(
  ScrubMap::object &obj, const std::string &name,
  const std::optional<T> &v)
{
  if (v) {
    bufferlist bl;
    encode(*v, bl, CEPH_FEATURES_ALL);
    so_set_attr(obj, name, std::move(bl));
  } else {
    obj.attrs.erase(name);
  }
}

template <typename T>
std::optional<T> so_get_attr_type(ScrubMap::object &obj, const std::string &name)
{
  auto maybe_bl = so_get_attr(obj, name);
  if (!maybe_bl) {
    return std::nullopt;
  }
  auto bl = std::move(*maybe_bl);
  try {
    T ret;
    auto bliter = bl.cbegin();
    decode(ret, bliter);
    return ret;
  } catch (...) {
    return std::nullopt;
  }
}

void so_set_oi(ScrubMap::object &obj, const std::optional<object_info_t> &oi)
{
  return so_set_attr_type<object_info_t>(obj, OI_ATTR, oi);
}

std::optional<object_info_t> so_get_oi(ScrubMap::object &obj)
{
  return so_get_attr_type<object_info_t>(obj, OI_ATTR);
}

template <typename F>
void so_mut_oi(ScrubMap::object &obj, F &&f) {
  so_set_oi(obj, std::invoke(std::forward<F>(f), so_get_oi(obj)));
}

void so_set_ss(ScrubMap::object &obj, const std::optional<SnapSet> &ss)
{
  return so_set_attr_type<SnapSet>(obj, SS_ATTR, ss);
}

std::optional<SnapSet> so_get_ss(ScrubMap::object &obj)
{
  return so_get_attr_type<SnapSet>(obj, SS_ATTR);
}

template <typename F>
void so_mut_ss(ScrubMap::object &obj, F &&f) {
  so_set_ss(obj, std::invoke(std::forward<F>(f), so_get_ss(obj)));
}

/**
 * so_builder_t
 *
 * Stores ScrubMap::object data without live bufferlist objects so that
 * test_obj_t can be safely held in GTest's global parameter registry.
 * Attrs are stored as raw byte strings (std::string) to avoid mempool-tracked
 * bufferlist objects in global storage. The live ScrubMap::object with proper
 * bufferlists is assembled on-demand by get().
 */
struct so_builder_t {
  // Scalars copied from ScrubMap::object (all POD — no mempool tracking)
  uint64_t size{0};
  uint32_t digest{0};
  bool digest_present{false};
  uint32_t omap_digest{0};
  bool omap_digest_present{false};
  bool read_error{false};
  bool stat_error{false};
  bool ec_hash_mismatch{false};
  bool ec_size_mismatch{false};
  uint64_t object_omap_keys{0};
  uint64_t object_omap_bytes{0};
  // Attrs as raw bytes — std::string uses the default allocator, not mempool
  std::map<std::string, std::string> attrs_raw;

  static hobject_t make_hoid(std::string name, snapid_t cloneid=CEPH_NOSNAP) {
    auto oid = object_t(name);
    return hobject_t{
      oid,
      "",
      cloneid,
      static_cast<uint32_t>(std::hash<object_t>()(oid)),
      1,
      ""
    };
  }

  // ---- attr helpers -------------------------------------------------------

  template <typename T>
  void set_attr_encoded(const std::string &name, const std::optional<T> &val) {
    if (!val) {
      attrs_raw.erase(name);
      return;
    }
    bufferlist bl;
    encode(*val, bl, CEPH_FEATURES_ALL);
    bl.rebuild();
    attrs_raw[name] = std::string(bl.c_str(), bl.length());
  }

  void set_attr_len(const std::string &name, size_t len) {
    attrs_raw[name] = std::string(len, '\0');
  }

  template <typename T>
  std::optional<T> get_attr_encoded(const std::string &name) const {
    auto it = attrs_raw.find(name);
    if (it == attrs_raw.end()) return std::nullopt;
    bufferlist bl;
    bl.append(it->second);
    try {
      T ret;
      auto bliter = bl.cbegin();
      decode(ret, bliter);
      return ret;
    } catch (...) {
      return std::nullopt;
    }
  }

  // ---- factory methods ----------------------------------------------------

  static so_builder_t make_head(std::string name) {
    auto hoid = make_hoid(name);
    so_builder_t ret;
    ret.set_attr_encoded(OI_ATTR, std::make_optional<object_info_t>(hoid));
    ret.set_attr_encoded(SS_ATTR, std::make_optional<SnapSet>());
    return ret;
  }

  static so_builder_t make_clone(std::string name, snapid_t cloneid = 4) {
    auto hoid = make_hoid(name, cloneid);
    so_builder_t ret;
    ret.set_attr_encoded(OI_ATTR, std::make_optional<object_info_t>(hoid));
    return ret;
  }

  // ---- mutation helpers ---------------------------------------------------

  so_builder_t &set_size(size_t sz) {
    size = sz;
    auto oi = get_attr_encoded<object_info_t>(OI_ATTR);
    if (oi) {
      oi->size = sz;
      set_attr_encoded(OI_ATTR, oi);
    }
    return *this;
  }

  so_builder_t &add_attr(const std::string &name, size_t len) {
    set_attr_len(name, len);
    return *this;
  }

  // Mutate the OI attr in-place
  template <typename F>
  so_builder_t &mut_oi(F &&f) {
    set_attr_encoded(OI_ATTR,
      std::invoke(std::forward<F>(f),
        get_attr_encoded<object_info_t>(OI_ATTR)));
    return *this;
  }

  // Mutate the SS attr in-place
  template <typename F>
  so_builder_t &mut_ss(F &&f) {
    set_attr_encoded(SS_ATTR,
      std::invoke(std::forward<F>(f),
        get_attr_encoded<SnapSet>(SS_ATTR)));
    return *this;
  }

  // ---- assembly -----------------------------------------------------------

  /// Build a live ScrubMap::object.  Called at test-body time, never at
  /// GTest-registration time, so bufferlists only exist while the Seastar
  /// reactor (and mempool) is running.
  ScrubMap::object get() const {
    ScrubMap::object obj;
    obj.size = size;
    obj.digest = digest;
    obj.digest_present = digest_present;
    obj.omap_digest = omap_digest;
    obj.omap_digest_present = omap_digest_present;
    obj.read_error = read_error;
    obj.stat_error = stat_error;
    obj.ec_hash_mismatch = ec_hash_mismatch;
    obj.ec_size_mismatch = ec_size_mismatch;
    obj.object_omap_keys = object_omap_keys;
    obj.object_omap_bytes = object_omap_bytes;
    for (const auto &[name, raw] : attrs_raw) {
      bufferlist bl;
      bl.append(raw);
      obj.attrs[name] = std::move(bl);
    }
    return obj;
  }
};

/**
 * test_obj_t
 *
 * test param combining an so_builder_t with human readable description with
 * a stripe_info.
 */
struct test_obj_t : so_builder_t {
  std::string desc;
  hobject_t hoid;

  test_obj_t(
    so_builder_t _builder,
    std::string _desc,
    hobject_t _hoid) :
    so_builder_t(std::move(_builder)),
    desc(std::move(_desc)),
    hoid(std::move(_hoid)) {
    ceph_assert(!desc.empty());
  }

  static test_obj_t make(
    const std::string &desc,
    so_builder_t builder) {
    auto oi = builder.get_attr_encoded<object_info_t>(OI_ATTR);
    ceph_assert(oi.has_value());
    hobject_t hoid = oi->soid;
    return test_obj_t{
      std::move(builder),
      desc,
      std::move(hoid)};
  }

  template <typename... Args>
  static test_obj_t make_head(const std::string &desc, Args&&... args) {
    return make(
      desc,
      so_builder_t::make_head(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static test_obj_t make_clone(const std::string &desc, Args&&... args) {
    return make(
      desc,
      so_builder_t::make_clone(std::forward<Args>(args)...));
  }

  test_obj_t &set_size(size_t size) {
    so_builder_t::set_size(size);
    return *this;
  }

  test_obj_t &add_attr(const std::string &name, size_t len) {
    so_builder_t::add_attr(name, len);
    return *this;
  }

  ScrubMap::object get() const {
    return so_builder_t::get();
  }
};

/**
 * Interface for a test case on a single object.
 */
struct SingleErrorTestCase {
  /// Describes limitations on test preconditions
  enum class restriction_t {
    NONE,         /// No limitations
    REPLICA_ONLY, /// Only works if injected on replica
    EC_ONLY,      /// Only valid for ec objects
    HEAD_ONLY     /// Only valid for head objects
  };

  /// returns human-readable string describing the test for debugging
  virtual std::string_view get_description() const = 0;

  /// returns test_obj_t with error injected
  virtual test_obj_t adjust_base_object(test_obj_t ret) const {
    return ret;
  }

  /// returns test_obj_t with error injected
  virtual test_obj_t inject_error(test_obj_t) const = 0;

  /// returns expected shard error for target shard (is_primary selects variant)
  virtual librados::err_t get_shard_error_sig(bool is_primary = false) const = 0;

  /// returns expected object error
  virtual librados::obj_err_t get_object_error_sig() const = 0;

  /// returns true if selected_oi may be on the target shard (e.g. digest errors)
  virtual bool selected_oi_may_be_on_target() const { return false; }

  /// returns true if missing_digest may be non-empty for this test case
  virtual bool missing_digest_may_be_non_empty() const { return false; }

  /// returns true if test should be run with passed restriction
  virtual bool valid_for_restriction(restriction_t restriction) const = 0;

  virtual ~SingleErrorTestCase() = default;
};

/// Utility template for implementing SimpleErrorTestCase
template <typename T>
struct SingleErrorTestCaseT : SingleErrorTestCase {
  /// Defaults for REQUIRE_EC and REQUIRES_HEAD
  constexpr static bool REQUIRES_EC = false;
  constexpr static bool REQUIRES_HEAD = false;

  /* Every implementor must define:
  constexpr static librados::err_t shard_error_sig{
  };
  constexpr static librados::obj_err_t object_error_sig{
  };
  */

  librados::err_t get_shard_error_sig(bool /*is_primary*/ = false) const override {
    return T::shard_error_sig;
  }
  librados::obj_err_t get_object_error_sig() const final {
    return T::object_error_sig;
  }

  constexpr static bool requires_ec() {
    return T::REQUIRES_EC;
  }
  constexpr static bool requires_head() {
    return T::REQUIRES_HEAD;
  }
  constexpr static bool requires_replica() {
    /* If there are no shard_errors, we'll take primary to be authoritative. */
    return T::shard_error_sig.errors == 0;
  }

  bool valid_for_restriction(restriction_t restriction) const final {
    // There aren't currently any tests with two restrictions, if this
    // changes, the suite instantiations will need to change as well.
    static_assert(
      (requires_ec() + requires_head() + requires_replica()) <= 1);
    return [] {
      if constexpr (requires_replica()) {
	return restriction_t::REPLICA_ONLY;
      } else if constexpr (requires_head()) {
	return restriction_t::HEAD_ONLY;
      } else if constexpr (requires_ec()) {
	return restriction_t::EC_ONLY;
      } else {
	return restriction_t::NONE;
      }
    }() == restriction;
  }
  virtual ~SingleErrorTestCaseT() = default;
};

/* The following classes exercise each possible error code detected
 * by evaluate_object_shard and compare_candidate_to_authoritative
 * in crimson/osd/scrub/scrub_validator.*
 *
 * Note, any newly added cases must also be added to the test_cases
 * array below.
 */

struct ECHashMismatch : SingleErrorTestCaseT<ECHashMismatch> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SHARD_EC_HASH_MISMATCH
  };
  constexpr static librados::obj_err_t object_error_sig{
  };

  std::string_view get_description() const {
    return "ECHashMismatch";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    obj.ec_hash_mismatch = true;
    return obj;
  }
};

struct ECSizeMismatch : SingleErrorTestCaseT<ECSizeMismatch> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SHARD_EC_SIZE_MISMATCH
  };
  constexpr static librados::obj_err_t object_error_sig{
  };

  std::string_view get_description() const {
    return "ECSizeMismatch";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    obj.ec_size_mismatch = true;
    return obj;
  }
};

struct ReadError : SingleErrorTestCaseT<ReadError> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SHARD_READ_ERR
  };
  constexpr static librados::obj_err_t object_error_sig{};

  std::string_view get_description() const {
    return "ReadError";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    obj.read_error = true;
    return obj;
  }
};

struct StatError : SingleErrorTestCaseT<StatError> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SHARD_STAT_ERR
  };
  constexpr static librados::obj_err_t object_error_sig{
  };

  std::string_view get_description() const {
    return "StatError";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    obj.stat_error = true;
    return obj;
  }
};

struct MissingOI : SingleErrorTestCaseT<MissingOI> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::INFO_MISSING
  };
  // Implementation guards OI comparison when candidate has INFO_MISSING/
  // INFO_CORRUPTED (matching classic scrub_backend.cc line 1564), so no
  // OBJECT_INFO_INCONSISTENCY is set at the object level — the shard-level
  // error bit in union_shards already captures it.
  constexpr static librados::obj_err_t object_error_sig{};

  std::string_view get_description() const {
    return "MissingOI";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    obj.set_attr_encoded<object_info_t>(OI_ATTR, std::nullopt);
    return obj;
  }
};

struct CorruptOI: SingleErrorTestCaseT<CorruptOI> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::INFO_CORRUPTED
  };
  // Same guard as MissingOI — OBJECT_INFO_INCONSISTENCY is suppressed when
  // the candidate has INFO_MISSING or INFO_CORRUPTED.
  constexpr static librados::obj_err_t object_error_sig{};

  std::string_view get_description() const {
    return "CorruptOI";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    obj.set_attr_len(OI_ATTR, 10);
    return obj;
  }
};

struct CorruptOndiskSize : SingleErrorTestCaseT<CorruptOndiskSize> {
  // Both primary and replica: the target's physical size differs from its own
  // OI size (OBJ_SIZE_INFO_MISMATCH, set per-shard) AND from the auth shard's
  // physical size (SIZE_MISMATCH_INFO, set cross-shard).  Both flags are set
  // in every injection scenario.
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SIZE_MISMATCH_INFO
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::SIZE_MISMATCH
  };

  librados::err_t get_shard_error_sig(bool /*is_primary*/ = false) const override {
    librados::err_t sig{};
    sig.errors = librados::err_t::SIZE_MISMATCH_INFO |
                 librados::err_t::OBJ_SIZE_INFO_MISMATCH;
    return sig;
  }

  std::string_view get_description() const {
    return "CorruptOndiskSize";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    obj.size += 2;  // physical size only — do NOT update OI size
    return obj;
  }
};

struct MissingSS : SingleErrorTestCaseT<MissingSS> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SNAPSET_MISSING
  };
  // Implementation suppresses SNAPSET_INCONSISTENCY at the object level when
  // either shard has a bad snapset (missing or corrupted): the shard-level
  // error already captures the problem in union_shards, matching classic
  // scrub_backend.cc behaviour.
  constexpr static librados::obj_err_t object_error_sig{};
  constexpr static bool REQUIRES_HEAD = true;

  std::string_view get_description() const {
    return "MissingSS";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(obj.hoid.is_head());
    obj.set_attr_encoded<SnapSet>(SS_ATTR, std::nullopt);
    return obj;
  }
};

struct CorruptSS : SingleErrorTestCaseT<CorruptSS> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SNAPSET_CORRUPTED
  };
  // Same suppression as MissingSS — shard-level error in union_shards suffices.
  constexpr static librados::obj_err_t object_error_sig{};
  constexpr static bool REQUIRES_HEAD = true;

  std::string_view get_description() const {
    return "CorruptSS";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(obj.hoid.is_head());
    obj.set_attr_len(SS_ATTR, 10);
    return obj;
  }
};

struct DataDigestMismatch : SingleErrorTestCaseT<DataDigestMismatch> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::DATA_DIGEST_MISMATCH_INFO
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::DATA_DIGEST_MISMATCH
  };

  // When injected on primary, primary IS the auth shard → selected_oi is on
  // the target shard; the non-target (replica) shard has no selected_oi.
  bool selected_oi_may_be_on_target() const override { return true; }
  // When injected on primary the auth's computed digest (2) ≠ OI digest (1),
  // so the implementation records a write-back entry in missing_digest.
  bool missing_digest_may_be_non_empty() const override { return true; }

  std::string_view get_description() const {
    return "DataDigestMismatch";
  };
  test_obj_t adjust_base_object(test_obj_t obj) const {
    obj.mut_oi([](auto maybe_oi) {
      ceph_assert(maybe_oi);
      maybe_oi->set_data_digest(1);
      return maybe_oi;
    });
    obj.digest_present = true;
    obj.digest = 1;
    return obj;
  }
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(obj.get_attr_encoded<object_info_t>(OI_ATTR)->is_data_digest());
    obj.digest = 2;
    return obj;
  }
};

struct OmapDigestMismatch : SingleErrorTestCaseT<OmapDigestMismatch> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::OMAP_DIGEST_MISMATCH_INFO
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::OMAP_DIGEST_MISMATCH
  };

  // Same rationale as DataDigestMismatch above.
  bool selected_oi_may_be_on_target() const override { return true; }
  bool missing_digest_may_be_non_empty() const override { return true; }

  std::string_view get_description() const {
    return "OmapDigestMismatch";
  };
  test_obj_t adjust_base_object(test_obj_t obj) const {
    obj.mut_oi([](auto maybe_oi) {
      ceph_assert(maybe_oi);
      maybe_oi->set_omap_digest(1);
      return maybe_oi;
    });
    obj.omap_digest_present = true;
    obj.omap_digest = 1;
    return obj;
  }
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(obj.get_attr_encoded<object_info_t>(OI_ATTR)->is_omap_digest());
    obj.omap_digest = 2;
    return obj;
  }
};

struct ExtraAttribute : SingleErrorTestCaseT<ExtraAttribute> {
  constexpr static librados::err_t shard_error_sig{};
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::ATTR_NAME_MISMATCH
  };

  std::string_view get_description() const {
    return "ExtraAttribute";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    obj.set_attr_len("attr_added_erroneously", 10);
    return obj;
  }
};

struct MissingAttribute : SingleErrorTestCaseT<MissingAttribute> {
  constexpr static librados::err_t shard_error_sig{};
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::ATTR_NAME_MISMATCH
  };

  std::string_view get_description() const {
    return "MissingAttribute";
  };
  test_obj_t adjust_base_object(test_obj_t obj) const {
    obj.set_attr_len("attr_to_be_missing", 10);
    return obj;
  }
  test_obj_t inject_error(test_obj_t obj) const {
    obj.attrs_raw.erase("attr_to_be_missing");
    return obj;
  }
};

template <>
struct fmt::formatter<SingleErrorTestCase> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const auto &test_case, FormatContext& ctx) const
  {
    return fmt::format_to(
      ctx.out(), "{}",
      test_case.get_description());
  }
};

std::unique_ptr<SingleErrorTestCase> test_cases[] = {
  std::make_unique<ECHashMismatch>(),
  std::make_unique<ECSizeMismatch>(),
  std::make_unique<ReadError>(),
  std::make_unique<StatError>(),
  std::make_unique<MissingOI>(),
  std::make_unique<CorruptOI>(),
  std::make_unique<CorruptOndiskSize>(),
  std::make_unique<MissingSS>(),
  std::make_unique<CorruptSS>(),
  std::make_unique<DataDigestMismatch>(),
  std::make_unique<OmapDigestMismatch>(),
  std::make_unique<ExtraAttribute>(),
  std::make_unique<MissingAttribute>()
};
const SingleErrorTestCase *to_ptr(
  const std::unique_ptr<SingleErrorTestCase> &tc) {
  return tc.get();
}
// iterator over the above set as pointers
using test_case_ptr_iter_t = boost::transform_iterator<
  std::function<decltype(to_ptr)>, decltype(std::begin(test_cases))>;
template <SingleErrorTestCase::restriction_t restriction>
struct test_case_filter_t {
  bool operator()(const SingleErrorTestCase *tc) const {
    return tc->valid_for_restriction(restriction);
  }
};
template <SingleErrorTestCase::restriction_t restriction>
// iterator over the above set filtered by restriction
using test_case_filter_iter_t = boost::filter_iterator<
  test_case_filter_t<restriction>,
  test_case_ptr_iter_t>;
template <SingleErrorTestCase::restriction_t restriction>
// begin and end, used below to instantiate test suites
auto test_cases_begin() {
  return test_case_filter_iter_t<restriction>(
    test_case_filter_t<restriction>(),
    test_case_ptr_iter_t(std::begin(test_cases), to_ptr),
    test_case_ptr_iter_t(std::end(test_cases), to_ptr));
}
template <SingleErrorTestCase::restriction_t restriction>
auto test_cases_end() {
  return test_case_filter_iter_t<restriction>(
    test_case_filter_t<restriction>(),
    test_case_ptr_iter_t(std::end(test_cases), to_ptr),
    test_case_ptr_iter_t(std::end(test_cases), to_ptr));
}

/// tuple defining each generated test case
using single_error_test_param_t = std::tuple<
  test_obj_t,                /// initial test object
  bool,                      /// inject on primary?
  const SingleErrorTestCase* /// test case
  >;
template <>
struct fmt::formatter<single_error_test_param_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const auto &param, FormatContext& ctx) const
  {
    const auto &[obj, is_primary, test_case] = param;
    return fmt::format_to(
      ctx.out(), "{}{}{}",
      obj.desc,
      is_primary ? "Primary" : "Replica",
      test_case->get_description());
  }
};
std::ostream &operator<<(std::ostream &out, const single_error_test_param_t &p)
{
  return out << fmt::format("{}", p);
}

class TestSingleError :
  public testing::TestWithParam<single_error_test_param_t> {
};

/**
 * compare_error_signatures
 *
 * Generic helper for comparing err_t, obj_err_t, and
 * inconsistent_snapset_t with descriptive output.
 */
auto compare_error_signatures(const auto &lh, const auto &rh)
{
  if (lh.errors == rh.errors) {
    return ::testing::AssertionSuccess() << fmt::format(
      "Signature match: {}", lh);
  } else {
    return ::testing::AssertionFailure() << fmt::format(
      "Signature mismatch: {} should be {}",
      lh, rh);
  }
}

TEST_P(TestSingleError, SingleError) {
  const auto &[_obj, is_primary, test_case] = GetParam();
  auto obj = test_case->adjust_base_object(_obj);

  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  const pg_shard_t replica(1, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "scrub"
  };
  const pg_shard_t &target = is_primary ? primary : replica;
  const std::vector<pg_shard_t> shards = {
    primary, replica
  };

  auto with_error = test_case->inject_error(obj);
  crimson::osd::scrub::scrub_map_set_t maps;
  for (const auto &osd : shards) {
    if (osd == target) {
      maps[osd].objects[obj.hoid] = with_error.get();
    } else {
      maps[osd].objects[obj.hoid] = obj.get();
    }
  }

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(
    dpp, policy, maps);
  const auto &object_errors = ret.object_errors;

  ASSERT_EQ(object_errors.size(), 1) << fmt::format(
    "{}: generated an incorrect number of errors: {}\n",
    *test_case, object_errors);

  auto &obj_error = object_errors.front();

  // Error counting: matches classic OSD scrub_backend.cc behaviour.
  // (a) Per-shard: +1 for each shard whose shard_info has shallow/deep errors.
  // (b) Per-object: +1 if the object-level flags carry shallow/deep errors.
  // These are additive — the test formula must mirror both categories.
  {
    int expected_shallow = 0, expected_deep = 0;
    for (const auto &[sid, si] : obj_error.shards) {
      if (si.has_shallow_errors()) ++expected_shallow;
      else if (si.has_deep_errors()) ++expected_deep;
    }
    if (obj_error.has_shallow_errors()) ++expected_shallow;
    else if (obj_error.has_deep_errors()) ++expected_deep;
    expected_shallow += static_cast<int>(ret.snapset_errors.size());
    EXPECT_EQ(ret.stats.num_shallow_scrub_errors, expected_shallow);
    EXPECT_EQ(ret.stats.num_deep_scrub_errors, expected_deep);
  }

  EXPECT_TRUE(compare_error_signatures(
    static_cast<const librados::obj_err_t&>(obj_error),
    test_case->get_object_error_sig()));

  EXPECT_EQ(obj_error.shards.size(), shards.size());
  bool found_selected_oi = false;
  for (const auto &shard : shards) {
    auto siter = obj_error.shards.find(
      librados::osd_shard_t{shard.osd, static_cast<int8_t>(shard.shard)}
    );
    if (siter == obj_error.shards.end()) {
      EXPECT_NE(siter, obj_error.shards.end());
      continue;
    }
    if (shard == target) {
      EXPECT_TRUE(compare_error_signatures(
 static_cast<const librados::err_t&>(siter->second),
 test_case->get_shard_error_sig(is_primary)));
      // For digest-mismatch cases injected on primary, the primary shard IS
      // both the target and the auth shard, so selected_oi lives here.
      if (test_case->selected_oi_may_be_on_target() && siter->second.selected_oi) {
        found_selected_oi = true;
      }
    } else {
      EXPECT_FALSE(siter->second.has_errors());
      if (siter->second.selected_oi) found_selected_oi = true;
    }
    if (shard == primary) {
      EXPECT_TRUE(siter->second.primary);
    }
  }
  EXPECT_TRUE(found_selected_oi);
  // missing_digest is allowed to be non-empty only for digest-mismatch cases
  // when the error is injected on the primary (auth) shard.
  if (!(test_case->missing_digest_may_be_non_empty() && is_primary)) {
    EXPECT_TRUE(ret.missing_digest.empty());
  }
}

/* Tests that don't have restrictions */
INSTANTIATE_TEST_SUITE_P(
  SingleErrorGeneral,
  TestSingleError,
  ::testing::Combine(
    ::testing::Values(
      test_obj_t::make_head("Small", "foo").set_size(64),
      test_obj_t::make_clone("EmptyWithAttr", "foo2").add_attr("extra_attr", 64),
      test_obj_t::make_head("ReplicatedRBD", "foo2").set_size(4<<20)
    ),
    ::testing::Bool(),
    ::testing::ValuesIn(
      test_cases_begin<SingleErrorTestCase::restriction_t::NONE>(),
      test_cases_end<SingleErrorTestCase::restriction_t::NONE>())
  ),
  [](const auto &info) {
    return fmt::format("{}", info.param);
  }
);

/* Some tests don't trigger shard errors, so we can't actually tell which
 * replica is wrong.  Such tests are written for the error to be injected
 * on the replica. */
INSTANTIATE_TEST_SUITE_P(
  SingleErrorPrimaryOnly,
  TestSingleError,
  ::testing::Combine(
    ::testing::Values(
      test_obj_t::make_head("Small", "foo").set_size(64),
      test_obj_t::make_clone("EmptyWithAttr", "foo2").add_attr("extra_attr", 64),
      test_obj_t::make_head("ReplicatedRBD", "foo2").set_size(4<<20)
    ),
    ::testing::Values(false), // replica only
    ::testing::ValuesIn(
      test_cases_begin<SingleErrorTestCase::restriction_t::REPLICA_ONLY>(),
      test_cases_end<SingleErrorTestCase::restriction_t::REPLICA_ONLY>())
  ),
  [](const auto &info) {
    return fmt::format("{}", info.param);
  }
);

/* Tests only valid on head objects.  Replica-only because when the primary
 * has a snapset attr error (SNAPSET_MISSING/CORRUPTED), the implementation
 * routes it through snapset_errors (not object_errors) — that path is
 * covered separately by TestSnapSetPrimaryAttrError below. */
INSTANTIATE_TEST_SUITE_P(
  SingleErrorHEAD,
  TestSingleError,
  ::testing::Combine(
    ::testing::Values(
      test_obj_t::make_head("Small", "foo").set_size(64),
      test_obj_t::make_head("ReplicatedRBD", "foo2").set_size(4<<20)
    ),
    ::testing::Values(false), // replica only
    ::testing::ValuesIn(
      test_cases_begin<SingleErrorTestCase::restriction_t::HEAD_ONLY>(),
      test_cases_end<SingleErrorTestCase::restriction_t::HEAD_ONLY>())
  ),
  [](const auto &info) {
    return fmt::format("{}", info.param);
  }
);


using test_clone_spec_t = std::pair<
  snapid_t, // clone id
  size_t    // clone size
  >;

/// descending order of clone id
using test_clone_list_t = std::vector<test_clone_spec_t>;

/**
 * snapset_test_case_t
 *
 * This descriptor can express 3 types of error
 * - missing clone
 * - extra clone
 * - clone size mismatch
 * in 4 positions using one bit for each pair.
 */
class snapset_test_case_t {
  uint32_t signature;

  snapset_test_case_t(uint32_t signature) : signature(signature) {}

  constexpr static uint32_t POSITION_BITS = 4;
  constexpr static uint32_t position_mask[] = {
    0x1, 0x2, 0x4, 0x8
  };
  constexpr static unsigned MAX_POS = std::size(position_mask);

  constexpr static uint32_t MIN_VALID = 0;
  constexpr static uint32_t MAX_VALID = 0xFFF;
  enum type_t {
    MISSING = 0,
    EXTRA,
    SIZE
  };

  bool should_inject(type_t type, unsigned position) const {
    ceph_assert(position < MAX_POS);
    return (signature >> (type * POSITION_BITS)) & position_mask[position];
  }
  static snapset_test_case_t make(type_t type, unsigned position) {
    ceph_assert(position < std::size(position_mask));
    return snapset_test_case_t{
      position_mask[position] << (type * POSITION_BITS)
    };
  }
  static auto generate_single_errors(type_t type) {
    std::vector<snapset_test_case_t> ret;
    ret.reserve(std::size(position_mask));
    for (unsigned i = 0; i < MAX_POS; ++i) {
      ret.push_back(make(type, i));
    }
    return ret;
  }

public:
  constexpr static unsigned get_max_pos() { return MAX_POS; }

  bool should_inject_missing(unsigned position) const {
    return should_inject(MISSING, position);
  }
  bool should_inject_extra(unsigned position) const {
    return should_inject(EXTRA, position);
  }
  bool should_inject_size(unsigned position) const {
    return should_inject(SIZE, position);
  }

  static auto generate_single_missing_errors() {
    return generate_single_errors(MISSING);
  }
  static auto generate_single_extra_errors() {
    return generate_single_errors(EXTRA);
  }
  static auto generate_single_size_errors() {
    return generate_single_errors(SIZE);
  }
  static auto generate_random_errors(size_t num, int seed = 0) {
    std::default_random_engine e1(seed);
    std::uniform_int_distribution<uint32_t> uniform_dist(1, MAX_VALID);

    std::vector<snapset_test_case_t> ret;
    ret.reserve(num);
    for (unsigned i = 0; i < num; ++i) {
      ret.push_back(snapset_test_case_t{uniform_dist(e1)});
    }
    return ret;
  }
  friend std::ostream &operator<<(std::ostream &out, snapset_test_case_t rhs);
};
std::ostream &operator<<(std::ostream &out, snapset_test_case_t rhs) {
  for (auto &[s, type] :
	 std::vector<std::pair<std::string, snapset_test_case_t::type_t>>(
	   {{"M", snapset_test_case_t::MISSING},
	    {"E", snapset_test_case_t::EXTRA},
	    {"S", snapset_test_case_t::SIZE}})) {
    out << s;
    for (unsigned i = 0;
	 i < snapset_test_case_t::MAX_POS; ++i) {
      if (rhs.should_inject(type, i)) {
	out << i;
      }
    }
  }
  return out;
}

class TestSnapSetCloneError :
  public testing::TestWithParam<snapset_test_case_t> {
};


SnapSet make_snapset(const test_clone_list_t &clone_list)
{
  SnapSet ss;
  for (const auto &[cloneid, size] : clone_list) {
    ss.clones.push_back(cloneid);
    ss.clone_size[cloneid] = size;
    ss.clone_overlap[cloneid];
    ss.clone_snaps[cloneid].push_back(cloneid);
    // seq must be >= largest clone id to avoid "snaps.seq not set" error
    if (cloneid > ss.seq) ss.seq = cloneid;
  }
  return ss;
}

std::pair<hobject_t, ScrubMap::object> make_clone(
  std::string name, std::pair<snapid_t, size_t> in)
{
  ScrubMap ret;
  auto [cloneid, size] = in;
  hobject_t hoid = so_builder_t::make_hoid(name, in.first);
  auto so = so_builder_t::make_clone(
    name, cloneid);
  so.set_size(size);
  return std::make_pair(hoid, so.get());
}

TEST_P(TestSnapSetCloneError, CloneError) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "scrub"
  };

  crimson::osd::scrub::scrub_map_set_t maps;
  const std::string name = "test_obj";
  auto &map = maps[primary];
  // head_expected_error: tracks only errors that appear in the HEAD snapset entry
  // (CLONE_MISSING and EXTRA_CLONES).
  // SIZE_MISMATCH goes on the per-clone entries, not on the head.
  inconsistent_snapset_wrapper head_expected_error;
  // clone_snap_ids with injected size errors, for verification below.
  std::set<snapid_t> size_error_clones;

  test_clone_list_t should_exist = {
    { 10, 32 }, { 25,  64 }, { 50,  32 }, { 100,  64 }
  };
  test_clone_list_t extra = {
    { 9, 64 }, { 11, 32 }, { 99, 64 }, { 101, 32 }
  };

  for (unsigned i = 0; i < snapset_test_case_t::get_max_pos(); ++i) {
    hobject_t hoid = so_builder_t::make_hoid(name, should_exist[i].first);
    if (!GetParam().should_inject_missing(i)) {
      auto to_insert = make_clone(name, should_exist[i]);
      if (GetParam().should_inject_size(i)) {
        // SIZE_MISMATCH lands on the clone entry, not the head entry.
        // Bump the physical size without touching OI size.
        size_error_clones.insert(should_exist[i].first);
        auto oi = so_get_oi(to_insert.second);
        ceph_assert(oi);
        to_insert.second.size = oi->size + 1;
      }
      map.objects.insert(to_insert);
    } else {
      head_expected_error.set_clone_missing(should_exist[i].first);
    }
    if (GetParam().should_inject_extra(i)) {
      map.objects.insert(make_clone(name, extra[i]));
      head_expected_error.set_clone(extra[i].first);
    }
  }

  hobject_t hoid = so_builder_t::make_hoid(name);
  map.objects[hoid] = so_builder_t::make_head(name).get();

  so_set_ss(map.objects[hoid], make_snapset(should_exist));

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(
    dpp, policy, maps);
  EXPECT_EQ(ret.object_errors.size(), 0);
  // The implementation emits one entry per extra/size-mismatched clone plus
  // the head entry.  Find the head entry (snap==CEPH_NOSNAP) and compare its
  // error signature against head_expected_error (only head-level flags).
  ASSERT_FALSE(ret.snapset_errors.empty()) << fmt::format(
    "Got no snapset_errors; expected at least 1. errors: {}",
    ret.snapset_errors);
  const inconsistent_snapset_wrapper *head_entry = nullptr;
  for (const auto &se : ret.snapset_errors) {
    if (se.object.snap == CEPH_NOSNAP) {
      head_entry = &se;
      break;
    }
  }
  ASSERT_NE(head_entry, nullptr) << fmt::format(
    "No head entry (snap==CEPH_NOSNAP) in snapset_errors: {}",
    ret.snapset_errors);
  EXPECT_TRUE(compare_error_signatures(*head_entry, head_expected_error));

  // Verify that SIZE_MISMATCH appears on the expected clone entries.
  for (auto snap : size_error_clones) {
    inconsistent_snapset_wrapper expected_clone_err;
    expected_clone_err.set_size_mismatch();
    bool found = false;
    for (const auto &se : ret.snapset_errors) {
      if (se.object.snap == snap) {
        EXPECT_TRUE(compare_error_signatures(se, expected_clone_err))
          << fmt::format("Clone snap={} has wrong error sig", snap);
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found) << fmt::format(
      "No clone entry for snap={} in snapset_errors: {}",
      snap, ret.snapset_errors);
  }

}

INSTANTIATE_TEST_SUITE_P(
  SingleMissing,
  TestSnapSetCloneError,
  ::testing::ValuesIn(snapset_test_case_t::generate_single_missing_errors())
);

INSTANTIATE_TEST_SUITE_P(
  SingleExtra,
  TestSnapSetCloneError,
  ::testing::ValuesIn(snapset_test_case_t::generate_single_extra_errors())
);

INSTANTIATE_TEST_SUITE_P(
  SingleSize,
  TestSnapSetCloneError,
  ::testing::ValuesIn(snapset_test_case_t::generate_single_size_errors())
);

INSTANTIATE_TEST_SUITE_P(
  MultipleRandom,
  TestSnapSetCloneError,
  ::testing::ValuesIn(snapset_test_case_t::generate_random_errors(100))
);

TEST(TestSnapSet, MissingHead) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "scrub"
  };

  crimson::osd::scrub::scrub_map_set_t maps;
  inconsistent_snapset_wrapper expected_error;

  test_clone_list_t clones = {
    { 10, 64 }, { 25, 32 }, { 50, 64 }, { 100, 32 }
  };
  for (const auto &desc : test_clone_list_t{clones}) {
    maps[primary].objects.emplace(make_clone("test_object", desc));
  }
  expected_error.set_headless();


  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(
    dpp, policy, maps);
  EXPECT_EQ(ret.object_errors.size(), 0);
  // The implementation emits one HEADLESS_CLONE entry per orphan clone.
  // There must be exactly one entry per clone (4 clones → 4 entries), all
  // carrying the HEADLESS_CLONE signature.
  ASSERT_EQ(ret.snapset_errors.size(), clones.size()) << fmt::format(
    "Got snapset_errors: {}", ret.snapset_errors);
  for (const auto &se : ret.snapset_errors) {
    EXPECT_TRUE(compare_error_signatures(se, expected_error)) << fmt::format(
      "Unexpected entry: {}", se);
  }

}

TEST(TestSnapSet, Stats) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "scrub"
  };


  object_stat_sum_t expected_stats;
  crimson::osd::scrub::scrub_map_set_t maps;
  auto &objs = maps[primary].objects;

  unsigned num = 0;
  auto add_simple_head = [&](size_t size, auto &&f)
    -> ScrubMap::object & {
    auto name = fmt::format("obj-{}", ++num);
    auto hoid = so_builder_t::make_hoid(name);
    auto obj = so_builder_t::make_head(name).set_size(size).get();
    so_mut_oi(obj, std::forward<decltype(f)>(f));
    expected_stats.num_bytes += size;
    expected_stats.num_objects++;
    return objs[hoid] = obj;
  };

  add_simple_head(64, [&expected_stats](auto maybe_oi) {
    ceph_assert(maybe_oi);
    maybe_oi->set_flag(object_info_t::FLAG_DIRTY);
    expected_stats.num_objects_dirty++;
    return maybe_oi;
  });

  add_simple_head(128, [&expected_stats](auto maybe_oi) {
    ceph_assert(maybe_oi);
    maybe_oi->set_flag(object_info_t::FLAG_MANIFEST);
    expected_stats.num_objects_manifest++;
    return maybe_oi;
  });

  add_simple_head(0, [&expected_stats](auto maybe_oi) {
    ceph_assert(maybe_oi);
    maybe_oi->set_flag(object_info_t::FLAG_WHITEOUT);
    expected_stats.num_whiteouts++;
    return maybe_oi;
  });

  {
    auto &so = add_simple_head(32, [](auto maybe_oi) {
      ceph_assert(maybe_oi);
      maybe_oi->set_flag(object_info_t::FLAG_OMAP);
      return maybe_oi;
    });
    expected_stats.num_omap_keys += (so.object_omap_keys = 10);
    expected_stats.num_omap_bytes += (so.object_omap_bytes = 100);
    expected_stats.num_objects_omap++;
  }

  {
    auto &so = add_simple_head(64, [](auto maybe_oi) {
      ceph_assert(maybe_oi);
      maybe_oi->set_flag(object_info_t::FLAG_OMAP);
      return maybe_oi;
    });
    expected_stats.num_omap_keys +=
      (so.object_omap_keys = (TEST_OMAP_KEY_LIMIT + 1));
    expected_stats.num_omap_bytes +=
      (so.object_omap_bytes = so.object_omap_keys);
    expected_stats.num_objects_omap++;
    expected_stats.num_large_omap_objects++;
  }

  {
    auto &so = add_simple_head(64, [](auto maybe_oi) {
      ceph_assert(maybe_oi);
      maybe_oi->set_flag(object_info_t::FLAG_OMAP);
      return maybe_oi;
    });
    expected_stats.num_omap_keys += (so.object_omap_keys = 1);
    expected_stats.num_omap_bytes +=
      (so.object_omap_bytes = (TEST_OMAP_BYTES_LIMIT + 1));
    expected_stats.num_objects_omap++;
    expected_stats.num_large_omap_objects++;
  }

  {
    auto name = fmt::format("obj-{}", ++num);

    std::map<snapid_t, interval_set<uint64_t>> clone_overlap;
    test_clone_list_t clones;
    auto add_clone = [&](std::pair<snapid_t, size_t> clone_desc,
			 interval_set<uint64_t> overlap) -> ScrubMap::object & {
      auto hoid = so_builder_t::make_hoid(name, clone_desc.first);
      clones.push_back(clone_desc);
      auto [_, obj] = make_clone(name, clone_desc);
      expected_stats.num_object_clones++;
      expected_stats.num_objects++;

      expected_stats.num_bytes += clone_desc.second - overlap.size();
      clone_overlap[clone_desc.first] = std::move(overlap);

      return objs[hoid] = obj;
    };

    auto make_is = [](uint64_t off, uint64_t len) {
      interval_set<uint64_t> ret;
      ret.insert(off, len);
      return ret;
    };

    add_clone({99, 32}, {});
    add_clone({100, 64}, make_is(31, 33));

    {
      auto hoid = so_builder_t::make_hoid(name);
      size_t size = 64;
      auto obj = so_builder_t::make_head(name).set_size(size).get();
      expected_stats.num_bytes += size;
      expected_stats.num_objects++;

      SnapSet ss = make_snapset(clones);
      ss.clone_overlap = std::move(clone_overlap);
      so_mut_ss(obj, [ss=std::move(ss)](auto) mutable {
	return std::move(ss);
      });

      objs[hoid] = obj;
    }
  }

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(
    dpp, policy, maps);
  EXPECT_EQ(ret.object_errors.size(), 0);
  ASSERT_EQ(ret.snapset_errors.size(), 0) << fmt::format(
    "Got snapset_errors: {}", ret.snapset_errors);
  // No classic-format log messages expected for a clean scrub
  EXPECT_TRUE(ret.snapset_log_messages.empty());
  // No digest write-back expected (objects have no pre-set computed digests)
  EXPECT_TRUE(ret.missing_digest.empty());

  EXPECT_EQ(ret.stats, expected_stats);
}

// ============================================================================
// Tests for missing_digest
// ============================================================================

/**
 * TEST(MissingDigest, DataDigestNotInOI)
 *
 * Deep scrub: the auth shard has digest_present=true with value 42, but the
 * stored OI has no data digest at all.  validate_chunk() must produce a
 * missing_digest entry asking for data_digest=42 to be written back.
 */
TEST(MissingDigest, DataDigestNotInOI) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "deep-scrub"
  };

  // Build a head object whose OI has no data digest yet.
  auto builder = so_builder_t::make_head("foo");
  builder.set_size(64);
  // Auth shard: deep-scan produced digest=42, but OI does not record it.
  builder.digest_present = true;
  builder.digest = 42;
  // OI must NOT have data digest recorded.
  builder.mut_oi([](auto maybe_oi) {
    ceph_assert(maybe_oi);
    maybe_oi->clear_data_digest();
    return maybe_oi;
  });

  hobject_t hoid = builder.get_attr_encoded<object_info_t>(OI_ATTR)->soid;
  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[hoid] = builder.get();

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  EXPECT_EQ(ret.object_errors.size(), 0);
  EXPECT_EQ(ret.snapset_errors.size(), 0);
  ASSERT_EQ(ret.missing_digest.size(), 1) << fmt::format(
    "Expected 1 missing_digest entry, got {}", ret.missing_digest.size());

  const auto &du = ret.missing_digest.front();
  EXPECT_EQ(du.oid, hoid);
  ASSERT_TRUE(du.data_digest.has_value());
  EXPECT_EQ(*du.data_digest, 42u);
  EXPECT_FALSE(du.omap_digest.has_value());
}

/**
 * TEST(MissingDigest, OmapDigestNotInOI)
 *
 * Deep scrub: the auth shard has omap_digest_present=true with value 99, but
 * the OI has no omap digest.  A missing_digest entry for omap_digest=99 must
 * be produced.
 */
TEST(MissingDigest, OmapDigestNotInOI) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "deep-scrub"
  };

  auto builder = so_builder_t::make_head("bar");
  builder.set_size(32);
  builder.omap_digest_present = true;
  builder.omap_digest = 99;
  builder.mut_oi([](auto maybe_oi) {
    ceph_assert(maybe_oi);
    maybe_oi->clear_omap_digest();
    return maybe_oi;
  });

  hobject_t hoid = builder.get_attr_encoded<object_info_t>(OI_ATTR)->soid;
  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[hoid] = builder.get();

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  EXPECT_EQ(ret.object_errors.size(), 0);
  EXPECT_EQ(ret.snapset_errors.size(), 0);
  ASSERT_EQ(ret.missing_digest.size(), 1);

  const auto &du = ret.missing_digest.front();
  EXPECT_EQ(du.oid, hoid);
  EXPECT_FALSE(du.data_digest.has_value());
  ASSERT_TRUE(du.omap_digest.has_value());
  EXPECT_EQ(*du.omap_digest, 99u);
}

/**
 * TEST(MissingDigest, BothDigestsMissing)
 *
 * Deep scrub: auth shard has both data and omap digests computed but neither
 * is stored in OI.  A single missing_digest entry must carry both updates.
 */
TEST(MissingDigest, BothDigestsMissing) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "deep-scrub"
  };

  auto builder = so_builder_t::make_head("baz");
  builder.set_size(128);
  builder.digest_present = true;
  builder.digest = 7;
  builder.omap_digest_present = true;
  builder.omap_digest = 13;
  builder.mut_oi([](auto maybe_oi) {
    ceph_assert(maybe_oi);
    maybe_oi->clear_data_digest();
    maybe_oi->clear_omap_digest();
    return maybe_oi;
  });

  hobject_t hoid = builder.get_attr_encoded<object_info_t>(OI_ATTR)->soid;
  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[hoid] = builder.get();

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  EXPECT_EQ(ret.object_errors.size(), 0);
  ASSERT_EQ(ret.missing_digest.size(), 1);

  const auto &du = ret.missing_digest.front();
  EXPECT_EQ(du.oid, hoid);
  ASSERT_TRUE(du.data_digest.has_value());
  EXPECT_EQ(*du.data_digest, 7u);
  ASSERT_TRUE(du.omap_digest.has_value());
  EXPECT_EQ(*du.omap_digest, 13u);
}

/**
 * TEST(MissingDigest, DigestMatchesOI)
 *
 * When the computed digest already matches what is stored in OI, no
 * missing_digest entry should be produced.
 */
TEST(MissingDigest, DigestMatchesOI) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "deep-scrub"
  };

  auto builder = so_builder_t::make_head("qux");
  builder.set_size(64);
  // OI records the same digest as the computed value.
  builder.mut_oi([](auto maybe_oi) {
    ceph_assert(maybe_oi);
    maybe_oi->set_data_digest(5);
    return maybe_oi;
  });
  builder.digest_present = true;
  builder.digest = 5;  // matches OI

  hobject_t hoid = builder.get_attr_encoded<object_info_t>(OI_ATTR)->soid;
  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[hoid] = builder.get();

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  EXPECT_EQ(ret.object_errors.size(), 0);
  EXPECT_TRUE(ret.missing_digest.empty()) << "No write-back needed when digest matches OI";
}

/**
 * TEST(MissingDigest, NoDigestComputed)
 *
 * Shallow scrub: no digest was computed (digest_present=false).  No
 * missing_digest entry should be produced regardless of what OI records.
 */
TEST(MissingDigest, NoDigestComputed) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "scrub"
  };

  auto builder = so_builder_t::make_head("quux");
  builder.set_size(64);
  // OI has a stored data digest, but the scrub map has digest_present=false
  // (shallow scrub did not compute the digest).
  builder.mut_oi([](auto maybe_oi) {
    ceph_assert(maybe_oi);
    maybe_oi->set_data_digest(3);
    return maybe_oi;
  });
  builder.digest_present = false;  // shallow scrub: no digest computed (already default)

  hobject_t hoid = builder.get_attr_encoded<object_info_t>(OI_ATTR)->soid;
  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[hoid] = builder.get();

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  EXPECT_EQ(ret.object_errors.size(), 0);
  EXPECT_TRUE(ret.missing_digest.empty()) << "No write-back when digest was not computed";
}

// ============================================================================
// Tests for snapset_log_messages
// ============================================================================

/**
 * TEST(SnapsetLogMessages, MissingClone)
 *
 * A head with a valid SnapSet listing one clone, but the clone object is
 * absent from the scrub map.  validate_chunk() must populate
 * snapset_log_messages with the classic "expected clone … missing" ERR line
 * and the "N missing clone(s)" INF summary, from the primary shard only.
 */
TEST(SnapsetLogMessages, MissingClone) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  const spg_t pgid{pg_t{0, 1}, shard_id_t::NO_SHARD};
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    pgid,
    "scrub"
  };

  const std::string name = "logtest";
  crimson::osd::scrub::scrub_map_set_t maps;
  auto &map = maps[primary];

  // Head with a snapset declaring snap 5 as a clone.
  hobject_t head_hoid = so_builder_t::make_hoid(name);
  auto head_so = so_builder_t::make_head(name).set_size(32).get();
  SnapSet ss;
  ss.seq = 5;
  ss.clones.push_back(5);
  ss.clone_size[5] = 32;
  ss.clone_overlap[5];
  ss.clone_snaps[5].push_back(5);
  so_set_ss(head_so, ss);
  map.objects[head_hoid] = head_so;
  // The clone object (snap=5) is intentionally omitted from the scrub map.

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  // snapset_errors must contain the head entry (CLONE_MISSING).
  ASSERT_GE(ret.snapset_errors.size(), 1u);
  EXPECT_TRUE(ret.snapset_errors.front().clone_missing());

  // snapset_log_messages must be non-empty.
  ASSERT_FALSE(ret.snapset_log_messages.empty()) <<
    "Expected log messages for missing clone";

  // Find the ERR line about the missing clone.
  bool found_err = false, found_inf = false;
  hobject_t clone_hoid = head_hoid;
  clone_hoid.snap = 5;
  const std::string err_fragment = fmt::format(
    "scrub {} {} : expected clone", pgid, head_hoid);
  const std::string inf_fragment = fmt::format(
    "scrub {} {} : 1 missing clone(s)", pgid, head_hoid);
  for (const auto &[level, msg] : ret.snapset_log_messages) {
    if (level == 'E' && msg.find(err_fragment) != std::string::npos) {
      found_err = true;
    }
    if (level == 'I' && msg == inf_fragment) {
      found_inf = true;
    }
  }
  if (!found_err) {
    std::string all_msgs;
    for (const auto &[level, msg] : ret.snapset_log_messages) {
      all_msgs += fmt::format("[{}] {}; ", level, msg);
    }
    FAIL() << fmt::format(
      "Did not find ERR message containing '{}' in: {}", err_fragment, all_msgs);
  }
  EXPECT_TRUE(found_inf) << fmt::format(
    "Did not find INF message '{}' in log messages", inf_fragment);
}

/**
 * TEST(SnapsetLogMessages, SeqNotSet)
 *
 * A head with a SnapSet that has seq==0 but lists a clone triggers the
 * classic "snaps.seq not set" error message.
 */
TEST(SnapsetLogMessages, SeqNotSet) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  const spg_t pgid{pg_t{0, 2}, shard_id_t::NO_SHARD};
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    pgid,
    "scrub"
  };

  const std::string name = "seqtest";
  crimson::osd::scrub::scrub_map_set_t maps;
  auto &map = maps[primary];

  hobject_t head_hoid = so_builder_t::make_hoid(name);
  auto head_so = so_builder_t::make_head(name).set_size(0).get();

  // Clone present in scrub map.
  hobject_t clone_hoid = so_builder_t::make_hoid(name, 3);
  auto clone_so = so_builder_t::make_clone(name, 3).set_size(32).get();
  map.objects[clone_hoid] = clone_so;

  // SnapSet: seq==0 (not set) but clones list is non-empty → snapset_error.
  SnapSet ss;
  ss.seq = 0;
  ss.clones.push_back(3);
  ss.clone_size[3] = 32;
  ss.clone_overlap[3];
  ss.clone_snaps[3].push_back(3);
  so_set_ss(head_so, ss);
  map.objects[head_hoid] = head_so;

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  // The head snapset_error must be reported.
  ASSERT_GE(ret.snapset_errors.size(), 1u);
  EXPECT_TRUE(ret.snapset_errors.front().snapset_error());

  // The log messages must contain the classic "snaps.seq not set" error.
  const std::string expected_msg = fmt::format(
    "scrub {} {} : snaps.seq not set", pgid, head_hoid);
  bool found = false;
  for (const auto &[level, msg] : ret.snapset_log_messages) {
    if (level == 'E' && msg == expected_msg) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found) << fmt::format(
    "Did not find '{}' in snapset_log_messages", expected_msg);
}

/**
 * TEST(SnapsetLogMessages, MissingSnapsetNoLogs)
 *
 * When the primary shard has a missing SnapSet (no SS_ATTR), the implementation
 * emits "clone ignored due to missing snapset" messages for each clone, but
 * only from the primary shard path.  Verify that messages appear and are 'E'.
 */
TEST(SnapsetLogMessages, MissingSnapset) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  const spg_t pgid{pg_t{0, 3}, shard_id_t::NO_SHARD};
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    pgid,
    "scrub"
  };

  const std::string name = "missss";
  crimson::osd::scrub::scrub_map_set_t maps;
  auto &map = maps[primary];

  // Head with SS_ATTR removed (simulates SNAPSET_MISSING).
  hobject_t head_hoid = so_builder_t::make_hoid(name);
  auto head_so = so_builder_t::make_head(name).set_size(0).get();
  so_set_ss(head_so, std::nullopt);  // remove SS_ATTR
  map.objects[head_hoid] = head_so;

  // One clone present.
  hobject_t clone_hoid = so_builder_t::make_hoid(name, 7);
  auto clone_so = so_builder_t::make_clone(name, 7).set_size(16).get();
  map.objects[clone_hoid] = clone_so;

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  // snapset_errors contains at least the head entry (SNAPSET_MISSING).
  // The head entry has snap==CEPH_NOSNAP; the clone entry (snap=7) sorts first.
  bool found_head_missing = false;
  for (const auto &se : ret.snapset_errors) {
    if (se.object.snap == CEPH_NOSNAP && se.snapset_missing()) {
      found_head_missing = true;
      break;
    }
  }
  ASSERT_GE(ret.snapset_errors.size(), 1u);
  EXPECT_TRUE(found_head_missing) <<
    "Expected a head entry with SNAPSET_MISSING in snapset_errors";

  // snapset_log_messages must contain the "clone ignored" ERR lines.
  bool found_clone_ignored = false;
  const std::string fragment = "clone ignored due to missing snapset";
  for (const auto &[level, msg] : ret.snapset_log_messages) {
    if (level == 'E' && msg.find(fragment) != std::string::npos) {
      found_clone_ignored = true;
      break;
    }
  }
  EXPECT_TRUE(found_clone_ignored) << fmt::format(
    "Expected '{}' in log messages", fragment);
}

/**
 * TEST(SnapsetLogMessages, ReplicaShardNotLogged)
 *
 * Log messages must only come from the primary shard's SnapSet evaluation.
 * When the replica has a missing SnapSet but the primary's is clean, no log
 * messages should be emitted (the replica's SnapSet error surfaces only in
 * object_errors / replica_snapset_errors, not in snapset_log_messages).
 */
TEST(SnapsetLogMessages, ReplicaShardNotLogged) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  const pg_shard_t replica(1, shard_id_t::NO_SHARD);
  const spg_t pgid{pg_t{0, 4}, shard_id_t::NO_SHARD};
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    pgid,
    "scrub"
  };

  const std::string name = "replog";
  hobject_t head_hoid = so_builder_t::make_hoid(name);

  // Primary: valid head with a proper SnapSet (no clones).
  auto primary_head = so_builder_t::make_head(name).set_size(0).get();
  // Replica: head missing SS_ATTR.
  auto replica_head = primary_head;
  replica_head.attrs.erase(SS_ATTR);

  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[head_hoid] = primary_head;
  maps[replica].objects[head_hoid] = replica_head;

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  // No snapset_log_messages: the primary's SnapSet is clean.
  EXPECT_TRUE(ret.snapset_log_messages.empty()) <<
    "Replica-shard SnapSet errors must not produce snapset_log_messages";
}

// ============================================================================
// Tests for replica_snapset_errors
// ============================================================================

/**
 * TEST(ReplicaSnapsetErrors, ReplicaMissingSnapset)
 *
 * Primary has a valid SnapSet; replica is missing SS_ATTR.
 * The replica's error must appear in replica_snapset_errors (not snapset_errors),
 * with SNAPSET_MISSING set.
 */
TEST(ReplicaSnapsetErrors, ReplicaMissingSnapset) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  const pg_shard_t replica(1, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "scrub"
  };

  const std::string name = "rsse_obj";
  hobject_t head_hoid = so_builder_t::make_hoid(name);

  // Primary: valid head with a proper (empty) SnapSet.
  auto primary_head = so_builder_t::make_head(name).set_size(0).get();

  // Replica: same head but SS_ATTR removed.
  auto replica_head = primary_head;
  replica_head.attrs.erase(SS_ATTR);

  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[head_hoid] = primary_head;
  maps[replica].objects[head_hoid] = replica_head;

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  // Primary's SnapSet is clean: no primary-path snapset errors.
  EXPECT_EQ(ret.snapset_errors.size(), 0u) <<
    "Primary SnapSet is valid; no primary snapset errors expected";

  // Replica's missing SS_ATTR must surface in replica_snapset_errors.
  ASSERT_EQ(ret.replica_snapset_errors.size(), 1u) << fmt::format(
    "Expected 1 replica_snapset_errors entry, got {}",
    ret.replica_snapset_errors.size());
  EXPECT_TRUE(ret.replica_snapset_errors.front().snapset_missing()) <<
    "Expected SNAPSET_MISSING on the replica entry";

  // No log messages: replica errors are not logged via snapset_log_messages.
  EXPECT_TRUE(ret.snapset_log_messages.empty());
}

/**
 * TEST(ReplicaSnapsetErrors, ReplicaCorruptSnapset)
 *
 * Primary has a valid SnapSet; the replica's SS_ATTR contains garbage bytes
 * (decode fails → SNAPSET_CORRUPTED).  That error must appear in
 * replica_snapset_errors, not in snapset_errors.
 */
TEST(ReplicaSnapsetErrors, ReplicaCorruptSnapset) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  const pg_shard_t replica(1, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "scrub"
  };

  const std::string name = "rsse_corrupt";
  hobject_t head_hoid = so_builder_t::make_hoid(name);

  auto primary_head = so_builder_t::make_head(name).set_size(0).get();

  // Replica: corrupt SS_ATTR (10 bytes of garbage, not a valid SnapSet).
  auto replica_head = primary_head;
  so_set_attr_len(replica_head, SS_ATTR, 10);

  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[head_hoid] = primary_head;
  maps[replica].objects[head_hoid] = replica_head;

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  EXPECT_EQ(ret.snapset_errors.size(), 0u);
  ASSERT_EQ(ret.replica_snapset_errors.size(), 1u) << fmt::format(
    "Expected 1 replica_snapset_errors entry, got {}",
    ret.replica_snapset_errors.size());
  EXPECT_TRUE(ret.replica_snapset_errors.front().snapset_corrupted()) <<
    "Expected SNAPSET_CORRUPTED on the replica entry";
}

/**
 * TEST(ReplicaSnapsetErrors, BothShardsBadSnapset)
 *
 * Both primary and replica have a missing SnapSet.  Primary's error goes to
 * snapset_errors (primary path); replica's error goes to
 * replica_snapset_errors.
 */
TEST(ReplicaSnapsetErrors, BothShardsBadSnapset) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  const pg_shard_t replica(1, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "scrub"
  };

  const std::string name = "both_bad";
  hobject_t head_hoid = so_builder_t::make_hoid(name);

  // Both shards: head with SS_ATTR removed.
  auto base_head = so_builder_t::make_head(name).set_size(0).get();
  base_head.attrs.erase(SS_ATTR);

  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[head_hoid] = base_head;
  maps[replica].objects[head_hoid] = base_head;

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  // When both primary and replica have SNAPSET_MISSING, the replica's shard
  // error promotes the head into object_errors.  The snapset validation loop
  // emits the snapset-missing entry; at least one of snapset_errors or
  // replica_snapset_errors must carry SNAPSET_MISSING.
  bool found_missing = false;
  for (const auto &se : ret.snapset_errors) {
    if (se.snapset_missing()) { found_missing = true; break; }
  }
  for (const auto &se : ret.replica_snapset_errors) {
    if (se.snapset_missing()) { found_missing = true; break; }
  }
  EXPECT_TRUE(found_missing) <<
    "Expected SNAPSET_MISSING in snapset_errors or replica_snapset_errors";

  // The head's SNAPSET_MISSING shard errors must appear in object_errors
  // (promoted by has_replica_snapset_shard_errors).
  EXPECT_GE(ret.object_errors.size(), 1u);
}

/**
 * TEST(ReplicaSnapsetErrors, SkippedWhenReplicaHasOtherErrors)
 *
 * When the replica shard already has a non-snapset shard error (e.g.
 * STAT_ERR), the replica SnapSet evaluation is skipped and
 * replica_snapset_errors must remain empty.
 */
TEST(ReplicaSnapsetErrors, SkippedWhenReplicaHasOtherErrors) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  const pg_shard_t replica(1, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT,
    spg_t{},
    "scrub"
  };

  const std::string name = "skip_replica";
  hobject_t head_hoid = so_builder_t::make_hoid(name);

  auto primary_head = so_builder_t::make_head(name).set_size(0).get();

  // Replica: stat_error=true and also no SS_ATTR.  The stat_error makes
  // skip_replica_snapset_eval true, so the missing snapset should be ignored.
  auto replica_head = primary_head;
  replica_head.attrs.erase(SS_ATTR);
  replica_head.stat_error = true;

  crimson::osd::scrub::scrub_map_set_t maps;
  maps[primary].objects[head_hoid] = primary_head;
  maps[replica].objects[head_hoid] = replica_head;

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(dpp, policy, maps);

  // Replica SnapSet evaluation is skipped due to stat_error; no replica snapset errors.
  EXPECT_TRUE(ret.replica_snapset_errors.empty()) <<
    "replica_snapset_errors must be empty when replica has stat_error";

  // The stat_error is still captured in object_errors.
  EXPECT_GE(ret.object_errors.size(), 1u);
}
