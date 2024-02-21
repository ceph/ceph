// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/transform_iterator.hpp>

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

void so_set_hinfo(
  ScrubMap::object &obj, const std::optional<ECUtil::HashInfo> &hinfo)
{
  return so_set_attr_type<ECUtil::HashInfo>(obj, ECUtil::get_hinfo_key(), hinfo);
}

std::optional<ECUtil::HashInfo> so_get_hinfo(ScrubMap::object &obj)
{
  return so_get_attr_type<ECUtil::HashInfo>(obj, ECUtil::get_hinfo_key());
}

template <typename F>
void so_mut_hinfo(ScrubMap::object &obj, F &&f) {
  auto maybe_hinfo = so_get_hinfo(obj);
  auto new_maybe_hinfo = std::invoke(std::forward<F>(f), std::move(maybe_hinfo));
  so_set_hinfo(obj, new_maybe_hinfo);
}

/**
 * so_builder_t
 *
 * Utility class for constructing test objects.
 */
struct so_builder_t {
  ScrubMap::object so;

  void set_defaults() {
    so.size = 0;
    so_mut_oi(so, [](auto maybe_oi) {
      if (maybe_oi) {
	maybe_oi->size = 0;
      }
      return maybe_oi;
    });
  }

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

  static so_builder_t make_head(std::string name) {
    auto hoid = make_hoid(name);
    so_builder_t ret;
    so_set_oi(ret.so, object_info_t{hoid});
    so_set_ss(ret.so, SnapSet{});
    ret.set_defaults();
    return ret;
  }

  static so_builder_t make_clone(
    std::string name,
    snapid_t cloneid = 4
  ) {
    auto hoid = make_hoid(name, cloneid);
    so_builder_t ret;
    so_set_oi(ret.so, object_info_t{hoid});
    ret.set_defaults();
    return ret;
  }

  static so_builder_t make_ec_head(std::string name) {
    auto ret = make_head(name);
    so_set_hinfo(ret.so, ECUtil::HashInfo{});
    return ret;
  }

  static so_builder_t make_ec_clone(
    std::string name,
    snapid_t cloneid = 4
  ) {
    auto ret = make_clone(name, cloneid);
    so_set_hinfo(ret.so, ECUtil::HashInfo{});
    return ret;
  }

  so_builder_t &set_size(
    size_t size,
    const std::optional<ECUtil::stripe_info_t> stripe_info = std::nullopt) {
    if (stripe_info) {
      so.size = stripe_info->logical_to_next_chunk_offset(size);
    } else {
      so.size = size;
    }

    so_mut_oi(so, [size](auto maybe_oi) {
      if (maybe_oi) {
	maybe_oi->size = size;
      }
      return maybe_oi;
    });
    so_mut_hinfo(so, [size, &stripe_info](auto maybe_hinfo) {
      if (maybe_hinfo) {
	ceph_assert(stripe_info);
	maybe_hinfo->set_total_chunk_size_clear_hash(
	  stripe_info->logical_to_next_chunk_offset(size));
      }
      return maybe_hinfo;
    });
    return *this;
  }

  so_builder_t &add_attr(const std::string &name, size_t len) {
    so_set_attr_len(so, name, len);
    return *this;
  }

  ScrubMap::object get() const {
    return so;
  }
};

/**
 * test_obj_t
 *
 * test param combining an so_builder_t with human readable description with
 * a stripe_info.
 */
struct test_obj_t : so_builder_t {
  std::optional<ECUtil::stripe_info_t> stripe_info;
  std::string desc;
  hobject_t hoid;

  test_obj_t(
    so_builder_t _builder,
    std::optional<ECUtil::stripe_info_t> _stripe_info,
    std::string _desc,
    hobject_t _hoid) :
    so_builder_t(std::move(_builder)),
    stripe_info(std::move(_stripe_info)),
    desc(std::move(_desc)),
    hoid(std::move(_hoid)) {
    ceph_assert(!desc.empty());
  }

  static test_obj_t make(
    const std::string &desc,
    std::optional<ECUtil::stripe_info_t> stripe_info,
    so_builder_t builder) {
    hobject_t hoid = so_get_oi(builder.so)->soid;
    return test_obj_t{
      std::move(builder),
      stripe_info,
      desc,
      std::move(hoid)};
  }

  template <typename... Args>
  static test_obj_t make_head(const std::string &desc, Args&&... args) {
    return make(
      desc,
      std::nullopt,
      so_builder_t::make_head(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static test_obj_t make_clone(const std::string &desc, Args&&... args) {
    return make(
      desc,
      std::nullopt,
      so_builder_t::make_clone(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static test_obj_t make_ec_head(const std::string &desc, Args&&... args) {
    return make(
      desc,
      ECUtil::stripe_info_t{4, 1<<20},
      so_builder_t::make_ec_head(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static test_obj_t make_ec_clone(const std::string &desc, Args&&... args) {
    return make(
      desc,
      ECUtil::stripe_info_t{4, 1<<20},
      so_builder_t::make_ec_clone(std::forward<Args>(args)...));
  }

  test_obj_t &set_size(
    size_t size) {
    so_builder_t::set_size(size, stripe_info);
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

  /// returns expected shard error
  virtual librados::err_t get_shard_error_sig() const = 0;

  /// returns expected object error
  virtual librados::obj_err_t get_object_error_sig() const = 0;

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

  librados::err_t get_shard_error_sig() const final {
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
    obj.so.ec_hash_mismatch = true;
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
    obj.so.ec_size_mismatch = true;
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
    obj.so.read_error = true;
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
    obj.so.stat_error = true;
    return obj;
  }
};

struct MissingOI : SingleErrorTestCaseT<MissingOI> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::INFO_MISSING
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::OBJECT_INFO_INCONSISTENCY
  };

  std::string_view get_description() const {
    return "MissingOI";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    so_mut_oi(obj.so, [](auto) { return std::nullopt; });
    return obj;
  }
};

struct CorruptOI: SingleErrorTestCaseT<CorruptOI> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::INFO_CORRUPTED
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::OBJECT_INFO_INCONSISTENCY
  };

  std::string_view get_description() const {
    return "CorruptOI";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    so_set_attr_len(obj.so, OI_ATTR, 10);
    return obj;
  }
};

struct CorruptOndiskSize : SingleErrorTestCaseT<CorruptOndiskSize> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SIZE_MISMATCH_INFO
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::SIZE_MISMATCH
  };

  std::string_view get_description() const {
    return "CorruptOndiskSize";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    obj.so.size += 2;
    return obj;
  }
};

struct MissingSS : SingleErrorTestCaseT<MissingSS> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SNAPSET_MISSING
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::SNAPSET_INCONSISTENCY
  };
  constexpr static bool REQUIRES_HEAD = true;

  std::string_view get_description() const {
    return "MissingSS";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(obj.hoid.is_head());
    so_mut_ss(obj.so, [](auto) { return std::nullopt; });
    return obj;
  }
};

struct CorruptSS : SingleErrorTestCaseT<CorruptSS> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::SNAPSET_CORRUPTED
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::SNAPSET_INCONSISTENCY
  };
  constexpr static bool REQUIRES_HEAD = true;

  std::string_view get_description() const {
    return "CorruptSS";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(obj.hoid.is_head());
    so_set_attr_len(obj.so, SS_ATTR, 10);
    return obj;
  }
};

struct MissingHinfo : SingleErrorTestCaseT<MissingHinfo> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::HINFO_MISSING
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::HINFO_INCONSISTENCY
  };
  constexpr static bool REQUIRES_EC = true;

  std::string_view get_description() const {
    return "MissingHinfo";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(obj.stripe_info);
    so_mut_hinfo(obj.so, [](auto) { return std::nullopt; });
    return obj;
  }
};

struct CorruptHinfo : SingleErrorTestCaseT<CorruptHinfo> {
  constexpr static librados::err_t shard_error_sig{
    librados::err_t::HINFO_CORRUPTED
  };
  constexpr static librados::obj_err_t object_error_sig{
    librados::obj_err_t::HINFO_INCONSISTENCY
  };
  constexpr static bool REQUIRES_EC = true;

  std::string_view get_description() const {
    return "CorruptHinfo";
  };
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(obj.stripe_info);
    so_set_attr_len(obj.so, ECUtil::get_hinfo_key(), 10);
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

  std::string_view get_description() const {
    return "DataDigestMismatch";
  };
  test_obj_t adjust_base_object(test_obj_t obj) const {
    so_mut_oi(obj.so, [](auto maybe_oi) {
      ceph_assert(maybe_oi);
      maybe_oi->set_data_digest(1);
      return maybe_oi;
    });
    obj.so.digest_present = true;
    obj.so.digest = 1;
    return obj;
  }
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(so_get_oi(obj.so)->is_data_digest());
    obj.so.digest = 2;
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

  std::string_view get_description() const {
    return "OmapDigestMismatch";
  };
  test_obj_t adjust_base_object(test_obj_t obj) const {
    so_mut_oi(obj.so, [](auto maybe_oi) {
      ceph_assert(maybe_oi);
      maybe_oi->set_omap_digest(1);
      return maybe_oi;
    });
    obj.so.omap_digest_present = true;
    obj.so.omap_digest = 1;
    return obj;
  }
  test_obj_t inject_error(test_obj_t obj) const {
    ceph_assert(so_get_oi(obj.so)->is_omap_digest());
    obj.so.omap_digest = 2;
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
    so_set_attr_len(obj.so, "attr_added_erroneously", 10);
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
    so_set_attr_len(obj.so, "attr_to_be_missing", 10);
    return obj;
  }
  test_obj_t inject_error(test_obj_t obj) const {
    obj.so.attrs.erase("attr_to_be_missing");
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
  std::make_unique<MissingHinfo>(),
  std::make_unique<CorruptHinfo>(),
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
    obj.stripe_info,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT
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

  EXPECT_EQ(
    ret.stats.num_shallow_scrub_errors,
    (obj_error.has_shallow_errors() ||
     obj_error.union_shards.has_shallow_errors()) +
    ret.snapset_errors.size());
  EXPECT_EQ(
    ret.stats.num_deep_scrub_errors,
    (obj_error.has_deep_errors() ||
     obj_error.union_shards.has_deep_errors()));

  EXPECT_TRUE(compare_error_signatures(
    static_cast<const librados::obj_err_t&>(obj_error),
    test_case->get_object_error_sig()));

  EXPECT_EQ(obj_error.shards.size(), shards.size());
  bool found_selected_oi = false;
  for (const auto &shard : shards) {
    auto siter = obj_error.shards.find(
      librados::osd_shard_t(shard.osd, shard.shard)
    );
    if (siter == obj_error.shards.end()) {
      EXPECT_NE(siter, obj_error.shards.end());
      continue;
    }
    if (shard == target) {
      EXPECT_TRUE(compare_error_signatures(
	static_cast<const librados::err_t&>(siter->second),
	test_case->get_shard_error_sig()));
    } else {
      EXPECT_FALSE(siter->second.has_errors());
      if (siter->second.selected_oi) found_selected_oi = true;
    }
    if (shard == primary) {
      EXPECT_TRUE(siter->second.primary);
    }
  }
  EXPECT_TRUE(found_selected_oi);
}

/* Tests that don't have restrictions */
INSTANTIATE_TEST_SUITE_P(
  SingleErrorGeneral,
  TestSingleError,
  ::testing::Combine(
    ::testing::Values(
      test_obj_t::make_head("Small", "foo").set_size(64),
      test_obj_t::make_clone("EmptyWithAttr", "foo2").add_attr("extra_attr", 64),
      test_obj_t::make_head("ReplicatedRBD", "foo2").set_size(4<<20),
      test_obj_t::make_ec_head("ECHead", "foo").set_size(4<<20),
      test_obj_t::make_ec_clone("LargeECClone", "foo").set_size(16<<20)
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
      test_obj_t::make_head("ReplicatedRBD", "foo2").set_size(4<<20),
      test_obj_t::make_ec_head("ECHead", "foo").set_size(4<<20),
      test_obj_t::make_ec_clone("LargeECClone", "foo").set_size(16<<20)
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

/* Some tests only make sense on ec objects. */
INSTANTIATE_TEST_SUITE_P(
  SingleErrorOnly,
  TestSingleError,
  ::testing::Combine(
    ::testing::Values(
      test_obj_t::make_ec_head("ECHead", "foo").set_size(4<<20),
      test_obj_t::make_ec_clone("LargeECClone", "foo").set_size(16<<20)
    ),
    ::testing::Bool(),
    ::testing::ValuesIn(
      test_cases_begin<SingleErrorTestCase::restriction_t::EC_ONLY>(),
      test_cases_end<SingleErrorTestCase::restriction_t::EC_ONLY>())
  ),
  [](const auto &info) {
    return fmt::format("{}", info.param);
  }
);

/* Some tests only make sense on head objects. */
INSTANTIATE_TEST_SUITE_P(
  SingleErrorHEAD,
  TestSingleError,
  ::testing::Combine(
    ::testing::Values(
      test_obj_t::make_head("Small", "foo").set_size(64),
      test_obj_t::make_head("ReplicatedRBD", "foo2").set_size(4<<20),
      test_obj_t::make_ec_head("ECHead", "foo").set_size(4<<20)
    ),
    ::testing::Bool(),
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
    std::nullopt,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT
  };

  crimson::osd::scrub::scrub_map_set_t maps;
  const std::string name = "test_obj";
  auto &map = maps[primary];
  inconsistent_snapset_wrapper expected_error;

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
	expected_error.set_size_mismatch();
	to_insert.second = so_builder_t(to_insert.second).set_size(
	  so_get_oi(to_insert.second)->size + 1).get();
      }
      map.objects.insert(to_insert);
    } else {
      expected_error.set_clone_missing(should_exist[i].first);
    }
    if (GetParam().should_inject_extra(i)) {
      map.objects.insert(make_clone(name, extra[i]));
      expected_error.set_clone(extra[i].first);
    }
  }

  hobject_t hoid = so_builder_t::make_hoid(name);
  map.objects[hoid] = so_builder_t::make_head(name).get();

  so_set_ss(map.objects[hoid], make_snapset(should_exist));

  DoutPrefix dpp(nullptr, ceph_subsys_test, "test_crimson_scrub");
  const auto ret = crimson::osd::scrub::validate_chunk(
    dpp, policy, maps);
  EXPECT_EQ(ret.object_errors.size(), 0);
  ASSERT_EQ(ret.snapset_errors.size(), 1) << fmt::format(
    "Got snapset_errors: {}", ret.snapset_errors);

  EXPECT_TRUE(compare_error_signatures(
    ret.snapset_errors.front(),
    expected_error));

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
    std::nullopt,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT
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
  ASSERT_EQ(ret.snapset_errors.size(), 1) << fmt::format(
    "Got snapset_errors: {}", ret.snapset_errors);

  EXPECT_TRUE(compare_error_signatures(
    ret.snapset_errors.front(),
    expected_error));

}

TEST(TestSnapSet, Stats) {
  const pg_shard_t primary(0, shard_id_t::NO_SHARD);
  crimson::osd::scrub::chunk_validation_policy_t policy {
    primary,
    std::nullopt,
    TEST_MAX_OBJECT_SIZE,
    std::string{TEST_INTERNAL_NAMESPACE},
    TEST_OMAP_KEY_LIMIT,
    TEST_OMAP_BYTES_LIMIT
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
    auto &so = add_simple_head(32, [](auto ret) { return ret; });
    expected_stats.num_omap_keys += (so.object_omap_keys = 10);
    expected_stats.num_omap_bytes += (so.object_omap_bytes = 100);
    expected_stats.num_objects_omap++;
  }

  {
    auto &so = add_simple_head(64, [](auto ret) { return ret; });
    expected_stats.num_omap_keys +=
      (so.object_omap_keys = (TEST_OMAP_KEY_LIMIT + 1));
    expected_stats.num_omap_bytes +=
      (so.object_omap_bytes = so.object_omap_keys);
    expected_stats.num_objects_omap++;
    expected_stats.num_large_omap_objects++;
  }

  {
    auto &so = add_simple_head(64, [](auto ret) { return ret; });
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

  EXPECT_EQ(ret.stats, expected_stats);
}
