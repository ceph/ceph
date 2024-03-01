#include <string>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include "common/hobject.h"
#include "gtest/gtest.h"

using namespace std::string_literals;
using std::string;

TEST(HObject, cmp)
{
  hobject_t c{object_t{"fooc"}, "food", CEPH_NOSNAP, 42, 0, "nspace"};
  hobject_t d{object_t{"food"}, "",     CEPH_NOSNAP, 42, 0, "nspace"};
  hobject_t e{object_t{"fooe"}, "food", CEPH_NOSNAP, 42, 0, "nspace"};
  ASSERT_EQ(-1, cmp(c, d));
  ASSERT_EQ(-1, cmp(d, e));
}

// ---- test methods that 'stringify' the object while escaping special characters ----


/*
 * Two methods are used here: first - using a preset list of objects & hobjects,
 * comparing the output to the expected string; and a second method: comparing the
 * output for a "random"(*) object to the results when using the code from 'Squid'.
 *
 * (*) the object is not random, but it's not part of the preset list.
 */


struct obj_n_expected_t {
  hobject_t obj;
  std::string expected_to_str;
  std::string expected_fmt;
};

static std::vector<obj_n_expected_t> known_examples = {

    // the first entry will be modified (by setting the max flag)
    {hobject_t{}, "MAX", "MAX"},

    {hobject_t{object_t("o%:/name2"), "aaaa"s, CEPH_NOSNAP, 67, 0, "n1"s},
     "0000000000000000.34000000.head.o%p:/name2.aaaa.n1",
     "0:c2000000:n1:aaaa:o%25%3a%2fname2:head"},

    {hobject_t{object_t("okey"), "okey"s, CEPH_NOSNAP, 1, 0, "n12"s},
     "0000000000000000.10000000.head.okey..n12", "0:80000000:n12::okey:head"},

    {hobject_t{}, "8000000000000000.00000000.0...", "MIN"},

/// \todo not sure whether the '-1' or the 'FFF..' is correct:
#if 0
   {hobject_t{object_t("oname"), std::string{}, 1, 234, -1, ""s},
      "FFFFFFFFFFFFFFFF.AE000000.1.oxxname..",
     "18446744073709551615:57000000:::oname:1"},
#endif
    {hobject_t{object_t{"oname3"}, "oname3"s, CEPH_SNAPDIR, 910, 1, "n2"s},
     "0000000000000001.E8300000.snapdir.oname3..n2",
     "1:71c00000:n2::oname3:snapdir"},

    {hobject_t{
	 object_t("nonprint\030%_%.%"), "c"s, 0x12345678, 0xe0e0f0f0, 0x2727,
	 "n5"s},
     "0000000000002727.0F0F0E0E.12345678.nonprint\x18%p%u%p%e%p.c.n5",
     "10023:0f0f0707:n5:c:nonprint%18%25_%25.%25:12345678"},

    {hobject_t{object_t("o//////"), string("ZZ"), 0xaaaa, 65, 1, "zzzzz"},
     "0000000000000001.14000000.aaaa.o//////.ZZ.zzzzz",
     "1:82000000:zzzzz:ZZ:o%2f%2f%2f%2f%2f%2f:aaaa"}};

// original Ceph code as it was in version Squid

struct test_hobject_fmt_t : public hobject_t {

  template <typename... ARGS>
  test_hobject_fmt_t(ARGS&&... args) : hobject_t{std::forward<ARGS>(args)...}
  {}

  test_hobject_fmt_t(const test_hobject_fmt_t& rhs) = default;
  test_hobject_fmt_t(test_hobject_fmt_t&& rhs) = default;
  test_hobject_fmt_t& operator=(const test_hobject_fmt_t& rhs) = default;
  test_hobject_fmt_t& operator=(test_hobject_fmt_t&& rhs) = default;
  test_hobject_fmt_t(hobject_t_max&& singleton) : test_hobject_fmt_t()
  {
    max = true;
  }
  test_hobject_fmt_t& operator=(hobject_t_max&& singleton)
  {
    *this = hobject_t();
    max = true;
    return *this;
  }
  bool is_max() const { return max; }
  bool is_min() const
  {
    // this needs to match how it's constructed
    return snap == 0 && hash == 0 && !max && pool == INT64_MIN;
  }

  auto operator<=>(const test_hobject_fmt_t& rhs) const noexcept
  {
    auto cmp = is_max() <=> rhs.is_max();
    if (cmp != 0)
      return cmp;
    cmp = pool <=> rhs.pool;
    if (cmp != 0)
      return cmp;
    cmp = get_bitwise_key() <=> rhs.get_bitwise_key();
    if (cmp != 0)
      return cmp;
    cmp = nspace <=> rhs.nspace;
    if (cmp != 0)
      return cmp;
    if (!(get_key().empty() && rhs.get_key().empty())) {
      cmp = get_effective_key() <=> rhs.get_effective_key();
      if (cmp != 0)
	return cmp;
    }
    cmp = oid <=> rhs.oid;
    if (cmp != 0)
      return cmp;
    return snap <=> rhs.snap;
  }
  bool operator==(const hobject_t& rhs) const noexcept
  {
    return operator<=>(rhs) == 0;
  }
};

static inline void append_out_escaped(const std::string& in, std::string* out)
{
  for (auto i = in.cbegin(); i != in.cend(); ++i) {
    if (*i == '%' || *i == ':' || *i == '/' || *i < 32 || *i >= 127) {
      char buf[4];
      snprintf(buf, sizeof(buf), "%%%02x", (int)(unsigned char)*i);
      out->append(buf);
    } else {
      out->push_back(*i);
    }
  }
}

// why don't we escape non-printable characters?
static void append_escaped(const string& in, string* out)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%') {
      out->push_back('%');
      out->push_back('p');
    } else if (*i == '.') {
      out->push_back('%');
      out->push_back('e');
    } else if (*i == '_') {
      out->push_back('%');
      out->push_back('u');
    } else {
      out->push_back(*i);
    }
  }
}

// original Ceph code as it was in version Squid
string hobject_t::to_str() const
{
  string out;

  char snap_with_hash[1000];
  char* t = snap_with_hash;
  const char* end = t + sizeof(snap_with_hash);

  uint64_t poolid(pool);
  t += snprintf(t, end - t, "%.*llX", 16, (long long unsigned)poolid);

  uint32_t revhash(get_nibblewise_key_u32());
  t += snprintf(t, end - t, ".%.*X", 8, revhash);

  if (snap == CEPH_NOSNAP)
    t += snprintf(t, end - t, ".head");
  else if (snap == CEPH_SNAPDIR)
    t += snprintf(t, end - t, ".snapdir");
  else
    t += snprintf(t, end - t, ".%llx", (long long unsigned)snap);

  out.append(snap_with_hash, t);

  out.push_back('.');
  append_escaped(oid.name, &out);
  out.push_back('.');
  append_escaped(get_key(), &out);
  out.push_back('.');
  append_escaped(nspace, &out);

  return out;
}


namespace fmt {
// original Ceph code as it was in version Squid
// (modified to use test_hobject_fmt_t)
template <>
struct formatter<test_hobject_fmt_t> {

  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const test_hobject_fmt_t& ho, FormatContext& ctx)
  {
    if (ho == hobject_t{}) {
      return fmt::format_to(ctx.out(), "MIN");
    }

    if (ho.is_max()) {
      return fmt::format_to(ctx.out(), "MAX");
    }

    std::string v;
    append_out_escaped(ho.nspace, &v);
    v.push_back(':');
    append_out_escaped(ho.get_key(), &v);
    v.push_back(':');
    append_out_escaped(ho.oid.name, &v);

    return fmt::format_to(
	ctx.out(), "{}:{:08x}:{}:{}", static_cast<uint64_t>(ho.pool),
	ho.get_bitwise_key_u32(), v, ho.snap);
  }
};
}  // namespace fmt


TEST(HObject, to_str)
{
  const auto dbg = false;  // turns on debug output
  known_examples[0].obj = hobject_t::get_max();

  for (const auto& [obj, expected_to_str, expected_fmt] : known_examples) {
    if (obj.is_max()) {
      // no 'max' for to_str()
      continue;
    }
    test_hobject_fmt_t legacy_obj{obj};
    if (dbg) {
      std::cout << "to_str(): legacy: " << legacy_obj.to_str()
		<< " . Now: " << obj.to_str() << std::endl;
    }
    EXPECT_EQ(legacy_obj.to_str(), obj.to_str());
    EXPECT_EQ(expected_to_str, obj.to_str());
  }
}

// test the fmt::formatter for hobject_t vs legacy & the stream operator
TEST(HObject, fmt)
{
  const auto dbg = false;  // turns on debug output
  known_examples[0].obj = hobject_t::get_max();

  for (const auto& [obj, expected_to_str, expected_fmt] : known_examples) {

    test_hobject_fmt_t legacy_obj{obj};
    if (dbg) {
      std::cout << fmt::format("fmt: legacy: {} now: {}", legacy_obj, obj)
		<< std::endl;
    }
    EXPECT_EQ(fmt::format("{}", legacy_obj), fmt::format("{}", obj));
    EXPECT_EQ(expected_fmt, fmt::format("{}", obj));

    if (dbg) {
      std::cout << "ostream: legacy: " << legacy_obj << " . Now: " << obj
		<< std::endl;
    }
    std::ostringstream oss;
    oss << obj;
    std::ostringstream oss_legacy;
    oss_legacy << legacy_obj;
    EXPECT_EQ(oss_legacy.str(), oss.str());
    EXPECT_EQ(oss.str(), fmt::format("{}", obj));
  }
}

TEST(HObject, fmt_random)
{
  const auto dbg = false;  // turns on debug output
  for (uint32_t i = 0; i < 10; i++) {

    auto name_length = (i * 17) % 51;
    std::string name;
    for (int j = 0; j < name_length; j++) {
      name.push_back((i * name_length + j) % 256);
    }

    std::string key =
	(i % 3) ? fmt::format("key_{}::", static_cast<unsigned char>(i)) : name;

    snapid_t snap = (i % 7) ? i : ((i % 2) ? CEPH_SNAPDIR : CEPH_NOSNAP);

    hobject_t obj{object_t{name}, key, snap, i, i % 10, "n:_%.space"s};

    test_hobject_fmt_t legacy_obj{obj};

    if (dbg) {
      std::cout << fmt::format("fmt: legacy: {} now: {}", legacy_obj, obj)
		<< std::endl;
    }
    EXPECT_EQ(fmt::format("{}", legacy_obj), fmt::format("{}", obj));

    if (dbg) {
      std::cout << "ostream: legacy: " << legacy_obj << " . Now: " << obj
		<< std::endl;
    }
    std::ostringstream oss;
    oss << obj;
    std::ostringstream oss_legacy;
    oss_legacy << legacy_obj;
    EXPECT_EQ(oss_legacy.str(), oss.str());
    EXPECT_EQ(oss.str(), fmt::format("{}", obj));
  }
}
