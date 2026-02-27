// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <list>
#include <string>
#include <string_view>

#include "common/mode_collector.h"
#include "gtest/gtest.h"

using mode_status_t = ModeFinder::mode_status_t;

// --- some Ceph structures look-alike ---

struct shard_id_t {
  int8_t id;

  shard_id_t() : id(0) {}
  explicit constexpr shard_id_t(int8_t _id) : id(_id) {}

  explicit constexpr operator int8_t() const { return id; }
  explicit constexpr operator int64_t() const { return id; }
  explicit constexpr operator int() const { return id; }
  explicit constexpr operator unsigned() const { return id; }

  const static shard_id_t NO_SHARD;

  static void generate_test_instances(std::list<shard_id_t*>& ls)
  {
    ls.push_back(new shard_id_t(1));
    ls.push_back(new shard_id_t(2));
  }

  shard_id_t& operator++()
  {
    ++id;
    return *this;
  }
  friend constexpr std::strong_ordering operator<=>(
      const shard_id_t& lhs,
      const shard_id_t& rhs)
  {
    return lhs.id <=> rhs.id;
  }

  friend constexpr std::strong_ordering operator<=>(
      int lhs,
      const shard_id_t& rhs)
  {
    return lhs <=> rhs.id;
  }
  friend constexpr std::strong_ordering operator<=>(
      const shard_id_t& lhs,
      int rhs)
  {
    return lhs.id <=> rhs;
  }

  shard_id_t& operator=(int other)
  {
    id = other;
    return *this;
  }
  bool operator==(const shard_id_t& other) const { return id == other.id; }

  shard_id_t operator+(int other) const { return shard_id_t(id + other); }
  shard_id_t operator-(int other) const { return shard_id_t(id - other); }
};


struct pg_shard_t {
  static const int32_t NO_OSD = 0x7fffffff;
  int32_t osd;
  shard_id_t shard;
  pg_shard_t() : osd(-1), shard(shard_id_t::NO_SHARD) {}
  explicit pg_shard_t(int osd) : osd(osd), shard(shard_id_t::NO_SHARD) {}
  pg_shard_t(int osd, shard_id_t shard) : osd(osd), shard(shard) {}
  bool is_undefined() const { return osd == -1; }
  std::string get_osd() const
  {
    return (osd == NO_OSD ? "NONE" : std::to_string(osd));
  }
  static void generate_test_instances(std::list<pg_shard_t*>& o)
  {
    o.push_back(new pg_shard_t);
    o.push_back(new pg_shard_t(1));
    o.push_back(new pg_shard_t(1, shard_id_t(2)));
  }
  auto operator<=>(const pg_shard_t&) const = default;
};

struct ceph_eversion {
  uint32_t epoch;  // note: originally 'le'32_t
  uint64_t version;
} __attribute__((packed));

typedef uint64_t version_t;
typedef uint32_t
    epoch_t;  // map epoch  (32bits -> 13 epochs/second for 10 years)


class eversion_t {
 public:
  version_t version;
  epoch_t epoch;
  __u32 __pad;
  eversion_t() : version(0), epoch(0), __pad(0) {}
  eversion_t(epoch_t e, version_t v) : version(v), epoch(e), __pad(0) {}

  // cppcheck-suppress noExplicitConstructor
  eversion_t(const ceph_eversion& ce)
      : version(ce.version)
      , epoch(ce.epoch)
      , __pad(0)
  {}


  static const eversion_t& max()
  {
    static const eversion_t max(-1, -1);
    return max;
  }

  operator ceph_eversion()
  {
    ceph_eversion c;
    c.epoch = epoch;
    c.version = version;
    return c;
  }

  static std::list<eversion_t> generate_test_instances()
  {
    std::list<eversion_t> o;
    o.emplace_back();
    o.push_back(eversion_t(1, 2));
    return o;
  }
};

inline bool operator==(const eversion_t& l, const eversion_t& r)
{
  return (l.epoch == r.epoch) && (l.version == r.version);
}
inline bool operator!=(const eversion_t& l, const eversion_t& r)
{
  return (l.epoch != r.epoch) || (l.version != r.version);
}
inline bool operator<(const eversion_t& l, const eversion_t& r)
{
  return (l.epoch == r.epoch) ? (l.version < r.version) : (l.epoch < r.epoch);
}
inline bool operator<=(const eversion_t& l, const eversion_t& r)
{
  return (l.epoch == r.epoch) ? (l.version <= r.version) : (l.epoch <= r.epoch);
}
inline bool operator>(const eversion_t& l, const eversion_t& r)
{
  return (l.epoch == r.epoch) ? (l.version > r.version) : (l.epoch > r.epoch);
}
inline bool operator>=(const eversion_t& l, const eversion_t& r)
{
  return (l.epoch == r.epoch) ? (l.version >= r.version) : (l.epoch >= r.epoch);
}
inline std::ostream& operator<<(std::ostream& out, const eversion_t& e)
{
  return out << e.epoch << "'" << e.version;
}

namespace std {
template <>
struct hash<eversion_t> {
  size_t operator()(const eversion_t& ev) const noexcept
  {
    // Combine epoch and version with a simple shift-based mix
    // This is fast and works well when differences are small
    return (size_t)ev.epoch ^ ((size_t)ev.version << 8 |
			       (size_t)ev.version >> (sizeof(size_t) * 8 - 8));
  }
};
}  // namespace std

// ---------------------------------------------------------------------------
// using uint64_t as key and object ID
using MP_u64_u64_ident = ModeCollector<uint64_t, uint64_t>;

// using pg_shard_t as object ID, and eversion_t (w/ std::hash) as key
using MP_pg_shard_t_eversion_t =
    ModeCollector<pg_shard_t, eversion_t, std::hash<eversion_t>>;


template <typename T, typename DT>
struct ModeCollectorTestB : public ::testing::Test {
  T mc;

  void collect(const DT& test_case)
  {
    for (const auto& [key, value] : test_case.data) {
      mc.insert(key, value);
    }
  }

  void verify(const DT& test_case)
  {
    const auto r = mc.find_mode();
    EXPECT_EQ(r.tag, test_case.expected.tag);
    if (r.tag == mode_status_t::authorative_value) {
      EXPECT_EQ(r.key, test_case.expected.key);
      EXPECT_EQ(r.count, test_case.expected.count);
      // object ID is "arbitrary" - just check it is one of the expected ones
      bool found = false;
      for (const auto& [k, v] : test_case.data) {
	if (v == r.key && k == test_case.expected.id) {
	  found = true;
	  break;
	}
      }
      EXPECT_TRUE(found);
    }
  }

  void expect_wrong_ID(const DT& test_case)
  {
    const auto r = mc.find_mode();
    EXPECT_EQ(r.tag, test_case.expected.tag);
    if (r.tag == mode_status_t::authorative_value) {
      EXPECT_EQ(r.key, test_case.expected.key);
      EXPECT_EQ(r.count, test_case.expected.count);
      // object ID is "arbitrary" - just check it is one of the expected ones
      bool found = false;
      for (const auto& [k, v] : test_case.data) {
	if (v == r.key && k == test_case.expected.id) {
	  found = true;
	  break;
	}
      }
      EXPECT_FALSE(found);
    }
  }
};


// ---------------------------------------------------------------------------
// basic tests - simple keys and objects

struct data_n_res_t {
  std::vector<std::pair<uint64_t, uint64_t>> data;
  MP_u64_u64_ident::results_t expected;
};


using ModeCollectorTest = ModeCollectorTestB<MP_u64_u64_ident, data_n_res_t>;


TEST_F(ModeCollectorTest, basic_ints_1)
{
  static const std::vector<std::pair<uint64_t, uint64_t>> test_data{
      std::pair(1000, 101), std::pair(1001, 2),	  std::pair(1002, 101),
      std::pair(1003, 3),   std::pair(1004, 101),
  };

  static const data_n_res_t test_case{
      test_data, MP_u64_u64_ident::results_t{
		     mode_status_t::authorative_value, 101, 1004, 3}};

  collect(test_case);
  verify(test_case);
}

TEST_F(ModeCollectorTest, basic_ints_2)
{
  static const std::vector<std::pair<uint64_t, uint64_t>> test_data{
      std::pair(1000, 101), std::pair(1001, 2),	  std::pair(1002, 101),
      std::pair(1003, 2),   std::pair(1004, 101), std::pair(1005, 2),
  };

  static const data_n_res_t test_case{
      test_data, MP_u64_u64_ident::results_t{
		     MP_u64_u64_ident::mode_status_t::no_mode_value, 0, 0, 0}};

  collect(test_case);
  verify(test_case);
}

TEST_F(ModeCollectorTest, basic_ints_3)
{
  static const std::vector<std::pair<uint64_t, uint64_t>> test_data{
      std::pair(1000, 101), std::pair(1001, 2), std::pair(1002, 101),
      std::pair(1003, 3),   std::pair(1004, 4), std::pair(1005, 5),
  };

  static const data_n_res_t test_case{
      test_data,
      MP_u64_u64_ident::results_t{
	  MP_u64_u64_ident::mode_status_t::mode_value, 101, 1002, 2}};

  collect(test_case);
  verify(test_case);
}

TEST_F(ModeCollectorTest, basic_ints_4)
{
  static const std::vector<std::pair<uint64_t, uint64_t>> test_data{
      std::pair(1000, 10), std::pair(1001, 11), std::pair(1002, 12),
      std::pair(1003, 13), std::pair(1004, 14), std::pair(1005, 15),
  };

  static const data_n_res_t test_case{
      test_data, MP_u64_u64_ident::results_t{
		     MP_u64_u64_ident::mode_status_t::no_mode_value, 0, 0, 0}};

  collect(test_case);
  verify(test_case);
}

// edge cases

TEST_F(ModeCollectorTest, edge_ints_smallest)
{
  static const std::vector<std::pair<uint64_t, uint64_t>> test_data{
      std::pair(1000, 99),
  };

  static const data_n_res_t test_case{
      test_data, MP_u64_u64_ident::results_t{
		     mode_status_t::authorative_value, 99, 1000, 1}};

  collect(test_case);
  verify(test_case);
}

TEST_F(ModeCollectorTest, edge_ints_pair)
{
  static const std::vector<std::pair<uint64_t, uint64_t>> test_data{
      std::pair(1000, 99),
      std::pair(1001, 55),
  };

  static const data_n_res_t test_case{
      test_data,
      MP_u64_u64_ident::results_t{mode_status_t::no_mode_value, 0, 0, 0}};

  collect(test_case);
  verify(test_case);
}


// ---------------------------------------------------------------------------
// shards & versions

struct data_n_res_t_2 {
  std::vector<std::pair<pg_shard_t, eversion_t>> data;
  MP_pg_shard_t_eversion_t::results_t expected;
};

using ModeCollectorTest_2 =
    ModeCollectorTestB<MP_pg_shard_t_eversion_t, data_n_res_t_2>;


TEST_F(ModeCollectorTest_2, basic_ev_1)
{
  static const std::vector<std::pair<pg_shard_t, eversion_t>> test_data{
      std::pair(pg_shard_t{1}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{2}, eversion_t{1002, 1002}),
      std::pair(pg_shard_t{3}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{4}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{5}, eversion_t{1005, 1005}),
  };

  static const data_n_res_t_2 test_case{
      test_data, MP_pg_shard_t_eversion_t::results_t{
		     mode_status_t::authorative_value, eversion_t{1001, 1001},
		     pg_shard_t{1}, 3}};

  collect(test_case);
  verify(test_case);
}

TEST_F(ModeCollectorTest_2, large_ev_1)
{
  static const std::vector<std::pair<pg_shard_t, eversion_t>> test_data{
      std::pair(pg_shard_t{1}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{2}, eversion_t{1002, 1002}),
      std::pair(pg_shard_t{3}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{4}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{5}, eversion_t{1005, 1005}),
      std::pair(pg_shard_t{6}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{7}, eversion_t{1002, 1002}),
      std::pair(pg_shard_t{8}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{9}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{10}, eversion_t{1005, 1005}),
      std::pair(pg_shard_t{11}, eversion_t{1005, 1005}),
      std::pair(pg_shard_t{12}, eversion_t{1012, 1012}),
      std::pair(pg_shard_t{13}, eversion_t{1013, 1013}),
      std::pair(pg_shard_t{14}, eversion_t{1014, 1014}),
  };

  static const data_n_res_t_2 test_case{
      test_data,
      MP_pg_shard_t_eversion_t::results_t{
	  mode_status_t::mode_value, eversion_t{1001, 1001}, pg_shard_t{1}, 3}};

  collect(test_case);
  verify(test_case);
}


TEST_F(ModeCollectorTest_2, large_ev_2)
{
  static const std::vector<std::pair<pg_shard_t, eversion_t>> test_data{
      std::pair(pg_shard_t{1}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{2}, eversion_t{1002, 1002}),
      std::pair(pg_shard_t{3}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{4}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{5}, eversion_t{1005, 1005}),
      std::pair(pg_shard_t{6}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{7}, eversion_t{1002, 1002}),
      std::pair(pg_shard_t{8}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{9}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{10}, eversion_t{1005, 1005}),
      std::pair(pg_shard_t{11}, eversion_t{1005, 1005}),
      std::pair(pg_shard_t{12}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{13}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{14}, eversion_t{1014, 1014}),
  };

  static const data_n_res_t_2 test_case{
      test_data, MP_pg_shard_t_eversion_t::results_t{
		     mode_status_t::authorative_value, eversion_t{1001, 1001},
		     pg_shard_t{1}, 8}};

  collect(test_case);
  verify(test_case);
}

TEST_F(ModeCollectorTest_2, failedID_ev)
{
  static const std::vector<std::pair<pg_shard_t, eversion_t>> test_data{
      std::pair(pg_shard_t{1}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{2}, eversion_t{1002, 1002}),
      std::pair(pg_shard_t{3}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{4}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{5}, eversion_t{1005, 1005}),
      std::pair(pg_shard_t{6}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{7}, eversion_t{1002, 1002}),
      std::pair(pg_shard_t{8}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{9}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{10}, eversion_t{1005, 1005}),
      std::pair(pg_shard_t{11}, eversion_t{1005, 1005}),
      std::pair(pg_shard_t{12}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{13}, eversion_t{1001, 1001}),
      std::pair(pg_shard_t{14}, eversion_t{1014, 1014}),
  };

  static const data_n_res_t_2 test_case{
      test_data, MP_pg_shard_t_eversion_t::results_t{
		     mode_status_t::authorative_value, eversion_t{1001, 1001},
		     pg_shard_t{14}, 8}};

  collect(test_case);
  expect_wrong_ID(test_case);
}
