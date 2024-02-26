// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <iostream>
#include <sstream>

#include "test/crimson/gtest_seastar.h"

#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/seastore/seastore.h"
#include "crimson/os/seastore/onode.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using SeaStoreShard = FuturizedStore::Shard;
using CTransaction = ceph::os::Transaction;
using namespace std;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

ghobject_t make_oid(int i) {
  stringstream ss;
  ss << "object_" << i;
  auto ret = ghobject_t(
    hobject_t(
      sobject_t(ss.str(), CEPH_NOSNAP)));
  ret.set_shard(shard_id_t(shard_id_t::NO_SHARD));
  ret.hobj.nspace = "asdf";
  ret.hobj.pool = 0;
  uint32_t reverse_hash = hobject_t::_reverse_bits(0);
  ret.hobj.set_bitwise_key_u32(reverse_hash + i * 100);
  return ret;
}

ghobject_t make_temp_oid(int i) {
  stringstream ss;
  ss << "temp_object_" << i;
  auto ret = ghobject_t(
    hobject_t(
      sobject_t(ss.str(), CEPH_NOSNAP)));
  ret.set_shard(shard_id_t(shard_id_t::NO_SHARD));
  ret.hobj.nspace = "hjkl";
  ret.hobj.pool = -2ll;
  uint32_t reverse_hash = hobject_t::_reverse_bits(0);
  ret.hobj.set_bitwise_key_u32(reverse_hash + i * 100);
  return ret;
}

struct seastore_test_t :
  public seastar_test_suite_t,
  SeaStoreTestState {

  coll_t coll_name{spg_t{pg_t{0, 0}}};
  CollectionRef coll;

  seastore_test_t() {}

  seastar::future<> set_up_fut() final {
    return tm_setup(
    ).then([this] {
      return sharded_seastore->create_new_collection(coll_name);
    }).then([this](auto coll_ref) {
      coll = coll_ref;
      CTransaction t;
      t.create_collection(coll_name, 0);
      return sharded_seastore->do_transaction(
	coll,
	std::move(t));
    });
  }

  seastar::future<> tear_down_fut() final {
    coll.reset();
    return tm_teardown();
  }

  void do_transaction(CTransaction &&t) {
    return sharded_seastore->do_transaction(
      coll,
      std::move(t)).get0();
  }

  void set_meta(
    const std::string& key,
    const std::string& value) {
    return seastore->write_meta(key, value).get0();
  }

  std::tuple<int, std::string> get_meta(
    const std::string& key) {
    return seastore->read_meta(key).get();
  }

  struct object_state_t {
    const coll_t cid;
    const CollectionRef coll;
    const ghobject_t oid;

    std::map<string, bufferlist> omap;
    bufferlist contents;

    std::map<snapid_t, bufferlist> clone_contents;

    void touch(
      CTransaction &t) {
      t.touch(cid, oid);
    }

    void touch(
      SeaStoreShard &sharded_seastore) {
      CTransaction t;
      touch(t);
      sharded_seastore.do_transaction(
        coll,
        std::move(t)).get0();
    }

    void truncate(
      CTransaction &t,
      uint64_t off) {
      t.truncate(cid, oid, off);
    }

    void truncate(
      SeaStoreShard &sharded_seastore,
      uint64_t off) {
      CTransaction t;
      truncate(t, off);
      sharded_seastore.do_transaction(
        coll,
        std::move(t)).get0();
    }

    std::map<uint64_t, uint64_t> fiemap(
      SeaStoreShard &sharded_seastore,
      uint64_t off,
      uint64_t len) {
      return sharded_seastore.fiemap(coll, oid, off, len).unsafe_get0();
    }

    bufferlist readv(
      SeaStoreShard &sharded_seastore,
      interval_set<uint64_t>&m) {
      return sharded_seastore.readv(coll, oid, m).unsafe_get0();
    }

    void remove(
      CTransaction &t) {
      t.remove(cid, oid);
      t.remove_collection(cid);
    }

    void remove(
      SeaStoreShard &sharded_seastore) {
      CTransaction t;
      remove(t);
      sharded_seastore.do_transaction(
        coll,
        std::move(t)).get0();
    }

    void set_omap(
      CTransaction &t,
      const string &key,
      const bufferlist &val) {
      omap[key] = val;
      std::map<string, bufferlist> arg;
      arg[key] = val;
      t.omap_setkeys(
	cid,
	oid,
	arg);
    }

    void set_omap(
      SeaStoreShard &sharded_seastore,
      const string &key,
      const bufferlist &val) {
      CTransaction t;
      set_omap(t, key, val);
      sharded_seastore.do_transaction(
	coll,
	std::move(t)).get0();
    }

    void write(
      SeaStoreShard &sharded_seastore,
      CTransaction &t,
      uint64_t offset,
      bufferlist bl)  {
      bufferlist new_contents;
      if (offset > 0 && contents.length()) {
	new_contents.substr_of(
	  contents,
	  0,
	  std::min<size_t>(offset, contents.length())
	);
      }
      new_contents.append_zero(offset - new_contents.length());
      new_contents.append(bl);

      auto tail_offset = offset + bl.length();
      if (contents.length() > tail_offset) {
	bufferlist tail;
	tail.substr_of(
	  contents,
	  tail_offset,
	  contents.length() - tail_offset);
	new_contents.append(tail);
      }
      contents.swap(new_contents);

      t.write(
	cid,
	oid,
	offset,
	bl.length(),
	bl);
    }

    void write(
      SeaStoreShard &sharded_seastore,
      uint64_t offset,
      bufferlist bl)  {
      CTransaction t;
      write(sharded_seastore, t, offset, bl);
      sharded_seastore.do_transaction(
	coll,
	std::move(t)).get0();
    }

    void clone(
      SeaStoreShard &sharded_seastore,
      snapid_t snap) {
      ghobject_t coid = oid;
      coid.hobj.snap = snap;
      CTransaction t;
      t.clone(cid, oid, coid);
      sharded_seastore.do_transaction(
	coll,
	std::move(t)).get0();
      clone_contents[snap].reserve(contents.length());
      auto it = contents.begin();
      it.copy_all(clone_contents[snap]);
    }

    object_state_t get_clone(snapid_t snap) {
      auto coid = oid;
      coid.hobj.snap = snap;
      auto clone_obj = object_state_t{cid, coll, coid};
      clone_obj.contents.reserve(clone_contents[snap].length());
      auto it = clone_contents[snap].begin();
      it.copy_all(clone_obj.contents);
      return clone_obj;
    }

    void rename(
      SeaStoreShard &sharded_seastore,
      object_state_t &other) {
      CTransaction t;
      t.collection_move_rename(cid, oid, cid, other.oid);
      sharded_seastore.do_transaction(
	coll,
	std::move(t)).get0();
      other.contents = contents;
      other.omap = omap;
      other.clone_contents = clone_contents;
    }

    void write(
      SeaStoreShard &sharded_seastore,
      uint64_t offset,
      size_t len,
      char fill)  {
      auto buffer = bufferptr(buffer::create(len));
      ::memset(buffer.c_str(), fill, len);
      bufferlist bl;
      bl.append(buffer);
      write(sharded_seastore, offset, bl);
    }

    void zero(
      SeaStoreShard &sharded_seastore,
      CTransaction &t,
      uint64_t offset,
      size_t len) {
      ceph::buffer::list bl;
      bl.append_zero(len);
      bufferlist new_contents;
      if (offset > 0 && contents.length()) {
        new_contents.substr_of(
          contents,
          0,
          std::min<size_t>(offset, contents.length())
        );
      }
      new_contents.append_zero(offset - new_contents.length());
      new_contents.append(bl);

      auto tail_offset = offset + bl.length();
      if (contents.length() > tail_offset) {
        bufferlist tail;
        tail.substr_of(
          contents,
          tail_offset,
          contents.length() - tail_offset);
        new_contents.append(tail);
      }
      contents.swap(new_contents);

      t.zero(
        cid,
        oid,
        offset,
        len);
    }

    void zero(
      SeaStoreShard &sharded_seastore,
      uint64_t offset,
      size_t len) {
      CTransaction t;
      zero(sharded_seastore, t, offset, len);
      sharded_seastore.do_transaction(
        coll,
        std::move(t)).get0();
    }

    void read(
      SeaStoreShard &sharded_seastore,
      uint64_t offset,
      uint64_t len) {
      bufferlist to_check;
      if (contents.length() >= offset) {
	to_check.substr_of(
	  contents,
	  offset,
	  std::min(len, (uint64_t)contents.length()));
      }
      auto ret = sharded_seastore.read(
	coll,
	oid,
	offset,
	len).unsafe_get0();
      EXPECT_EQ(ret.length(), to_check.length());
      EXPECT_EQ(ret, to_check);
    }

    void check_size(SeaStoreShard &sharded_seastore) {
      auto st = sharded_seastore.stat(
	coll,
	oid).get0();
      EXPECT_EQ(contents.length(), st.st_size);
    }

    void set_attr(
      SeaStoreShard &sharded_seastore,
      std::string key,
      bufferlist& val) {
      CTransaction t;
      t.setattr(cid, oid, key, val);
      sharded_seastore.do_transaction(
        coll,
        std::move(t)).get0();
    }

    void rm_attr(
      SeaStoreShard &sharded_seastore,
      std::string key) {
      CTransaction t;
      t.rmattr(cid, oid, key);
      sharded_seastore.do_transaction(
        coll,
        std::move(t)).get0();
    }

    void rm_attrs(
      SeaStoreShard &sharded_seastore) {
      CTransaction t;
      t.rmattrs(cid, oid);
      sharded_seastore.do_transaction(
        coll,
        std::move(t)).get0();
    }

    SeaStoreShard::attrs_t get_attrs(
      SeaStoreShard &sharded_seastore) {
      return sharded_seastore.get_attrs(coll, oid)
	.handle_error(
	  SeaStoreShard::get_attrs_ertr::assert_all{"unexpected error"})
	.get();
    }

    ceph::bufferlist get_attr(
      SeaStoreShard& sharded_seastore,
      std::string_view name) {
      return sharded_seastore.get_attr(coll, oid, name)
	.handle_error(
	  SeaStoreShard::get_attr_errorator::assert_all{"unexpected error"})
	.get();
    }

    void check_omap_key(
      SeaStoreShard &sharded_seastore,
      const string &key) {
      std::set<string> to_check;
      to_check.insert(key);
      auto result = sharded_seastore.omap_get_values(
	coll,
	oid,
	to_check).unsafe_get0();
      if (result.empty()) {
	EXPECT_EQ(omap.find(key), omap.end());
      } else {
	auto iter = omap.find(key);
	EXPECT_NE(iter, omap.end());
	if (iter != omap.end()) {
	  EXPECT_EQ(result.size(), 1);
	  EXPECT_EQ(iter->second, result.begin()->second);
	}
      }
    }

    void check_omap(SeaStoreShard &sharded_seastore) {
      auto refiter = omap.begin();
      std::optional<std::string> start;
      while(true) {
        auto [done, kvs] = sharded_seastore.omap_get_values(
          coll,
          oid,
          start).unsafe_get0();
        auto iter = kvs.begin();
        while (true) {
	  if ((done && iter == kvs.end()) && refiter == omap.end()) {
	    return; // finished
          } else if (!done && iter == kvs.end()) {
	    break; // reload kvs
          }
          if (iter == kvs.end() || refiter->first < iter->first) {
	    logger().debug(
	      "check_omap: missing omap key {}",
	      refiter->first);
	    GTEST_FAIL() << "missing omap key " << refiter->first;
	    ++refiter;
          } else if (refiter == omap.end() || refiter->first > iter->first) {
	    logger().debug(
	      "check_omap: extra omap key {}",
	      iter->first);
	    GTEST_FAIL() << "extra omap key " << iter->first;
            ++iter;
          } else {
	    EXPECT_EQ(iter->second, refiter->second);
            ++iter;
            ++refiter;
          }
        }
        if (!done) {
          start = kvs.rbegin()->first;
        }
      }
    }
  };

  map<ghobject_t, object_state_t> test_objects;
  object_state_t &get_object(
    const ghobject_t &oid) {
    return test_objects.emplace(
      std::make_pair(
	oid,
	object_state_t{coll_name, coll, oid})).first->second;
  }

  void remove_object(
    object_state_t &sobj) {

    sobj.remove(*sharded_seastore);
    auto erased = test_objects.erase(sobj.oid);
    ceph_assert(erased == 1);
  }

  void validate_objects() const {
    std::vector<ghobject_t> oids;
    for (auto& [oid, obj] : test_objects) {
      oids.emplace_back(oid);
    }
    auto ret = sharded_seastore->list_objects(
        coll,
        ghobject_t(),
        ghobject_t::get_max(),
        std::numeric_limits<uint64_t>::max()).get0();
    EXPECT_EQ(std::get<1>(ret), ghobject_t::get_max());
    EXPECT_EQ(std::get<0>(ret), oids);
  }

  // create temp objects
  struct bound_t {
    enum class type_t {
      MIN,
      MAX,
      TEMP,
      TEMP_END,
      NORMAL_BEGIN,
      NORMAL,
    } type = type_t::MIN;
    unsigned index = 0;

    static bound_t get_temp(unsigned index) {
      return bound_t{type_t::TEMP, index};
    }
    static bound_t get_normal(unsigned index) {
      return bound_t{type_t::NORMAL, index};
    }
    static bound_t get_min() { return bound_t{type_t::MIN}; }
    static bound_t get_max() { return bound_t{type_t::MAX}; }
    static bound_t get_temp_end() { return bound_t{type_t::TEMP_END}; }
    static bound_t get_normal_begin() {
      return bound_t{type_t::NORMAL_BEGIN};
    }

    ghobject_t get_oid(SeaStore &seastore, CollectionRef &coll) const {
      switch (type) {
      case type_t::MIN:
	return ghobject_t();
      case type_t::MAX:
	return ghobject_t::get_max();
      case type_t::TEMP:
	return make_temp_oid(index);
      case type_t::TEMP_END:
	return seastore.get_objs_range(coll, 0).temp_end;
      case type_t::NORMAL_BEGIN:
	return seastore.get_objs_range(coll, 0).obj_begin;
      case type_t::NORMAL:
	return make_oid(index);
      default:
	assert(0 == "impossible");
	return ghobject_t();
      }
    }
  };
  struct list_test_case_t {
    bound_t left;
    bound_t right;
    unsigned limit;
  };
  // list_test_cases_t :: [<limit, left_bound, right_bound>]
  using list_test_cases_t = std::list<std::tuple<unsigned, bound_t, bound_t>>;

  void test_list(
    unsigned temp_to_create,   /// create temp 0..temp_to_create-1
    unsigned normal_to_create, /// create normal 0..normal_to_create-1
    list_test_cases_t cases              /// cases to test
  ) {
    std::vector<ghobject_t> objs;

    // setup
    auto create = [this, &objs](ghobject_t hoid) {
      objs.emplace_back(std::move(hoid));
      auto &obj = get_object(objs.back());
      obj.touch(*sharded_seastore);
      obj.check_size(*sharded_seastore);
    };
    for (unsigned i = 0; i < temp_to_create; ++i) {
      create(make_temp_oid(i));
    }
    for (unsigned i = 0; i < normal_to_create; ++i) {
      create(make_oid(i));
    }

    // list and validate each case
    for (auto [limit, in_left_bound, in_right_bound] : cases) {
      auto left_bound = in_left_bound.get_oid(*seastore, coll);
      auto right_bound = in_right_bound.get_oid(*seastore, coll);

      // get results from seastore
      auto [listed, next] = sharded_seastore->list_objects(
	coll, left_bound, right_bound, limit).get0();

      // compute correct answer
      auto correct_begin = std::find_if(
	objs.begin(), objs.end(),
	[&left_bound](const auto &in) {
	  return in >= left_bound;
	});
      unsigned count = 0;
      auto correct_end = correct_begin;
      for (; count < limit &&
	     correct_end != objs.end() &&
	     *correct_end < right_bound;
	   ++correct_end, ++count);

      // validate return -- [correct_begin, correct_end) should match listed
      decltype(objs) correct_listed(correct_begin, correct_end);
      EXPECT_EQ(listed, correct_listed);

      if (count < limit) {
	if (correct_end == objs.end()) {
	  // if listed extends to end of range, next should be >= right_bound
	  EXPECT_GE(next, right_bound);
	} else {
	  // next <= *correct_end since *correct_end is the next object to list
	  EXPECT_LE(listed.back(), *correct_end);
	  // next > *(correct_end - 1) since we already listed it
	  EXPECT_GT(next, *(correct_end - 1));
	}
      } else {
	// we listed exactly limit objects
	EXPECT_EQ(limit, listed.size());

	EXPECT_GE(next, left_bound);
	if (limit == 0) {
	  if (correct_end != objs.end()) {
	    // next <= *correct_end since *correct_end is the next object to list
	    EXPECT_LE(next, *correct_end);
	  }
	} else {
	  // next > *(correct_end - 1) since we already listed it
	  EXPECT_GT(next, *(correct_end - 1));
	}
      }
    }

    // teardown
    for (auto &&hoid : objs) { get_object(hoid).remove(*sharded_seastore); }
  }
};

template <typename T, typename V>
auto contains(const T &t, const V &v) {
  return std::find(
    t.begin(),
    t.end(),
    v) != t.end();
}

TEST_P(seastore_test_t, collection_create_list_remove)
{
  run_async([this] {
    coll_t test_coll{spg_t{pg_t{1, 0}}};
    {
      sharded_seastore->create_new_collection(test_coll).get0();
      {
	CTransaction t;
	t.create_collection(test_coll, 4);
	do_transaction(std::move(t));
      }
      auto colls_cores = seastore->list_collections().get0();
      std::vector<coll_t> colls;
      colls.resize(colls_cores.size());
      std::transform(
        colls_cores.begin(), colls_cores.end(), colls.begin(),
        [](auto p) { return p.first; });
      EXPECT_EQ(colls.size(), 2);
      EXPECT_TRUE(contains(colls, coll_name));
      EXPECT_TRUE(contains(colls,  test_coll));
    }

    {
      {
	CTransaction t;
	t.remove_collection(test_coll);
	do_transaction(std::move(t));
      }
      auto colls_cores = seastore->list_collections().get0();
      std::vector<coll_t> colls;
      colls.resize(colls_cores.size());
      std::transform(
        colls_cores.begin(), colls_cores.end(), colls.begin(),
        [](auto p) { return p.first; });
      EXPECT_EQ(colls.size(), 1);
      EXPECT_TRUE(contains(colls, coll_name));
    }
  });
}

TEST_P(seastore_test_t, meta) {
  run_async([this] {
    set_meta("key1", "value1");
    set_meta("key2", "value2");

    const auto [ret1, value1] = get_meta("key1");
    const auto [ret2, value2] = get_meta("key2");
    EXPECT_EQ(ret1, 0);
    EXPECT_EQ(ret2, 0);
    EXPECT_EQ(value1, "value1");
    EXPECT_EQ(value2, "value2");
  });
}

TEST_P(seastore_test_t, touch_stat_list_remove)
{
  run_async([this] {
    auto &test_obj = get_object(make_oid(0));
    test_obj.touch(*sharded_seastore);
    test_obj.check_size(*sharded_seastore);
    validate_objects();

    remove_object(test_obj);
    validate_objects();
  });
}

using bound_t = seastore_test_t::bound_t;
constexpr unsigned MAX_LIMIT = std::numeric_limits<unsigned>::max();
static const seastore_test_t::list_test_cases_t temp_list_cases{
  // list all temp, maybe overlap to normal on right
  {MAX_LIMIT, bound_t::get_min()     , bound_t::get_max()     },
  {        5, bound_t::get_min()     , bound_t::get_temp_end()},
  {        6, bound_t::get_min()     , bound_t::get_temp_end()},
  {        6, bound_t::get_min()     , bound_t::get_max()     },

  // list temp starting at min up to but not past boundary
  {        3, bound_t::get_min()     , bound_t::get_temp(3)   },
  {        3, bound_t::get_min()     , bound_t::get_temp(4)   },
  {        3, bound_t::get_min()     , bound_t::get_temp(2)   },

  // list temp starting > min up to or past boundary
  {        3, bound_t::get_temp(2)   , bound_t::get_temp_end()},
  {        3, bound_t::get_temp(2)   , bound_t::get_max()     },
  {        3, bound_t::get_temp(3)   , bound_t::get_max()     },
  {        3, bound_t::get_temp(1)   , bound_t::get_max()     },

  // 0 limit
  {        0, bound_t::get_min()     , bound_t::get_max()     },
  {        0, bound_t::get_temp(1)   , bound_t::get_max()     },
  {        0, bound_t::get_temp_end(), bound_t::get_max()     },
};

TEST_P(seastore_test_t, list_objects_temp_only)
{
  run_async([this] { test_list(5, 0, temp_list_cases); });
}

TEST_P(seastore_test_t, list_objects_temp_overlap)
{
  run_async([this] { test_list(5, 5, temp_list_cases); });
}

static const seastore_test_t::list_test_cases_t normal_list_cases{
  // list all normal, maybe overlap to temp on left
  {MAX_LIMIT, bound_t::get_min()         , bound_t::get_max()    },
  {        5, bound_t::get_normal_begin(), bound_t::get_max()    },
  {        6, bound_t::get_normal_begin(), bound_t::get_max()    },
  {        6, bound_t::get_temp(4)       , bound_t::get_max()    },

  // list normal starting <= normal_begin < end
  {        3, bound_t::get_normal_begin(), bound_t::get_normal(3)},
  {        3, bound_t::get_normal_begin(), bound_t::get_normal(4)},
  {        3, bound_t::get_normal_begin(), bound_t::get_normal(2)},
  {        3, bound_t::get_temp(5)       , bound_t::get_normal(2)},
  {        3, bound_t::get_temp(4)       , bound_t::get_normal(2)},

  // list normal starting > min up to end
  {        3, bound_t::get_normal(2)     , bound_t::get_max()    },
  {        3, bound_t::get_normal(2)     , bound_t::get_max()    },
  {        3, bound_t::get_normal(3)     , bound_t::get_max()    },
  {        3, bound_t::get_normal(1)     , bound_t::get_max()    },

  // 0 limit
  {        0, bound_t::get_min()         , bound_t::get_max()    },
  {        0, bound_t::get_normal(1)     , bound_t::get_max()    },
  {        0, bound_t::get_normal_begin(), bound_t::get_max()    },
};

TEST_P(seastore_test_t, list_objects_normal_only)
{
  run_async([this] { test_list(5, 0, normal_list_cases); });
}

TEST_P(seastore_test_t, list_objects_normal_overlap)
{
  run_async([this] { test_list(5, 5, normal_list_cases); });
}

bufferlist make_bufferlist(size_t len) {
  bufferptr ptr(len);
  bufferlist bl;
  bl.append(ptr);
  return bl;
}

TEST_P(seastore_test_t, omap_test_simple)
{
  run_async([this] {
    auto &test_obj = get_object(make_oid(0));
    test_obj.touch(*sharded_seastore);
    test_obj.set_omap(
      *sharded_seastore,
      "asdf",
      make_bufferlist(128));
    test_obj.check_omap_key(
      *sharded_seastore,
      "asdf");
  });
}

TEST_P(seastore_test_t, rename)
{
  run_async([this] {
    auto &test_obj = get_object(make_oid(0));
    test_obj.write(*sharded_seastore, 0, 4096, 'a');
    test_obj.set_omap(
      *sharded_seastore,
      "asdf",
      make_bufferlist(128));
    auto test_other = object_state_t{
      test_obj.cid, 
      test_obj.coll,
      ghobject_t(hobject_t(sobject_t(std::string("object_1"), CEPH_NOSNAP)))};
    test_obj.rename(*sharded_seastore, test_other);
    test_other.read(*sharded_seastore, 0, 4096);
    test_other.check_omap(*sharded_seastore);
  });
}

TEST_P(seastore_test_t, clone_aligned_extents)
{
  run_async([this] {
    auto &test_obj = get_object(make_oid(0));
    test_obj.write(*sharded_seastore, 0, 4096, 'a');

    test_obj.clone(*sharded_seastore, 10);
    test_obj.read(*sharded_seastore, 0, 4096);
    test_obj.write(*sharded_seastore, 0, 4096, 'b');
    test_obj.write(*sharded_seastore, 4096, 4096, 'c');
    test_obj.read(*sharded_seastore, 0, 8192);
    auto clone_obj10 = test_obj.get_clone(10);
    clone_obj10.read(*sharded_seastore, 0, 8192);

    test_obj.clone(*sharded_seastore, 20);
    test_obj.read(*sharded_seastore, 0, 4096);
    test_obj.write(*sharded_seastore, 0, 4096, 'd');
    test_obj.write(*sharded_seastore, 4096, 4096, 'e');
    test_obj.write(*sharded_seastore, 8192, 4096, 'f');
    test_obj.read(*sharded_seastore, 0, 12288);
    auto clone_obj20 = test_obj.get_clone(20);
    clone_obj10.read(*sharded_seastore, 0, 12288);
    clone_obj20.read(*sharded_seastore, 0, 12288);
  });
}

TEST_P(seastore_test_t, clone_unaligned_extents)
{
  run_async([this] {
    auto &test_obj = get_object(make_oid(0));
    test_obj.write(*sharded_seastore, 0, 8192, 'a');
    test_obj.write(*sharded_seastore, 8192, 8192, 'b');
    test_obj.write(*sharded_seastore, 16384, 8192, 'c');

    test_obj.clone(*sharded_seastore, 10);
    test_obj.write(*sharded_seastore, 4096, 12288, 'd');
    test_obj.read(*sharded_seastore, 0, 24576);

    auto clone_obj10 = test_obj.get_clone(10);
    clone_obj10.read(*sharded_seastore, 0, 24576);

    test_obj.clone(*sharded_seastore, 20);
    test_obj.write(*sharded_seastore, 8192, 12288, 'e');
    test_obj.read(*sharded_seastore, 0, 24576);

    auto clone_obj20 = test_obj.get_clone(20);
    clone_obj10.read(*sharded_seastore, 0, 24576);
    clone_obj20.read(*sharded_seastore, 0, 24576);

    test_obj.write(*sharded_seastore, 0, 24576, 'f');
    test_obj.clone(*sharded_seastore, 30);
    test_obj.write(*sharded_seastore, 8192, 4096, 'g');
    test_obj.read(*sharded_seastore, 0, 24576);

    auto clone_obj30 = test_obj.get_clone(30);
    clone_obj10.read(*sharded_seastore, 0, 24576);
    clone_obj20.read(*sharded_seastore, 0, 24576);
    clone_obj30.read(*sharded_seastore, 0, 24576);
  });
}

TEST_P(seastore_test_t, attr)
{
  run_async([this] {
    auto& test_obj = get_object(make_oid(0));
    test_obj.touch(*sharded_seastore);
  {
    std::string oi("asdfasdfasdf");
    bufferlist bl;
    encode(oi, bl);
    test_obj.set_attr(*sharded_seastore, OI_ATTR, bl);

    std::string ss("fdsfdsfs");
    bl.clear();
    encode(ss, bl);
    test_obj.set_attr(*sharded_seastore, SS_ATTR, bl);

    std::string test_val("ssssssssssss");
    bl.clear();
    encode(test_val, bl);
    test_obj.set_attr(*sharded_seastore, "test_key", bl);

    auto attrs = test_obj.get_attrs(*sharded_seastore);
    std::string oi2;
    bufferlist bl2 = attrs[OI_ATTR];
    decode(oi2, bl2);
    bl2.clear();
    bl2 = attrs[SS_ATTR];
    std::string ss2;
    decode(ss2, bl2);
    std::string test_val2;
    bl2.clear();
    bl2 = attrs["test_key"];
    decode(test_val2, bl2);
    EXPECT_EQ(ss, ss2);
    EXPECT_EQ(oi, oi2);
    EXPECT_EQ(test_val, test_val2);

    bl2.clear();
    bl2 = test_obj.get_attr(*sharded_seastore, "test_key");
    test_val2.clear();
    decode(test_val2, bl2);
    EXPECT_EQ(test_val, test_val2);
    //test rm_attrs
    test_obj.rm_attrs(*sharded_seastore);
    attrs = test_obj.get_attrs(*sharded_seastore);
    EXPECT_EQ(attrs.find(OI_ATTR), attrs.end());
    EXPECT_EQ(attrs.find(SS_ATTR), attrs.end());
    EXPECT_EQ(attrs.find("test_key"), attrs.end());

    //create OI_ATTR with len > onode_layout_t::MAX_OI_LENGTH, rm OI_ATTR
    //create SS_ATTR with len > onode_layout_t::MAX_SS_LENGTH, rm SS_ATTR
    char oi_array[onode_layout_t::MAX_OI_LENGTH + 1] = {'a'};
    std::string oi_str(&oi_array[0], sizeof(oi_array));
    bl.clear();
    encode(oi_str, bl);
    test_obj.set_attr(*sharded_seastore, OI_ATTR, bl);

    char ss_array[onode_layout_t::MAX_SS_LENGTH + 1] = {'b'};
    std::string ss_str(&ss_array[0], sizeof(ss_array));
    bl.clear();
    encode(ss_str, bl);
    test_obj.set_attr(*sharded_seastore, SS_ATTR, bl);

    attrs = test_obj.get_attrs(*sharded_seastore);
    bl2.clear();
    bl2 = attrs[OI_ATTR];
    std::string oi_str2;
    decode(oi_str2, bl2);
    EXPECT_EQ(oi_str, oi_str2);

    bl2.clear();
    bl2 = attrs[SS_ATTR];
    std::string ss_str2;
    decode(ss_str2, bl2);
    EXPECT_EQ(ss_str, ss_str2);

    bl2.clear();
    ss_str2.clear();
    bl2 = test_obj.get_attr(*sharded_seastore, SS_ATTR);
    decode(ss_str2, bl2);
    EXPECT_EQ(ss_str, ss_str2);

    bl2.clear();
    oi_str2.clear();
    bl2 = test_obj.get_attr(*sharded_seastore, OI_ATTR);
    decode(oi_str2, bl2);
    EXPECT_EQ(oi_str, oi_str2);

    test_obj.rm_attr(*sharded_seastore, OI_ATTR);
    test_obj.rm_attr(*sharded_seastore, SS_ATTR);

    attrs = test_obj.get_attrs(*sharded_seastore);
    EXPECT_EQ(attrs.find(OI_ATTR), attrs.end());
    EXPECT_EQ(attrs.find(SS_ATTR), attrs.end());
  }
  {
    //create OI_ATTR with len <= onode_layout_t::MAX_OI_LENGTH, rm OI_ATTR
    //create SS_ATTR with len <= onode_layout_t::MAX_SS_LENGTH, rm SS_ATTR
    std::string oi("asdfasdfasdf");
    bufferlist bl;
    encode(oi, bl);
    test_obj.set_attr(*sharded_seastore, OI_ATTR, bl);

    std::string ss("f");
    bl.clear();
    encode(ss, bl);
    test_obj.set_attr(*sharded_seastore, SS_ATTR, bl);

    std::string test_val("ssssssssssss");
    bl.clear();
    encode(test_val, bl);
    test_obj.set_attr(*sharded_seastore, "test_key", bl);

    auto attrs = test_obj.get_attrs(*sharded_seastore);
    std::string oi2;
    bufferlist bl2 = attrs[OI_ATTR];
    decode(oi2, bl2);
    bl2.clear();
    bl2 = attrs[SS_ATTR];
    std::string ss2;
    decode(ss2, bl2);
    std::string test_val2;
    bl2.clear();
    bl2 = attrs["test_key"];
    decode(test_val2, bl2);
    EXPECT_EQ(ss, ss2);
    EXPECT_EQ(oi, oi2);
    EXPECT_EQ(test_val, test_val2);

    test_obj.rm_attr(*sharded_seastore, OI_ATTR);
    test_obj.rm_attr(*sharded_seastore, SS_ATTR);
    test_obj.rm_attr(*sharded_seastore, "test_key");

    attrs = test_obj.get_attrs(*sharded_seastore);
    EXPECT_EQ(attrs.find(OI_ATTR), attrs.end());
    EXPECT_EQ(attrs.find(SS_ATTR), attrs.end());
    EXPECT_EQ(attrs.find("test_key"), attrs.end());
  }
  {
    // create OI_ATTR with len > onode_layout_t::MAX_OI_LENGTH, then
    // overwrite it with another OI_ATTR len of which < onode_layout_t::MAX_OI_LENGTH
    // create SS_ATTR with len > onode_layout_t::MAX_SS_LENGTH, then
    // overwrite it with another SS_ATTR len of which < onode_layout_t::MAX_SS_LENGTH
    char oi_array[onode_layout_t::MAX_OI_LENGTH + 1] = {'a'};
    std::string oi(&oi_array[0], sizeof(oi_array));
    bufferlist bl;
    encode(oi, bl);
    test_obj.set_attr(*sharded_seastore, OI_ATTR, bl);

    oi = "asdfasdfasdf";
    bl.clear();
    encode(oi, bl);
    test_obj.set_attr(*sharded_seastore, OI_ATTR, bl);

    char ss_array[onode_layout_t::MAX_SS_LENGTH + 1] = {'b'};
    std::string ss(&ss_array[0], sizeof(ss_array));
    bl.clear();
    encode(ss, bl);
    test_obj.set_attr(*sharded_seastore, SS_ATTR, bl);

    ss = "f";
    bl.clear();
    encode(ss, bl);
    test_obj.set_attr(*sharded_seastore, SS_ATTR, bl);

    auto attrs = test_obj.get_attrs(*sharded_seastore);
    std::string oi2, ss2;
    bufferlist bl2 = attrs[OI_ATTR];
    decode(oi2, bl2);
    bl2.clear();
    bl2 = attrs[SS_ATTR];
    decode(ss2, bl2);
    EXPECT_EQ(oi, oi2);
    EXPECT_EQ(ss, ss2);
  }
  });
}

TEST_P(seastore_test_t, omap_test_iterator)
{
  run_async([this] {
    auto make_key = [](unsigned i) {
      std::stringstream ss;
      ss << "key" << i;
      return ss.str();
    };
    auto &test_obj = get_object(make_oid(0));
    test_obj.touch(*sharded_seastore);
    for (unsigned i = 0; i < 20; ++i) {
      test_obj.set_omap(
	*sharded_seastore,
	make_key(i),
	make_bufferlist(128));
    }
    test_obj.check_omap(*sharded_seastore);
  });
}

TEST_P(seastore_test_t, object_data_omap_remove)
{
  run_async([this] {
    auto make_key = [](unsigned i) {
      std::stringstream ss;
      ss << "key" << i;
      return ss.str();
    };
    auto &test_obj = get_object(make_oid(0));
    test_obj.touch(*sharded_seastore);
    for (unsigned i = 0; i < 1024; ++i) {
      test_obj.set_omap(
	*sharded_seastore,
	make_key(i),
	make_bufferlist(128));
    }
    test_obj.check_omap(*sharded_seastore);

    for (uint64_t i = 0; i < 16; i++) {
      test_obj.write(
	*sharded_seastore,
	4096 * i,
	4096,
	'a');
    }
    test_obj.remove(*sharded_seastore);
  });
}


TEST_P(seastore_test_t, simple_extent_test)
{
  run_async([this] {
    auto &test_obj = get_object(make_oid(0));
    test_obj.write(
      *sharded_seastore,
      1024,
      1024,
      'a');
    test_obj.read(
      *sharded_seastore,
      1024,
      1024);
    test_obj.check_size(*sharded_seastore);
  });
}

TEST_P(seastore_test_t, fiemap_empty)
{
  run_async([this] {
    auto &test_obj = get_object(make_oid(0));
    test_obj.touch(*sharded_seastore);
    test_obj.truncate(*sharded_seastore, 100000);

    std::map<uint64_t, uint64_t> m;
    m = test_obj.fiemap(*sharded_seastore, 0, 100000);
    EXPECT_TRUE(m.empty());

    test_obj.remove(*sharded_seastore);
  });
}

TEST_P(seastore_test_t, fiemap_holes)
{
  run_async([this] {
    const uint64_t MAX_EXTENTS = 100;

    // large enough to ensure that seastore will allocate each write seperately
    const uint64_t SKIP_STEP = 16 << 10;
    auto &test_obj = get_object(make_oid(0));
    bufferlist bl;
    bl.append("foo");

    test_obj.touch(*sharded_seastore);
    for (uint64_t i = 0; i < MAX_EXTENTS; i++) {
      test_obj.write(*sharded_seastore, SKIP_STEP * i, bl);
    }

    { // fiemap test from 0 to SKIP_STEP * (MAX_EXTENTS - 1) + 3
      auto m = test_obj.fiemap(
	*sharded_seastore, 0, SKIP_STEP * (MAX_EXTENTS - 1) + 3);
      ASSERT_EQ(m.size(), MAX_EXTENTS);
      for (uint64_t i = 0; i < MAX_EXTENTS; i++) {
	ASSERT_TRUE(m.count(SKIP_STEP * i));
	ASSERT_GE(m[SKIP_STEP * i], bl.length());
      }
    }

    { // fiemap test from SKIP_STEP to SKIP_STEP * (MAX_EXTENTS - 2) + 3
      auto m = test_obj.fiemap(
	*sharded_seastore, SKIP_STEP, SKIP_STEP * (MAX_EXTENTS - 3) + 3);
      ASSERT_EQ(m.size(), MAX_EXTENTS - 2);
      for (uint64_t i = 1; i < MAX_EXTENTS - 1; i++) {
	ASSERT_TRUE(m.count(SKIP_STEP * i));
	ASSERT_GE(m[SKIP_STEP * i], bl.length());
      }
    }

    { // fiemap test SKIP_STEP + 1 to 2 * SKIP_STEP + 1 (partial overlap)
      auto m = test_obj.fiemap(
	*sharded_seastore, SKIP_STEP + 1, SKIP_STEP + 1);
      ASSERT_EQ(m.size(), 2);
      ASSERT_EQ(m.begin()->first, SKIP_STEP + 1);
      ASSERT_GE(m.begin()->second, bl.length());
      ASSERT_LE(m.rbegin()->first, (2 * SKIP_STEP) + 1);
      ASSERT_EQ(m.rbegin()->first + m.rbegin()->second, 2 * SKIP_STEP + 2);
    }

    test_obj.remove(*sharded_seastore);
  });
}

TEST_P(seastore_test_t, sparse_read)
{
  run_async([this] {
    const uint64_t MAX_EXTENTS = 100;
    const uint64_t SKIP_STEP = 16 << 10;
    auto &test_obj = get_object(make_oid(0));
    bufferlist wbl;
    wbl.append("foo");

    test_obj.touch(*sharded_seastore);
    for (uint64_t i = 0; i < MAX_EXTENTS; i++) {
      test_obj.write(*sharded_seastore, SKIP_STEP * i, wbl);
    }
    interval_set<uint64_t> m;
    m = interval_set<uint64_t>(
	test_obj.fiemap(*sharded_seastore, 0, SKIP_STEP * (MAX_EXTENTS - 1) + 3));
    ASSERT_TRUE(!m.empty());
    uint64_t off = 0;
    auto rbl = test_obj.readv(*sharded_seastore, m);

    for (auto &&miter : m) {
      bufferlist subl;
      subl.substr_of(rbl, off, std::min(miter.second, uint64_t(wbl.length())));
      ASSERT_TRUE(subl.contents_equal(wbl));
      off += miter.second;
    }
    test_obj.remove(*sharded_seastore);
  });
}

TEST_P(seastore_test_t, zero)
{
  run_async([this] {
    auto test_zero = [this](
      // [(off, len, repeat)]
      std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> writes,
      uint64_t zero_off, uint64_t zero_len) {

      // Test zero within a block
      auto &test_obj = get_object(make_oid(0));
      uint64_t size = 0;
      for (auto &[off, len, repeat]: writes) {
	for (decltype(repeat) i = 0; i < repeat; ++i) {
	  test_obj.write(*sharded_seastore, off + (len * repeat), len, 'a');
	}
	size = off + (len * (repeat + 1));
      }
      test_obj.read(
	*sharded_seastore,
	0,
	size);
      test_obj.check_size(*sharded_seastore);
      test_obj.zero(*sharded_seastore, zero_off, zero_len);
      test_obj.read(
	*sharded_seastore,
	0,
	size);
      test_obj.check_size(*sharded_seastore);
      remove_object(test_obj);
    };

    const uint64_t BS = 4<<10;

    // Test zero within a block
    test_zero(
      {{1<<10, 1<<10, 1}},
      1124, 200);

    // Multiple writes, partial on left, partial on right.
    test_zero(
      {{BS, BS, 10}},
      BS + 128,
      BS * 4);

    // Single large write, block boundary on right, partial on left.
    test_zero(
      {{BS, BS * 10, 1}},
      BS + 128,
      (BS * 4) - 128);

    // Multiple writes, block boundary on left, partial on right.
    test_zero(
      {{BS, BS, 10}},
      BS,
      (BS * 4) + 128);
  });
}
INSTANTIATE_TEST_SUITE_P(
  seastore_test,
  seastore_test_t,
  ::testing::Values (
    "segmented",
    "circularbounded"
  )
);
