// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <iterator>
#include <map>
#include <set>
#include <boost/scoped_ptr.hpp>
#include <sys/types.h>
#include <cstdlib>

#include "include/buffer.h"
#include "common/map_cacher.hpp"
#include "osd/osd_types_fmt.h"
#include "osd/SnapMapper.h"
#include "common/Cond.h"

#include "gtest/gtest.h"

using namespace std;

template <typename T>
typename T::iterator rand_choose(T &cont) {
  if (std::empty(cont)) {
    return std::end(cont);
  }
  return std::next(std::begin(cont), rand() % cont.size());
}

string random_string(size_t size)
{
  string name;
  for (size_t j = 0; j < size; ++j) {
    name.push_back('a' + (rand() % 26));
  }
  return name;
}

class PausyAsyncMap : public MapCacher::StoreDriver<string, bufferlist> {
  struct _Op {
    virtual void operate(map<string, bufferlist> *store) = 0;
    virtual ~_Op() {}
  };
  typedef std::shared_ptr<_Op> Op;
  struct Remove : public _Op {
    set<string> to_remove;
    explicit Remove(const set<string> &to_remove) : to_remove(to_remove) {}
    void operate(map<string, bufferlist> *store) override {
      for (set<string>::iterator i = to_remove.begin();
	   i != to_remove.end();
	   ++i) {
	store->erase(*i);
      }
    }
  };
  struct Insert : public _Op {
    map<string, bufferlist> to_insert;
    explicit Insert(const map<string, bufferlist> &to_insert) : to_insert(to_insert) {}
    void operate(map<string, bufferlist> *store) override {
      for (map<string, bufferlist>::iterator i = to_insert.begin();
	   i != to_insert.end();
	   ++i) {
	store->erase(i->first);
	store->insert(*i);
      }
    }
  };
  struct Callback : public _Op {
    Context *context;
    explicit Callback(Context *c) : context(c) {}
    void operate(map<string, bufferlist> *store) override {
      context->complete(0);
    }
  };
public:
  class Transaction : public MapCacher::Transaction<string, bufferlist> {
    friend class PausyAsyncMap;
    list<Op> ops;
    list<Op> callbacks;
  public:
    void set_keys(const map<string, bufferlist> &i) override {
      ops.push_back(Op(new Insert(i)));
    }
    void remove_keys(const set<string> &r) override {
      ops.push_back(Op(new Remove(r)));
    }
    void add_callback(Context *c) override {
      callbacks.push_back(Op(new Callback(c)));
    }
  };
private:

  ceph::mutex lock = ceph::make_mutex("PausyAsyncMap");
  map<string, bufferlist> store;

  class Doer : public Thread {
    static const size_t MAX_SIZE = 100;
    PausyAsyncMap *parent;
    ceph::mutex lock = ceph::make_mutex("Doer lock");
    ceph::condition_variable cond;
    int stopping;
    bool paused;
    list<Op> queue;
  public:
    explicit Doer(PausyAsyncMap *parent) :
      parent(parent), stopping(0), paused(false) {}
    void *entry() override {
      while (1) {
	list<Op> ops;
	{
	  std::unique_lock l{lock};
	  cond.wait(l, [this] {
            return stopping || (!queue.empty() && !paused);
	  });
	  if (stopping && queue.empty()) {
	    stopping = 2;
	    cond.notify_all();
	    return 0;
	  }
	  ceph_assert(!queue.empty());
	  ceph_assert(!paused);
	  ops.swap(queue);
	  cond.notify_all();
	}
	ceph_assert(!ops.empty());

	for (list<Op>::iterator i = ops.begin();
	     i != ops.end();
	     ops.erase(i++)) {
	  if (!(rand()%3))
	    usleep(1+(rand() % 5000));
	  std::lock_guard l{parent->lock};
	  (*i)->operate(&(parent->store));
	}
      }
    }

    void pause() {
      std::lock_guard l{lock};
      paused = true;
      cond.notify_all();
    }

    void resume() {
      std::lock_guard l{lock};
      paused = false;
      cond.notify_all();
    }

    void submit(list<Op> &in) {
      std::unique_lock l{lock};
      cond.wait(l, [this] { return queue.size() < MAX_SIZE;});
      queue.splice(queue.end(), in, in.begin(), in.end());
      cond.notify_all();
    }

    void stop() {
      std::unique_lock l{lock};
      stopping = 1;
      cond.notify_all();
      cond.wait(l, [this] { return stopping == 2; });
      cond.notify_all();
    }
  } doer;

public:
  PausyAsyncMap() : doer(this) {
    doer.create("doer");
  }
  ~PausyAsyncMap() override {
    doer.join();
  }
  int get_keys(
    const set<string> &keys,
    map<string, bufferlist> *out) override {
    std::lock_guard l{lock};
    for (set<string>::const_iterator i = keys.begin();
	 i != keys.end();
	 ++i) {
      map<string, bufferlist>::iterator j = store.find(*i);
      if (j != store.end())
	out->insert(*j);
    }
    return 0;
  }
  int get_next(
    const string &key,
    pair<string, bufferlist> *next) override {
    std::lock_guard l{lock};
    map<string, bufferlist>::iterator j = store.upper_bound(key);
    if (j != store.end()) {
      if (next)
	*next = *j;
      return 0;
    } else {
      return -ENOENT;
    }
  }
  void submit(Transaction *t) {
    doer.submit(t->ops);
    doer.submit(t->callbacks);
  }

  void flush() {
    ceph::mutex lock = ceph::make_mutex("flush lock");
    ceph::condition_variable cond;
    bool done = false;

    class OnFinish : public Context {
      ceph::mutex *lock;
      ceph::condition_variable *cond;
      bool *done;
    public:
      OnFinish(ceph::mutex *lock, ceph::condition_variable *cond, bool *done)
	: lock(lock), cond(cond), done(done) {}
      void finish(int) override {
	std::lock_guard l{*lock};
	*done = true;
	cond->notify_all();
      }
    };
    Transaction t;
    t.add_callback(new OnFinish(&lock, &cond, &done));
    submit(&t);
    {
      std::unique_lock l{lock};
      cond.wait(l, [&] { return done; });
    }
  }

  void pause() {
    doer.pause();
  }
  void resume() {
    doer.resume();
  }
  void stop() {
    doer.stop();
  }

};

class MapCacherTest : public ::testing::Test {
protected:
  boost::scoped_ptr< PausyAsyncMap > driver;
  boost::scoped_ptr<MapCacher::MapCacher<string, bufferlist> > cache;
  map<string, bufferlist> truth;
  set<string> names;
public:
  void assert_bl_eq(bufferlist &bl1, bufferlist &bl2) {
    ASSERT_EQ(bl1.length(), bl2.length());
    bufferlist::iterator j = bl2.begin();
    for (bufferlist::iterator i = bl1.begin();
	 !i.end();
	 ++i, ++j) {
      ASSERT_TRUE(!j.end());
      ASSERT_EQ(*i, *j);
    }
  }
  void assert_bl_map_eq(map<string, bufferlist> &m1,
			map<string, bufferlist> &m2) {
    ASSERT_EQ(m1.size(), m2.size());
    map<string, bufferlist>::iterator j = m2.begin();
    for (map<string, bufferlist>::iterator i = m1.begin();
	 i != m1.end();
	 ++i, ++j) {
      ASSERT_TRUE(j != m2.end());
      ASSERT_EQ(i->first, j->first);
      assert_bl_eq(i->second, j->second);
    }

  }
  size_t random_num() {
    return random() % 10;
  }
  size_t random_size() {
    return random() % 1000;
  }
  void random_bl(size_t size, bufferlist *bl) {
    for (size_t i = 0; i < size; ++i) {
      bl->append(rand());
    }
  }
  void do_set() {
    size_t set_size = random_num();
    map<string, bufferlist> to_set;
    for (size_t i = 0; i < set_size; ++i) {
      bufferlist bl;
      random_bl(random_size(), &bl);
      string key = *rand_choose(names);
      to_set.insert(
	make_pair(key, bl));
    }
    for (map<string, bufferlist>::iterator i = to_set.begin();
	 i != to_set.end();
	 ++i) {
      truth.erase(i->first);
      truth.insert(*i);
    }
    {
      PausyAsyncMap::Transaction t;
      cache->set_keys(to_set, &t);
      driver->submit(&t);
    }
  }
  void remove() {
    size_t remove_size = random_num();
    set<string> to_remove;
    for (size_t i = 0; i < remove_size ; ++i) {
      to_remove.insert(*rand_choose(names));
    }
    for (set<string>::iterator i = to_remove.begin();
	 i != to_remove.end();
	 ++i) {
      truth.erase(*i);
    }
    {
      PausyAsyncMap::Transaction t;
      cache->remove_keys(to_remove, &t);
      driver->submit(&t);
    }
  }
  void get() {
    set<string> to_get;
    size_t get_size = random_num();
    for (size_t i = 0; i < get_size; ++i) {
      to_get.insert(*rand_choose(names));
    }

    map<string, bufferlist> got_truth;
    for (set<string>::iterator i = to_get.begin();
	 i != to_get.end();
	 ++i) {
      map<string, bufferlist>::iterator j = truth.find(*i);
      if (j != truth.end())
	got_truth.insert(*j);
    }

    map<string, bufferlist> got;
    cache->get_keys(to_get, &got);

    assert_bl_map_eq(got, got_truth);
  }

  void get_next() {
    string cur;
    while (true) {
      pair<string, bufferlist> next;
      int r = cache->get_next(cur, &next);

      pair<string, bufferlist> next_truth;
      map<string, bufferlist>::iterator i = truth.upper_bound(cur);
      int r_truth = (i == truth.end()) ? -ENOENT : 0;
      if (i != truth.end())
	next_truth = *i;

      ASSERT_EQ(r, r_truth);
      if (r == -ENOENT)
	break;

      ASSERT_EQ(next.first, next_truth.first);
      assert_bl_eq(next.second, next_truth.second);
      cur = next.first;
    }
  }
  void SetUp() override {
    driver.reset(new PausyAsyncMap());
    cache.reset(new MapCacher::MapCacher<string, bufferlist>(driver.get()));
    names.clear();
    truth.clear();
    size_t names_size(random_num() + 10);
    for (size_t i = 0; i < names_size; ++i) {
      names.insert(random_string(1 + (random_size() % 10)));
    }
  }
  void TearDown() override {
    driver->stop();
    cache.reset();
    driver.reset();
  }

};

TEST_F(MapCacherTest, Simple)
{
  driver->pause();
  map<string, bufferlist> truth;
  set<string> truth_keys;
  string blah("asdf");
  bufferlist bl;
  encode(blah, bl);
  truth[string("asdf")] = bl;
  truth_keys.insert(truth.begin()->first);
  {
    PausyAsyncMap::Transaction t;
    cache->set_keys(truth, &t);
    driver->submit(&t);
    cache->set_keys(truth, &t);
    driver->submit(&t);
  }

  map<string, bufferlist> got;
  cache->get_keys(truth_keys, &got);
  assert_bl_map_eq(got, truth);

  driver->resume();
  sleep(1);

  got.clear();
  cache->get_keys(truth_keys, &got);
  assert_bl_map_eq(got, truth);
}

TEST_F(MapCacherTest, Random)
{
  for (size_t i = 0; i < 5000; ++i) {
    if (!(i % 50)) {
      std::cout << "On iteration " << i << std::endl;
    }
    switch (rand() % 4) {
    case 0:
      get();
      break;
    case 1:
      do_set();
      break;
    case 2:
      get_next();
      break;
    case 3:
      remove();
      break;
    }
  }
}

class MapperVerifier {
  PausyAsyncMap *driver;
  boost::scoped_ptr< SnapMapper > mapper;
  map<snapid_t, set<hobject_t> > snap_to_hobject;
  map<hobject_t, set<snapid_t>> hobject_to_snap;
  snapid_t next;
  uint32_t mask;
  uint32_t bits;
  ceph::mutex lock = ceph::make_mutex("lock");
public:

  MapperVerifier(
    PausyAsyncMap *driver,
    uint32_t mask,
    uint32_t bits)
    : driver(driver),
      mapper(new SnapMapper(g_ceph_context, driver, mask, bits, 0, shard_id_t(1))),
             mask(mask), bits(bits) {}

  hobject_t random_hobject() {
    return hobject_t(
      random_string(1+(rand() % 16)),
      random_string(1+(rand() % 16)),
      snapid_t(rand() % 1000),
      (rand() & ((~0)<<bits)) | (mask & ~((~0)<<bits)),
      0, random_string(rand() % 16));
  }

  void choose_random_snaps(int num, set<snapid_t> *snaps) {
    ceph_assert(snaps);
    ceph_assert(!snap_to_hobject.empty());
    for (int i = 0; i < num || snaps->empty(); ++i) {
      snaps->insert(rand_choose(snap_to_hobject)->first);
    }
  }

  void create_snap() {
    snap_to_hobject[next];
    ++next;
  }

  void create_object() {
    std::lock_guard l{lock};
    if (snap_to_hobject.empty())
      return;
    hobject_t obj;
    do {
      obj = random_hobject();
    } while (hobject_to_snap.count(obj));

    set<snapid_t> &snaps = hobject_to_snap[obj];
    choose_random_snaps(1 + (rand() % 20), &snaps);
    for (set<snapid_t>::iterator i = snaps.begin();
	 i != snaps.end();
	 ++i) {
      map<snapid_t, set<hobject_t> >::iterator j = snap_to_hobject.find(*i);
      ceph_assert(j != snap_to_hobject.end());
      j->second.insert(obj);
    }
    {
      PausyAsyncMap::Transaction t;
      mapper->add_oid(obj, snaps, &t);
      driver->submit(&t);
    }
  }

  std::pair<std::string, ceph::buffer::list> to_raw(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw(to_map);
  }

  std::string to_legacy_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_legacy_raw_key(to_map);
  }

  template <typename... Args>
  std::string to_object_key(Args&&... args) {
    return mapper->to_object_key(std::forward<Args>(args)...);
  }

  std::string to_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw_key(to_map);
  }

  template <typename... Args>
  std::string make_purged_snap_key(Args&&... args) {
    return mapper->make_purged_snap_key(std::forward<Args>(args)...);
  }

  void trim_snap() {
    std::lock_guard l{lock};
    if (snap_to_hobject.empty())
      return;
    map<snapid_t, set<hobject_t> >::iterator snap =
      rand_choose(snap_to_hobject);
    set<hobject_t> hobjects = snap->second;

    vector<hobject_t> hoids;
    while (mapper->get_next_objects_to_trim(
	     snap->first, rand() % 5 + 1, &hoids) == 0) {
      for (auto &&hoid: hoids) {
	ceph_assert(!hoid.is_max());
	ceph_assert(hobjects.count(hoid));
	hobjects.erase(hoid);

	map<hobject_t, set<snapid_t>>::iterator j =
	  hobject_to_snap.find(hoid);
	ceph_assert(j->second.count(snap->first));
	set<snapid_t> old_snaps(j->second);
	j->second.erase(snap->first);

	{
	  PausyAsyncMap::Transaction t;
	  mapper->update_snaps(
	    hoid,
	    j->second,
	    &old_snaps,
	    &t);
	  driver->submit(&t);
	}
	if (j->second.empty()) {
	  hobject_to_snap.erase(j);
	}
	hoid = hobject_t::get_max();
      }
      hoids.clear();
    }
    ceph_assert(hobjects.empty());
    snap_to_hobject.erase(snap);
  }

  void remove_oid() {
    std::lock_guard l{lock};
    if (hobject_to_snap.empty())
      return;
    map<hobject_t, set<snapid_t>>::iterator obj =
      rand_choose(hobject_to_snap);
    for (set<snapid_t>::iterator i = obj->second.begin();
	 i != obj->second.end();
	 ++i) {
      map<snapid_t, set<hobject_t> >::iterator j =
	snap_to_hobject.find(*i);
      ceph_assert(j->second.count(obj->first));
      j->second.erase(obj->first);
    }
    {
      PausyAsyncMap::Transaction t;
      mapper->remove_oid(
	obj->first,
	&t);
      driver->submit(&t);
    }
    hobject_to_snap.erase(obj);
  }

  void check_oid() {
    std::lock_guard l{lock};
    if (hobject_to_snap.empty())
      return;
    map<hobject_t, set<snapid_t>>::iterator obj =
      rand_choose(hobject_to_snap);
    set<snapid_t> snaps;
    int r = mapper->get_snaps(obj->first, &snaps);
    ceph_assert(r == 0);
    ASSERT_EQ(snaps, obj->second);
  }
};

class SnapMapperTest : public ::testing::Test {
protected:
  boost::scoped_ptr< PausyAsyncMap > driver;
  map<pg_t, std::shared_ptr<MapperVerifier> > mappers;
  uint32_t pgnum;

  void SetUp() override {
    driver.reset(new PausyAsyncMap());
    pgnum = 0;
  }

  void TearDown() override {
    driver->stop();
    mappers.clear();
    driver.reset();
  }

  MapperVerifier &get_tester() {
    //return *(mappers.begin()->second);
    return *(rand_choose(mappers)->second);
  }

  void init(uint32_t to_set) {
    pgnum = to_set;
    for (uint32_t i = 0; i < pgnum; ++i) {
      pg_t pgid(i, 0);
      mappers[pgid].reset(
	new MapperVerifier(
	  driver.get(),
	  i,
	  pgid.get_split_bits(pgnum)
	  )
	);
    }
  }

  void run() {
    for (int i = 0; i < 5000; ++i) {
      if (!(i % 50))
	std::cout << i << std::endl;
      switch (rand() % 5) {
      case 0:
	get_tester().create_snap();
	break;
      case 1:
	get_tester().create_object();
	break;
      case 2:
	get_tester().trim_snap();
	break;
      case 3:
	get_tester().check_oid();
	break;
      case 4:
	get_tester().remove_oid();
	break;
      }
    }
  }
};

TEST_F(SnapMapperTest, Simple) {
  init(1);
  get_tester().create_snap();
  get_tester().create_object();
  get_tester().trim_snap();
}

TEST_F(SnapMapperTest, More) {
  init(1);
  run();
}

TEST_F(SnapMapperTest, MultiPG) {
  init(50);
  run();
}

// Check to_object_key against current format to detect accidental changes in encoding
TEST_F(SnapMapperTest, CheckObjectKeyFormat) {
  init(1);
  // <object, test_raw_key>
  std::vector<std::tuple<hobject_t, std::string>> object_to_object_key({
      {hobject_t{"test_object", "", 20, 0x01234567, 20, ""},
	  "OBJ_.1_0000000000000014.76543210.14.test%uobject.."},
      {hobject_t{"test._ob.ject", "k.ey", 20, 0x01234567, 20, ""},
	  "OBJ_.1_0000000000000014.76543210.14.test%e%uob%eject.k%eey."},
      {hobject_t{"test_object", "", 20, 0x01234567, 20, "namespace"},
	  "OBJ_.1_0000000000000014.76543210.14.test%uobject..namespace"},
      {hobject_t{
	  "test_object", "", std::numeric_limits<snapid_t>::max() - 20, 0x01234567,
	    std::numeric_limits<int64_t>::max() - 20, "namespace"},
	  "OBJ_.1_7FFFFFFFFFFFFFEB.76543210.ffffffffffffffec.test%uobject..namespace"}
    });

  for (auto &[object, test_object_key]: object_to_object_key) {
    auto object_key = get_tester().to_object_key(object);
    if (object_key != test_object_key) {
      std::cout << object << " should be "
	        << test_object_key << " is "
	        << get_tester().to_object_key(object)
	        << std::endl;
    }
    ASSERT_EQ(object_key, test_object_key);
  }
}


// Check to_raw_key against current format to detect accidental changes in encoding
TEST_F(SnapMapperTest, CheckRawKeyFormat) {
  init(1);
  // <object, snapid, test_raw_key>
  std::vector<std::tuple<hobject_t, snapid_t, std::string>> object_to_raw_key({
      {hobject_t{"test_object", "", 20, 0x01234567, 20, ""}, 25,
	  "SNA_20_0000000000000019_.1_0000000000000014.76543210.14.test%uobject.."},
      {hobject_t{"test._ob.ject", "k.ey", 20, 0x01234567, 20, ""}, 25,
	  "SNA_20_0000000000000019_.1_0000000000000014.76543210.14.test%e%uob%eject.k%eey."},
      {hobject_t{"test_object", "", 20, 0x01234567, 20, "namespace"}, 25,
	  "SNA_20_0000000000000019_.1_0000000000000014.76543210.14.test%uobject..namespace"},
      {hobject_t{
	  "test_object", "", std::numeric_limits<snapid_t>::max() - 20, 0x01234567,
	    std::numeric_limits<int64_t>::max() - 20, "namespace"}, std::numeric_limits<snapid_t>::max() - 20,
	  "SNA_9223372036854775787_FFFFFFFFFFFFFFEC_.1_7FFFFFFFFFFFFFEB.76543210.ffffffffffffffec.test%uobject..namespace"}
    });

  for (auto &[object, snap, test_raw_key]: object_to_raw_key) {
    auto raw_key = get_tester().to_raw_key(std::make_pair(snap, object));
    if (raw_key != test_raw_key) {
      std::cout << object << " " << snap << " should be "
	        << test_raw_key << " is "
	        << get_tester().to_raw_key(std::make_pair(snap, object))
	        << std::endl;
    }
    ASSERT_EQ(raw_key, test_raw_key);
  }
}

// Check make_purged_snap_key against current format to detect accidental changes
// in encoding
TEST_F(SnapMapperTest, CheckMakePurgedSnapKeyFormat) {
  init(1);
  // <pool, snap, test_key>
  std::vector<std::tuple<int64_t, snapid_t, std::string>> purged_snap_to_key({
      {20, 30, "PSN__20_000000000000001e"},
      {std::numeric_limits<int64_t>::max() - 20,
       std::numeric_limits<snapid_t>::max() - 20,
       "PSN__9223372036854775787_ffffffffffffffec"}
  });

  for (auto &[pool, snap, test_key]: purged_snap_to_key) {
    auto raw_purged_snap_key = get_tester().make_purged_snap_key(pool, snap);
    if (raw_purged_snap_key != test_key) {
      std::cout << "<" << pool << ", " << snap << "> should be " << test_key
                << " is " << raw_purged_snap_key << std::endl;
    }
    // retesting (mostly for test numbers accounting)
    ASSERT_EQ(raw_purged_snap_key, test_key);
  }
}

TEST_F(SnapMapperTest, LegacyKeyConvertion) {
    init(1);
    auto obj = get_tester().random_hobject();
    snapid_t snapid = random() % 10;
    auto snap_obj = make_pair(snapid, obj);
    auto raw = get_tester().to_raw(snap_obj);
    std::string old_key = get_tester().to_legacy_raw_key(snap_obj);
    std::string converted_key =
      SnapMapper::convert_legacy_key(old_key, raw.second);
    std::string new_key = get_tester().to_raw_key(snap_obj);
    if (converted_key != new_key) {
      std::cout << "Converted: " << old_key << "\nTo:        " << converted_key
	        << "\nNew key:   " << new_key << std::endl;
    }
    ASSERT_EQ(converted_key, new_key);
}

/**
 * 'DirectMapper' provides simple, controlled, interface to the underlying
 * SnapMapper.
 */
class DirectMapper {
public:
  std::unique_ptr<PausyAsyncMap> driver{make_unique<PausyAsyncMap>()};
  std::unique_ptr<SnapMapper> mapper;
  uint32_t mask;
  uint32_t bits;
  ceph::mutex lock = ceph::make_mutex("lock");

  DirectMapper(
    uint32_t mask,
    uint32_t bits)
   : mapper(new SnapMapper(g_ceph_context, driver.get(), mask, bits, 0, shard_id_t(1))), 
             mask(mask), bits(bits) {}

  hobject_t random_hobject() {
    return hobject_t(
      random_string(1+(rand() % 16)),
      random_string(1+(rand() % 16)),
      snapid_t(rand() % 1000),
      (rand() & ((~0)<<bits)) | (mask & ~((~0)<<bits)),
      0, random_string(rand() % 16));
  }

  void create_object(const hobject_t& obj, const set<snapid_t> &snaps) {
    std::lock_guard l{lock};
      PausyAsyncMap::Transaction t;
      mapper->add_oid(obj, snaps, &t);
      driver->submit(&t);
  }

  std::pair<std::string, ceph::buffer::list> to_raw(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw(to_map);
  }

  std::string to_legacy_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_legacy_raw_key(to_map);
  }

  std::string to_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw_key(to_map);
  }

  void shorten_mapping_key(snapid_t snap, const hobject_t &clone)
  {
    // calculate the relevant key
    std::string k = mapper->to_raw_key(snap, clone);

    // find the value for this key
    map<string, bufferlist> kvmap;
    auto r = mapper->backend.get_keys(set{k}, &kvmap);
    ASSERT_GE(r, 0);

    // replace the key with its shortened version
    PausyAsyncMap::Transaction t;
    mapper->backend.remove_keys(set{k}, &t);
    auto short_k = k.substr(0, 10);
    mapper->backend.set_keys(map<string, bufferlist>{{short_k, kvmap[k]}}, &t);
    driver->submit(&t);
    driver->flush();
  }
};

class DirectMapperTest : public ::testing::Test {
 public:
  // ctor & initialization
  DirectMapperTest() = default;
  ~DirectMapperTest() = default;
  void SetUp() override;
  void TearDown() override;

 protected:
  std::unique_ptr<DirectMapper> direct;
};

void DirectMapperTest::SetUp()
{
  direct = std::make_unique<DirectMapper>(0, 0);
}

void DirectMapperTest::TearDown()
{
  direct->driver->stop();
  direct->mapper.reset();
  direct->driver.reset();
}


TEST_F(DirectMapperTest, BasciObject)
{
  auto obj = direct->random_hobject();
  set<snapid_t> snaps{100, 200};
  direct->create_object(obj, snaps);

  // verify that the OBJ_ & SNA_ entries are there
  auto osn1 = direct->mapper->get_snaps(obj);
  ASSERT_EQ(snaps, osn1);
  auto vsn1 = direct->mapper->get_snaps_check_consistency(obj);
  ASSERT_EQ(snaps, vsn1);
}

TEST_F(DirectMapperTest, CorruptedSnaRecord)
{
  object_t base_name{"obj"};
  std::string key{"key"};

  hobject_t head{base_name, key, CEPH_NOSNAP, 0x17, 0, ""};
  hobject_t cln1{base_name, key, 10, 0x17, 0, ""};
  hobject_t cln2{base_name, key, 20, 0x17, 0, ""}; // the oldest version
  set<snapid_t> head_snaps{400, 500};
  set<snapid_t> cln1_snaps{300};
  set<snapid_t> cln2_snaps{100, 200};

  PausyAsyncMap::Transaction t;
  direct->mapper->add_oid(head, head_snaps, &t);
  direct->mapper->add_oid(cln1, cln1_snaps, &t);
  direct->mapper->add_oid(cln2, cln2_snaps, &t);
  direct->driver->submit(&t);
  direct->driver->flush();

  // verify that the OBJ_ & SNA_ entries are there
  {
    auto osn1 = direct->mapper->get_snaps(cln1);
    EXPECT_EQ(cln1_snaps, osn1);
    auto osn2 = direct->mapper->get_snaps(cln2);
    EXPECT_EQ(cln2_snaps, osn2);
    auto osnh = direct->mapper->get_snaps(head);
    EXPECT_EQ(head_snaps, osnh);
  }
  {
    auto vsn1 = direct->mapper->get_snaps_check_consistency(cln1);
    EXPECT_EQ(cln1_snaps, vsn1);
    auto vsn2 = direct->mapper->get_snaps_check_consistency(cln2);
    EXPECT_EQ(cln2_snaps, vsn2);
    auto vsnh = direct->mapper->get_snaps_check_consistency(head);
    EXPECT_EQ(head_snaps, vsnh);
  }

  // corrupt the SNA_ entry for cln1
  direct->shorten_mapping_key(300, cln1);
  {
    auto vsnh = direct->mapper->get_snaps_check_consistency(head);
    EXPECT_EQ(head_snaps, vsnh);
    auto vsn1 = direct->mapper->get_snaps(cln1);
    EXPECT_EQ(cln1_snaps, vsn1);
    auto osn1 = direct->mapper->get_snaps_check_consistency(cln1);
    EXPECT_NE(cln1_snaps, osn1);
    auto vsn2 = direct->mapper->get_snaps_check_consistency(cln2);
    EXPECT_EQ(cln2_snaps, vsn2);
  }
}

///\todo test the case of a corrupted OBJ_ entry
