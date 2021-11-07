// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <vector>
#include <boost/circular_buffer.hpp>
#include <boost/intrusive/set.hpp>
#include <gtest/gtest.h>
#include "cls/rgw/cls_rgw_client.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/random_string.h"
#include "global/global_context.h"
#include "test/librados/test_cxx.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

// create/destroy a pool that's shared by all tests in the process
struct RadosEnv : public ::testing::Environment {
  static std::optional<std::string> pool_name;
 public:
  static librados::Rados rados;
  static librados::IoCtx ioctx;

  void SetUp() override {
    // create pool
    std::string name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(name, rados));
    pool_name = name;
    ASSERT_EQ(rados.ioctx_create(name.c_str(), ioctx), 0);
  }
  void TearDown() override {
    ioctx.close();
    if (pool_name) {
      ASSERT_EQ(destroy_one_pool_pp(*pool_name, rados), 0);
    }
  }
};
std::optional<std::string> RadosEnv::pool_name;
librados::Rados RadosEnv::rados;
librados::IoCtx RadosEnv::ioctx;

auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);


std::ostream& operator<<(std::ostream& out, const rgw_bucket_category_stats& c) {
  return out << "{count=" << c.num_entries << " size=" << c.total_size << '}';
}

rgw_bucket_entry_ver last_version(librados::IoCtx& ioctx)
{
  rgw_bucket_entry_ver ver;
  ver.pool = ioctx.get_id();
  ver.epoch = ioctx.get_last_version();
  return ver;
}

int index_init(librados::IoCtx& ioctx, const std::string& oid)
{
  librados::ObjectWriteOperation op;
  cls_rgw_bucket_init_index(op);
  return ioctx.operate(oid, &op);
}

int index_prepare(librados::IoCtx& ioctx, const std::string& oid,
                  const cls_rgw_obj_key& key, const std::string& tag,
                  RGWModifyOp type)
{
  librados::ObjectWriteOperation op;
  const std::string loc; // empty
  constexpr bool log_op = false;
  constexpr int flags = 0;
  rgw_zone_set zones;
  cls_rgw_bucket_prepare_op(op, type, tag, key, loc, log_op, flags, zones);
  return ioctx.operate(oid, &op);
}

int index_complete(librados::IoCtx& ioctx, const std::string& oid,
                   const cls_rgw_obj_key& key, const std::string& tag,
                   RGWModifyOp type, const rgw_bucket_entry_ver& ver,
                   const rgw_bucket_dir_entry_meta& meta)
{
  librados::ObjectWriteOperation op;
  constexpr std::list<cls_rgw_obj_key>* remove_objs = nullptr;
  constexpr bool log_op = false;
  constexpr int flags = 0;
  constexpr rgw_zone_set* zones = nullptr;
  cls_rgw_bucket_complete_op(op, type, tag, ver, key, meta,
                             remove_objs, log_op, flags, zones);
  return ioctx.operate(oid, &op);
}

void read_stats(librados::IoCtx& ioctx, const std::string& oid,
                rgw_bucket_dir_stats& stats)
{
  auto oids = std::map<int, std::string>{{0, oid}};
  std::map<int, rgw_cls_list_ret> results;
  ASSERT_EQ(0, CLSRGWIssueGetDirHeader(ioctx, oids, results, 8)());
  ASSERT_EQ(1, results.size());
  stats = std::move(results.begin()->second.dir.header.stats);
}

static void account_entry(rgw_bucket_dir_stats& stats,
                          const rgw_bucket_dir_entry_meta& meta)
{
  rgw_bucket_category_stats& c = stats[meta.category];
  c.num_entries++;
  c.total_size += meta.accounted_size;
  c.total_size_rounded += cls_rgw_get_rounded_size(meta.accounted_size);
  c.actual_size += meta.size;
}

static void unaccount_entry(rgw_bucket_dir_stats& stats,
                            const rgw_bucket_dir_entry_meta& meta)
{
  rgw_bucket_category_stats& c = stats[meta.category];
  c.num_entries--;
  c.total_size -= meta.accounted_size;
  c.total_size_rounded -= cls_rgw_get_rounded_size(meta.accounted_size);
  c.actual_size -= meta.size;
}


struct object : rgw_bucket_dir_entry, boost::intrusive::set_base_hook<> {
  explicit object(const cls_rgw_obj_key& key) {
    this->key = key;
  }
};

struct object_key {
  using type = cls_rgw_obj_key;
  const type& operator()(const object& o) const { return o.key; }
};

using object_map_base = boost::intrusive::set<object,
      boost::intrusive::key_of_value<object_key>>;

struct object_map : object_map_base {
  ~object_map() {
    clear_and_dispose(std::default_delete<object>{});
  }
};

struct operation {
  RGWModifyOp type;
  cls_rgw_obj_key key;
  std::string tag;
  rgw_bucket_entry_ver ver;
  rgw_bucket_dir_entry_meta meta;
};

struct config {
  size_t max_operations = 0;
  size_t max_entries = 0;
  size_t max_pending = 0;
  size_t max_object_size = 0;
};

class simulator {
 public:
  simulator(librados::IoCtx& ioctx,
            std::string oid,
            const config& cfg)
      : ioctx(ioctx),
        oid(std::move(oid)),
        cfg(cfg),
        pending(cfg.max_pending)
  {
    // generate a set of object keys. each operation chooses one at random
    keys.reserve(cfg.max_entries);
    for (size_t i = 0; i < cfg.max_entries; i++) {
      keys.emplace_back(gen_rand_alphanumeric(g_ceph_context, 12));
    }
  }

  void run();

 private:
  void step();
  void start();
  int try_start(const cls_rgw_obj_key& key,
                const std::string& tag);
  void finish(const operation& op);
  void complete(const operation& op, RGWModifyOp type);
  void suggest(const operation& op, char suggestion);

  object_map::iterator find_or_create(const cls_rgw_obj_key& key);
  void check_stats();

  librados::IoCtx& ioctx;
  std::string oid;
  const config& cfg;

  std::vector<cls_rgw_obj_key> keys;
  object_map objects;
  boost::circular_buffer<operation> pending;
  rgw_bucket_dir_stats stats;
};


void simulator::run()
{
  // init the bucket index object
  ASSERT_EQ(0, index_init(ioctx, oid));
  // for the simulation for N steps
  for (size_t i = 0; i < cfg.max_operations; i++) {
    step();
  }

  // TODO: recalc stats and compare
}

void simulator::step()
{
  if (pending.full()) {
    auto& op = pending.front();
    finish(op);
    pending.pop_front();
    check_stats();
  }
  start();
  check_stats();
}

object_map::iterator simulator::find_or_create(const cls_rgw_obj_key& key)
{
  object_map::insert_commit_data commit;
  auto result = objects.insert_check(key, std::less<cls_rgw_obj_key>{}, commit);
  if (result.second) { // inserting new entry
    auto p = new object(key);
    result.first = objects.insert_commit(*p, commit);
  }
  return result.first;
}

int simulator::try_start(const cls_rgw_obj_key& key, const std::string& tag)
{
  // choose randomly betwen create and delete
  const auto type = static_cast<RGWModifyOp>(
      ceph::util::generate_random_number<size_t, size_t>(CLS_RGW_OP_ADD,
                                                         CLS_RGW_OP_DEL));
  // prepare operation
  int r = index_prepare(ioctx, oid, key, tag, type);
  if (r != 0) {
    derr << "> failed to prepare operation key=" << key
        << " tag=" << tag << " type=" << type
        << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  // on success, initialize the pending operation
  auto op = operation{type, key, tag, last_version(ioctx)};

  op.meta.category = static_cast<RGWObjCategory>(
      ceph::util::generate_random_number(0, 1));
  op.meta.size = op.meta.accounted_size =
      ceph::util::generate_random_number(1, cfg.max_object_size);

  dout(1) << "> prepared operation key=" << key
      << " tag=" << tag << " type=" << type
      << " cat=" << op.meta.category
      << " size=" << op.meta.size << dendl;
  ceph_assert(!pending.full());
  pending.push_back(std::move(op));

  find_or_create(key);
  return 0;
}

void simulator::start()
{
  // choose a random object key
  const size_t index = ceph::util::generate_random_number(0, keys.size() - 1);
  const auto& key = keys[index];
  // generate a random tag
  const auto tag = gen_rand_alphanumeric(g_ceph_context, 12);

  // retry until success
  while (try_start(key, tag) != 0)
    ;
}

void simulator::finish(const operation& op)
{
  // complete most operations, but finish some with cancel or dir suggest
  constexpr int cancel_percent = 10;
  constexpr int suggest_update_percent = 10;
  constexpr int suggest_remove_percent = 10;

  int result = ceph::util::generate_random_number(0, 99);
  if (result < cancel_percent) {
    complete(op, CLS_RGW_OP_CANCEL);
    return;
  }
  result -= cancel_percent;
  if (result < suggest_update_percent) {
    suggest(op, CEPH_RGW_UPDATE);
    return;
  }
  result -= suggest_update_percent;
  if (result < suggest_remove_percent) {
    suggest(op, CEPH_RGW_REMOVE);
    return;
  }
  complete(op, op.type);
}

void simulator::complete(const operation& op, RGWModifyOp type)
{
  int r = index_complete(ioctx, oid, op.key, op.tag, type, op.ver, op.meta);
  if (r != 0) {
    derr << "< failed to complete operation key=" << op.key
        << " tag=" << op.tag << " type=" << op.type
        << " cat=" << op.meta.category
        << " size=" << op.meta.size
        << ": " << cpp_strerror(r) << dendl;
    return;
  }

  if (type == CLS_RGW_OP_CANCEL) {
    dout(1) << "< canceled operation key=" << op.key
        << " tag=" << op.tag << " type=" << op.type
        << " cat=" << op.meta.category
        << " size=" << op.meta.size << dendl;
  } else if (type == CLS_RGW_OP_ADD) {
    auto obj = find_or_create(op.key);
    if (obj->exists) {
      unaccount_entry(stats, obj->meta);
    }
    obj->exists = true;
    obj->meta = op.meta;
    account_entry(stats, obj->meta);
    dout(1) << "< completed write operation key=" << op.key
        << " tag=" << op.tag << " type=" << type
        << " cat=" << op.meta.category
        << " size=" << op.meta.size << dendl;
  } else {
    ceph_assert(type == CLS_RGW_OP_DEL);
    auto obj = objects.find(op.key, std::less<cls_rgw_obj_key>{});
    if (obj != objects.end()) {
      if (obj->exists) {
        unaccount_entry(stats, obj->meta);
      }
      objects.erase_and_dispose(obj, std::default_delete<object>{});
    }
    dout(1) << "< completed delete operation key=" << op.key
        << " tag=" << op.tag << " type=" << type
        << " cat=" << op.meta.category << dendl;
  }
}

void simulator::suggest(const operation& op, char suggestion)
{
  // read and decode the current dir entry
  rgw_cls_bi_entry bi_entry;
  int r = cls_rgw_bi_get(ioctx, oid, BIIndexType::Plain, op.key, &bi_entry);
  if (r != 0) {
    derr << "< no bi entry to suggest for operation key=" << op.key
        << " tag=" << op.tag << " type=" << op.type
        << " cat=" << op.meta.category
        << " size=" << op.meta.size
        << ": " << cpp_strerror(r) << dendl;
    return;
  }
  ASSERT_EQ(bi_entry.type, BIIndexType::Plain);

  rgw_bucket_dir_entry entry;
  auto p = bi_entry.data.cbegin();
  ASSERT_NO_THROW(decode(entry, p));

  ASSERT_EQ(entry.key, op.key);

  // clear pending info and write it back; this cancels those pending
  // operations (we'll see EINVAL when we try to complete them), but dir
  // suggest is ignored unless the pending_map is empty
  entry.pending_map.clear();

  bi_entry.data.clear();
  encode(entry, bi_entry.data);

  r = cls_rgw_bi_put(ioctx, oid, bi_entry);
  ASSERT_EQ(0, r);

  // now suggest changes for this entry
  entry.ver = last_version(ioctx);
  entry.exists = (suggestion == CEPH_RGW_UPDATE);
  entry.meta = op.meta;

  bufferlist update;
  cls_rgw_encode_suggestion(suggestion, entry, update);

  librados::ObjectWriteOperation write_op;
  cls_rgw_suggest_changes(write_op, update);
  r = ioctx.operate(oid, &write_op);
  if (r != 0) {
    derr << "< failed to suggest operation key=" << op.key
        << " tag=" << op.tag << " type=" << op.type
        << " cat=" << op.meta.category
        << " size=" << op.meta.size
        << ": " << cpp_strerror(r) << dendl;
    return;
  }

  // update our cache accordingly
  if (suggestion == CEPH_RGW_UPDATE) {
    auto obj = find_or_create(op.key);
    if (obj->exists) {
      unaccount_entry(stats, obj->meta);
    }
    obj->exists = true;
    obj->meta = op.meta;
    account_entry(stats, obj->meta);
    dout(1) << "< suggested update operation key=" << op.key
        << " tag=" << op.tag << " type=" << op.type
        << " cat=" << op.meta.category
        << " size=" << op.meta.size << dendl;
  } else {
    ceph_assert(suggestion == CEPH_RGW_REMOVE);
    auto obj = objects.find(op.key, std::less<cls_rgw_obj_key>{});
    if (obj != objects.end()) {
      if (obj->exists) {
        unaccount_entry(stats, obj->meta);
      }
      objects.erase_and_dispose(obj, std::default_delete<object>{});
    }
    dout(1) << "< suggested remove operation key=" << op.key
        << " tag=" << op.tag << " type=" << op.type
        << " cat=" << op.meta.category << dendl;
  }
}

void simulator::check_stats()
{
  rgw_bucket_dir_stats stored_stats;
  read_stats(ioctx, oid, stored_stats);

  const rgw_bucket_dir_stats& expected_stats = stats;
  ASSERT_EQ(expected_stats, stored_stats);
}

TEST(cls_rgw_stats, simulate)
{
  const char* bucket_oid = __func__;
  config cfg;
  cfg.max_operations = 2048;
  cfg.max_entries = 64;
  cfg.max_pending = 16;
  cfg.max_object_size = 4*1024*1024; // 4M
  auto sim = simulator{RadosEnv::ioctx, bucket_oid, cfg};
  sim.run();
}
