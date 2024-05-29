// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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


// simulation parameters:

// total number of index operations to prepare
constexpr size_t max_operations = 2048;
// total number of object names. each operation chooses one at random
constexpr size_t max_entries = 32;
// maximum number of pending operations. once the limit is reached, the oldest
// pending operation is finished before another can start
constexpr size_t max_pending = 16;
// object size is randomly distributed between 0 and 4M
constexpr size_t max_object_size = 4*1024*1024;
// multipart upload threshold
constexpr size_t max_part_size = 1024*1024;


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


// librados helpers
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
                   const rgw_bucket_dir_entry_meta& meta,
                   std::list<cls_rgw_obj_key>* remove_objs)
{
  librados::ObjectWriteOperation op;
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


// a map of cached dir entries representing the expected state of cls_rgw
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


// models a bucket index operation, starting with cls_rgw_bucket_prepare_op().
// stores all of the state necessary to complete the operation, either with
// cls_rgw_bucket_complete_op() or cls_rgw_suggest_changes(). uploads larger
// than max_part_size are modeled as multipart uploads
struct operation {
  RGWModifyOp type;
  cls_rgw_obj_key key;
  std::string tag;
  rgw_bucket_entry_ver ver;
  std::string upload_id; // empty unless multipart
  rgw_bucket_dir_entry_meta meta;
};


class simulator {
 public:
  simulator(librados::IoCtx& ioctx, std::string oid)
      : ioctx(ioctx), oid(std::move(oid)), pending(max_pending)
  {
    // generate a set of object keys. each operation chooses one at random
    keys.reserve(max_entries);
    for (size_t i = 0; i < max_entries; i++) {
      keys.emplace_back(gen_rand_alphanumeric_upper(g_ceph_context, 12));
    }
  }

  void run();

 private:
  void start();
  int try_start(const cls_rgw_obj_key& key,
                const std::string& tag);

  void finish(const operation& op);
  void complete(const operation& op, RGWModifyOp type);
  void suggest(const operation& op, char suggestion);

  int init_multipart(const operation& op);
  void complete_multipart(const operation& op);

  object_map::iterator find_or_create(const cls_rgw_obj_key& key);

  librados::IoCtx& ioctx;
  std::string oid;

  std::vector<cls_rgw_obj_key> keys;
  object_map objects;
  boost::circular_buffer<operation> pending;
  rgw_bucket_dir_stats stats;
};


void simulator::run()
{
  // init the bucket index object
  ASSERT_EQ(0, index_init(ioctx, oid));
  // run the simulation for N steps
  for (size_t i = 0; i < max_operations; i++) {
    if (pending.full()) {
      // if we're at max_pending, finish the oldest operation
      auto& op = pending.front();
      finish(op);
      pending.pop_front();

      // verify bucket stats
      rgw_bucket_dir_stats stored_stats;
      read_stats(ioctx, oid, stored_stats);

      const rgw_bucket_dir_stats& expected_stats = stats;
      ASSERT_EQ(expected_stats, stored_stats);
    }

    // initiate the next operation
    start();

    // verify bucket stats
    rgw_bucket_dir_stats stored_stats;
    read_stats(ioctx, oid, stored_stats);

    const rgw_bucket_dir_stats& expected_stats = stats;
    ASSERT_EQ(expected_stats, stored_stats);
  }
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
  // choose randomly between create and delete
  const auto type = static_cast<RGWModifyOp>(
      ceph::util::generate_random_number<size_t, size_t>(CLS_RGW_OP_ADD,
                                                         CLS_RGW_OP_DEL));
  auto op = operation{type, key, tag};

  op.meta.category = RGWObjCategory::Main;
  op.meta.size = op.meta.accounted_size =
      ceph::util::generate_random_number(1, max_object_size);

  if (type == CLS_RGW_OP_ADD && op.meta.size > max_part_size) {
    // simulate multipart for uploads over the max_part_size threshold
    op.upload_id = gen_rand_alphanumeric_upper(g_ceph_context, 12);

    int r = init_multipart(op);
    if (r != 0) {
      derr << "> failed to prepare multipart upload key=" << key
          << " upload=" << op.upload_id << " tag=" << tag
          << " type=" << type << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    dout(1) << "> prepared multipart upload key=" << key
        << " upload=" << op.upload_id << " tag=" << tag
        << " type=" << type << " size=" << op.meta.size << dendl;
  } else {
    // prepare operation
    int r = index_prepare(ioctx, oid, op.key, op.tag, op.type);
    if (r != 0) {
      derr << "> failed to prepare operation key=" << key
          << " tag=" << tag << " type=" << type
          << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    dout(1) << "> prepared operation key=" << key
        << " tag=" << tag << " type=" << type
        << " size=" << op.meta.size << dendl;
  }
  op.ver = last_version(ioctx);

  ceph_assert(!pending.full());
  pending.push_back(std::move(op));
  return 0;
}

void simulator::start()
{
  // choose a random object key
  const size_t index = ceph::util::generate_random_number(0, keys.size() - 1);
  const auto& key = keys[index];
  // generate a random tag
  const auto tag = gen_rand_alphanumeric_upper(g_ceph_context, 12);

  // retry until success. failures don't count towards max_operations
  while (try_start(key, tag) != 0)
    ;
}

void simulator::finish(const operation& op)
{
  if (op.type == CLS_RGW_OP_ADD && !op.upload_id.empty()) {
    // multipart uploads either complete or abort based on part uploads
    complete_multipart(op);
    return;
  }

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
  int r = index_complete(ioctx, oid, op.key, op.tag, type,
                         op.ver, op.meta, nullptr);
  if (r != 0) {
    derr << "< failed to complete operation key=" << op.key
        << " tag=" << op.tag << " type=" << op.type
        << " size=" << op.meta.size
        << ": " << cpp_strerror(r) << dendl;
    return;
  }

  if (type == CLS_RGW_OP_CANCEL) {
    dout(1) << "< canceled operation key=" << op.key
        << " tag=" << op.tag << " type=" << op.type
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
        << " tag=" << op.tag << " type=" << type << dendl;
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
        << " tag=" << op.tag << " type=" << op.type << dendl;
  }
}

int simulator::init_multipart(const operation& op)
{
  // create (not just prepare) the meta object
  const auto meta_key = cls_rgw_obj_key{
    fmt::format("_multipart_{}.2~{}.meta", op.key.name, op.upload_id)};
  const std::string empty_tag; // empty tag enables complete without prepare
  const rgw_bucket_entry_ver empty_ver;
  rgw_bucket_dir_entry_meta meta_meta;
  meta_meta.category = RGWObjCategory::MultiMeta;
  int r = index_complete(ioctx, oid, meta_key, empty_tag, CLS_RGW_OP_ADD,
                         empty_ver, meta_meta, nullptr);
  if (r != 0) {
    derr << "  < failed to create multipart meta key=" << meta_key
        << ": " << cpp_strerror(r) << dendl;
    return r;
  } else {
    // account for meta object
    auto obj = find_or_create(meta_key);
    if (obj->exists) {
      unaccount_entry(stats, obj->meta);
    }
    obj->exists = true;
    obj->meta = meta_meta;
    account_entry(stats, obj->meta);
  }

  // prepare part uploads
  std::list<cls_rgw_obj_key> remove_objs;
  size_t part_id = 0;

  size_t remaining = op.meta.size;
  while (remaining > max_part_size) {
    remaining -= max_part_size;
    const auto part_size = std::min(remaining, max_part_size);
    const auto part_key = cls_rgw_obj_key{
      fmt::format("_multipart_{}.2~{}.{}", op.key.name, op.upload_id, part_id)};
    part_id++;

    r = index_prepare(ioctx, oid, part_key, op.tag, op.type);
    if (r != 0) {
      // if part prepare fails, remove the meta object and remove_objs
      [[maybe_unused]] int ignored =
          index_complete(ioctx, oid, meta_key, empty_tag, CLS_RGW_OP_DEL,
                         empty_ver, meta_meta, &remove_objs);
      derr << "  > failed to prepare part key=" << part_key
          << " size=" << part_size << dendl;
      return r; // return the error from prepare
    }
    dout(1) << "  > prepared part key=" << part_key
        << " size=" << part_size << dendl;
    remove_objs.push_back(part_key);
  }
  return 0;
}

void simulator::complete_multipart(const operation& op)
{
  const std::string empty_tag; // empty tag enables complete without prepare
  const rgw_bucket_entry_ver empty_ver;

  // try to finish part uploads
  size_t part_id = 0;
  std::list<cls_rgw_obj_key> remove_objs;

  RGWModifyOp type = op.type; // OP_ADD, or OP_CANCEL for abort

  size_t remaining = op.meta.size;
  while (remaining > max_part_size) {
    remaining -= max_part_size;
    const auto part_size = std::min(remaining, max_part_size);
    const auto part_key = cls_rgw_obj_key{
      fmt::format("_multipart_{}.2~{}.{}", op.key.name, op.upload_id, part_id)};
    part_id++;

    // cancel 10% of part uploads (and abort the multipart upload)
    constexpr int cancel_percent = 10;
    const int result = ceph::util::generate_random_number(0, 99);
    if (result < cancel_percent) {
      type = CLS_RGW_OP_CANCEL; // abort multipart
      dout(1) << "  < canceled part key=" << part_key
          << " size=" << part_size << dendl;
    } else {
      rgw_bucket_dir_entry_meta meta;
      meta.category = op.meta.category;
      meta.size = meta.accounted_size = part_size;

      int r = index_complete(ioctx, oid, part_key, op.tag, op.type,
                             empty_ver, meta, nullptr);
      if (r != 0) {
        derr << "  < failed to complete part key=" << part_key
            << " size=" << meta.size << ": " << cpp_strerror(r) << dendl;
        type = CLS_RGW_OP_CANCEL; // abort multipart
      } else {
        dout(1) << "  < completed part key=" << part_key
            << " size=" << meta.size << dendl;
        // account for successful part upload
        auto obj = find_or_create(part_key);
        if (obj->exists) {
          unaccount_entry(stats, obj->meta);
        }
        obj->exists = true;
        obj->meta = meta;
        account_entry(stats, obj->meta);
      }
    }
    remove_objs.push_back(part_key);
  }

  // delete the multipart meta object
  const auto meta_key = cls_rgw_obj_key{
    fmt::format("_multipart_{}.2~{}.meta", op.key.name, op.upload_id)};
  rgw_bucket_dir_entry_meta meta_meta;
  meta_meta.category = RGWObjCategory::MultiMeta;

  int r = index_complete(ioctx, oid, meta_key, empty_tag, CLS_RGW_OP_DEL,
                         empty_ver, meta_meta, nullptr);
  if (r != 0) {
    derr << "  < failed to remove multipart meta key=" << meta_key
        << ": " << cpp_strerror(r) << dendl;
  } else {
    // unaccount for meta object
    auto obj = objects.find(meta_key, std::less<cls_rgw_obj_key>{});
    if (obj != objects.end()) {
      if (obj->exists) {
        unaccount_entry(stats, obj->meta);
      }
      objects.erase_and_dispose(obj, std::default_delete<object>{});
    }
  }

  // create or cancel the head object
  r = index_complete(ioctx, oid, op.key, empty_tag, type,
                     empty_ver, op.meta, &remove_objs);
  if (r != 0) {
    derr << "< failed to complete multipart upload key=" << op.key
        << " upload=" << op.upload_id << " tag=" << op.tag
        << " type=" << type << " size=" << op.meta.size
        << ": " << cpp_strerror(r) << dendl;
    return;
  }

  if (type == CLS_RGW_OP_ADD) {
    dout(1) << "< completed multipart upload key=" << op.key
        << " upload=" << op.upload_id << " tag=" << op.tag
        << " type=" << op.type << " size=" << op.meta.size << dendl;

    // account for head stats
    auto obj = find_or_create(op.key);
    if (obj->exists) {
      unaccount_entry(stats, obj->meta);
    }
    obj->exists = true;
    obj->meta = op.meta;
    account_entry(stats, obj->meta);
  } else {
    dout(1) << "< canceled multipart upload key=" << op.key
        << " upload=" << op.upload_id << " tag=" << op.tag
        << " type=" << op.type << " size=" << op.meta.size << dendl;
  }

  // unaccount for remove_objs
  for (const auto& part_key : remove_objs) {
    auto obj = objects.find(part_key, std::less<cls_rgw_obj_key>{});
    if (obj != objects.end()) {
      if (obj->exists) {
        unaccount_entry(stats, obj->meta);
      }
      objects.erase_and_dispose(obj, std::default_delete<object>{});
    }
  }
}

TEST(cls_rgw_stats, simulate)
{
  const char* bucket_oid = __func__;
  auto sim = simulator{RadosEnv::ioctx, bucket_oid};
  sim.run();
}
