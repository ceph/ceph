#include "cyan_store.h"

#include <fmt/format.h>

#include "common/safe_io.h"

#include "crimson/os/cyan_collection.h"
#include "crimson/os/cyan_object.h"
#include "crimson/os/Transaction.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_filestore);
  }
}

namespace ceph::os {

using ObjectRef = boost::intrusive_ptr<Object>;

CyanStore::CyanStore(const std::string& path)
  : path{path}
{}

CyanStore::~CyanStore() = default;

seastar::future<> CyanStore::mount()
{
  bufferlist bl;
  string fn = path + "/collections";
  string err;
  if (int r = bl.read_file(fn.c_str(), &err); r < 0) {
    throw std::runtime_error("read_file");
  }

  set<coll_t> collections;
  auto p = bl.cbegin();
  decode(collections, p);

  for (auto& coll : collections) {
    string fn = fmt::format("{}/{}", path, coll);
    bufferlist cbl;
    if (int r = cbl.read_file(fn.c_str(), &err); r < 0) {
      throw std::runtime_error("read_file");
    }
    CollectionRef c{new Collection{coll}};
    auto p = cbl.cbegin();
    c->decode(p);
    coll_map[coll] = c;
    used_bytes += c->used_bytes();
  }
  return seastar::now();
}

seastar::future<> CyanStore::umount()
{
  set<coll_t> collections;
  for (auto& [col, ch] : coll_map) {
    collections.insert(col);
    bufferlist bl;
    ceph_assert(ch);
    ch->encode(bl);
    string fn = fmt::format("{}/{}", path, col);
    if (int r = bl.write_file(fn.c_str()); r < 0) {
      throw std::runtime_error("write_file");
    }
  }

  string fn = path + "/collections";
  bufferlist bl;
  encode(collections, bl);
  if (int r = bl.write_file(fn.c_str()); r < 0) {
    throw std::runtime_error("write_file");
  }
  return seastar::now();
}

seastar::future<> CyanStore::mkfs(uuid_d osd_fsid)
{
  string fsid_str;
  int r = read_meta("fsid", &fsid_str);
  if (r == -ENOENT) {
    write_meta("fsid", fmt::format("{}", osd_fsid));
  } else if (r < 0) {
    throw std::runtime_error("read_meta");
  } else {
    logger().error("{} already has fsid {}", __func__, fsid_str);
    throw std::runtime_error("mkfs");
  }

  string fn = path + "/collections";
  bufferlist bl;
  set<coll_t> collections;
  encode(collections, bl);
  r = bl.write_file(fn.c_str());
  if (r < 0)
    throw std::runtime_error("write_file");

  write_meta("type", "memstore");
  return seastar::now();
}

CyanStore::CollectionRef CyanStore::create_new_collection(const coll_t& cid)
{
  auto c = new Collection{cid};
  return new_coll_map[cid] = c;
}

CyanStore::CollectionRef CyanStore::open_collection(const coll_t& cid)
{
  auto cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return {};
  return cp->second;
}

std::vector<coll_t> CyanStore::list_collections()
{
  std::vector<coll_t> collections;
  for (auto& coll : coll_map) {
    collections.push_back(coll.first);
  }
  return collections;
}

seastar::future<bufferlist> CyanStore::read(CollectionRef c,
                                            const ghobject_t& oid,
                                            uint64_t offset,
                                            size_t len,
                                            uint32_t op_flags)
{
  logger().info("{} {} {} {}~{}",
                __func__, c->cid, oid, offset, len);
  if (!c->exists) {
    throw std::runtime_error(fmt::format("collection does not exist: {}", c->cid));
  }
  ObjectRef o = c->get_object(oid);
  if (!o) {
    throw std::runtime_error(fmt::format("object does not exist: {}", oid));
  }
  if (offset >= o->get_size())
    return seastar::make_ready_future<bufferlist>();
  size_t l = len;
  if (l == 0 && offset == 0)  // note: len == 0 means read the entire object
    l = o->get_size();
  else if (offset + l > o->get_size())
    l = o->get_size() - offset;
  bufferlist bl;
  if (int r = o->read(offset, l, bl); r < 0) {
    throw std::runtime_error("read");
  }
  return seastar::make_ready_future<bufferlist>(std::move(bl));
}

seastar::future<CyanStore::omap_values_t>
CyanStore::omap_get_values(CollectionRef c,
                           const ghobject_t& oid,
                           std::vector<std::string>&& keys)
{
  logger().info("{} {} {}",
                __func__, c->cid, oid);
  auto o = c->get_object(oid);
  if (!o) {
    throw std::runtime_error(fmt::format("object does not exist: {}", oid));
  }
  omap_values_t values;
  for (auto& key : keys) {
    if (auto found = o->omap.find(key); found != o->omap.end()) {
      values.insert(*found);
    }
  }
  return seastar::make_ready_future<omap_values_t>(std::move(values));
}

seastar::future<> CyanStore::do_transaction(CollectionRef ch,
                                            Transaction&& t)
{
  auto i = t.begin();
  while (i.have_op()) {
    Transaction::Op* op = i.decode_op();
    int r = 0;
    switch (op->op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_WRITE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
        r = _write(cid, oid, off, len, bl, fadvise_flags);
      }
      break;
    case Transaction::OP_MKCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
        r = _create_collection(cid, op->split_bits);
      }
      break;
    default:
      logger().error("bad op {}", static_cast<unsigned>(op->op));
      abort();
    }
    if (r < 0) {
      abort();
    }
  }
  return seastar::now();
}

int CyanStore::_write(const coll_t& cid, const ghobject_t& oid,
                       uint64_t offset, size_t len, const bufferlist& bl,
                       uint32_t fadvise_flags)
{
  logger().info("{} {} {} {} ~ {}",
                __func__, cid, oid, offset, len);
  assert(len == bl.length());

  auto c = open_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_or_create_object(oid);
  if (len > 0) {
    const ssize_t old_size = o->get_size();
    o->write(offset, bl);
    used_bytes += (o->get_size() - old_size);
  }

  return 0;
}

int CyanStore::_create_collection(const coll_t& cid, int bits)
{
  auto result = coll_map.insert(std::make_pair(cid, CollectionRef()));
  if (!result.second)
    return -EEXIST;
  auto p = new_coll_map.find(cid);
  assert(p != new_coll_map.end());
  result.first->second = p->second;
  result.first->second->bits = bits;
  new_coll_map.erase(p);
  return 0;
}

void CyanStore::write_meta(const std::string& key,
                           const std::string& value)
{
  std::string v = value;
  v += "\n";
  if (int r = safe_write_file(path.c_str(), key.c_str(),
                              v.c_str(), v.length());
      r < 0) {
    throw std::runtime_error{fmt::format("unable to write_meta({})", key)};
  }
}

int CyanStore::read_meta(const std::string& key,
                          std::string* value)
{
  char buf[4096];
  int r = safe_read_file(path.c_str(), key.c_str(),
                         buf, sizeof(buf));
  if (r <= 0) {
    return r;
  }
  // drop trailing newlines
  while (r && isspace(buf[r-1])) {
    --r;
  }
  *value = string{buf, static_cast<size_t>(r)};
  return 0;
}
}
