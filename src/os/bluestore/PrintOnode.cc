#include "BlueStore.h"
#include "common/pretty_binary.h"
#include "os/kv.h"

#undef dout_prefix
#define dout_prefix *_dout << "bluestore "
#undef dout_context
#define dout_context store.cct
#define dout_subsys ceph_subsys_bluestore

const std::string PREFIX_OBJ = "O";    // object name -> onode_t
const std::string PREFIX_SHARED_BLOB = "X"; // u64 SB id -> shared_blob_t

template<typename S>
int get_key_object(const S& key, ghobject_t *oid);

template<typename S>
void get_extent_shard_key(const S& onode_key, uint32_t offset, std::string *key);

class BlueStore::ExtentMap::ExtentDecoderPrinter : public BlueStore::ExtentMap::ExtentDecoder {
  BlueStore& store;
  BlueStore::Extent extent;
  std::vector<BlobRef> blobs;
  BlueStore::blob_map_t& spanning_blobs;
  std::stringstream extent_desc;
  std::stringstream blob_desc;
  std::string blob_name;

protected:
  std::string desc_blob(const BlobRef blob) {
    const bluestore_blob_t& bb = blob->get_blob();
    std::stringstream ss;
    if (bb.flags & bluestore_blob_t::FLAG_SHARED) {
      uint64_t sbid = blob->shared_blob->get_sbid();
      ss << "sb(" << sbid << ")";
    }
    ss << bb.get_extents();
    if (bb.flags & bluestore_blob_t::FLAG_COMPRESSED) {
      ss << "compr(" << std::hex << bb.get_logical_length() << "/"
         << bb.get_compressed_payload_length() << std::dec << ")";
    }
    if (bb.flags & bluestore_blob_t::FLAG_CSUM) {
      ss << "csum(" << bb.csum_data.length() << "bytes)";
    }
    return ss.str();
  }

  void consume_blobid(BlueStore::Extent* le, bool spanning, uint64_t blobid) override {
    dout(30) << __func__ << " spanning=" << spanning << " blobid=" << blobid << dendl;
    if (spanning) {
      le->blob = spanning_blobs[blobid];
      blob_name = "spann " + std::to_string(blobid);
    } else {
      le->blob = blobs[blobid];
      blob_name = "local " + std::to_string(blobid);
    }
  }

  void consume_blob(BlueStore::Extent* le,
                    uint64_t extent_no,
                    uint64_t sbid,
                    BlobRef b) override {
    le->blob = b;
    dout(30) << __func__ << " n=" << extent_no << " sbid=" << sbid
             << " le=" << *le << dendl;
    blobs.resize(extent_no + 1);
    blobs[extent_no] = b;
    blob_name = "local " + std::to_string(extent_no);
    if (sbid) {
      b->shared_blob = new SharedBlob(sbid, nullptr);
    }
    blob_desc << blob_name << " ";
    blob_desc << desc_blob(le->blob);
    blob_desc << std::endl;
  }

  void consume_spanning_blob(uint64_t sbid, BlobRef b) override {
    dout(30) << __func__ << " b->id=" << b->id << " sbid=" << sbid << dendl;
    ceph_assert(b->id >= 0);
    spanning_blobs[b->id] = b;
    if (sbid) {
      ceph_assert(b->get_blob().flags & bluestore_blob_t::FLAG_SHARED);
      b->shared_blob = new SharedBlob(sbid, nullptr);
    }
    blob_desc << "spann " << std::to_string(b->id)
              << " " << desc_blob(b) << std::endl;
  }

  BlueStore::Extent* get_next_extent() override {
    dout(30) << __func__ << dendl;
    BlueStore::Extent* le = new Extent();
    return le;
  }

  void add_extent(Extent* le) override {
    dout(30) << __func__ << *le << dendl;
    extent_desc << std::hex << "0x" << le->logical_offset << "~" << le->length << std::dec;
    extent_desc << " @" << blob_name
                << "/0x" << std::hex << le->blob_offset << std::dec << std::endl;
    le->blob.reset();
    delete le;
  }

public:
  ExtentDecoderPrinter(BlueStore& _store, BlueStore::blob_map_t& spanning_blobs)
    : store(_store)
    , spanning_blobs(spanning_blobs) {
  }

  std::string get_string() {
    return blob_desc.str() + extent_desc.str();
  }
};

#undef dout_context
#define dout_context cct

int BlueStore::print_onode(const std::string& key, std::string* out)
{
  std::stringstream report;
  ghobject_t oid;
  coll_t cid;
  Collection* c = new Collection(this, nullptr, nullptr, cid);
  CollectionRef cr(c);
  bufferlist v;
  size_t metadata_size = 0;
  int r;
  r = db->get(PREFIX_OBJ, key.c_str(), key.size(), &v);
  if (r != 0) {
    return -ENOENT;
  }
  get_key_object(key, &oid);
  report << "oid " << oid
         << " size " << v.length() << std::endl;
  metadata_size += v.length();
  Onode on(c, oid, key);
  on.exists = true;

  BlueStore::blob_map_t spanning_blobs;
  ExtentMap::ExtentDecoderPrinter edecoder(*this, spanning_blobs);
  //copied from Onode::decode_raw
  auto p = v.front().begin_deep();
  on.onode.decode(p);
  // here we have bluestore_onode_t onode just decoded

  // initialize extent_map
  edecoder.decode_spanning_blobs(p, c);
  if (on.onode.extent_map_shards.empty()) {
    bufferlist v;
    denc(v, p);
    edecoder.decode_some(v, c);
    report << edecoder.get_string();
  } else {
    // we might have dumped spanning blobs
    report << edecoder.get_string();
  }
  //^copied from Onode::decode_raw

  for (auto& x : on.onode.extent_map_shards) {
    bufferlist v;
    std::string shard_key;
    get_extent_shard_key(key, x.offset, &shard_key);
    dout(30) << __func__ << " shard_key=" << pretty_binary_string(shard_key) << dendl;
    r = db->get(PREFIX_OBJ, shard_key.c_str(), shard_key.size(), &v);
    if (r != 0) {
      return -EIO;
    }
    report << "shard @ 0x" << std::hex << x.offset << std::dec
       << " size " << v.length() << std::endl;
    metadata_size += v.length();
    ExtentMap::ExtentDecoderPrinter edecoder(*this, spanning_blobs);
    edecoder.decode_some(v, c);
    report << edecoder.get_string();
  }
  report << "total metadata size " << metadata_size << std::endl;
  dout(20) << std::endl << report.str() << dendl;
  *out = report.str();
  return 0;
}

int BlueStore::print_shared_blob(uint64_t blob_id, std::string* out)
{
  std::stringstream report;
  std::string key;
  bufferlist v;
  _key_encode_u64(blob_id, &key);
  dout(30) << __func__ << " blob_key=" << pretty_binary_string(key) << dendl;
  int r = db->get(PREFIX_SHARED_BLOB, key, &v);
  if (r < 0) {
    return -ENOENT;
  }
  bluestore_shared_blob_t blob(blob_id);
  auto p = v.cbegin();
  decode(blob, p);
  report << "ID: " << blob_id << std::endl;
  report << blob.ref_map << std::endl;
  *out = report.str();
  return 0;
}
