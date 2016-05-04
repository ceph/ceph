#include "scrub_types.h"

using namespace librados;

void object_id_wrapper::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(name, bl);
  ::encode(nspace, bl);
  ::encode(locator, bl);
  ::encode(snap, bl);
  ENCODE_FINISH(bl);
}

void object_id_wrapper::decode(bufferlist::iterator& bp)
{
  DECODE_START(1, bp);
  ::decode(name, bp);
  ::decode(nspace, bp);
  ::decode(locator, bp);
  ::decode(snap, bp);
  DECODE_FINISH(bp);
}

static void encode(const object_id_t& obj, bufferlist& bl)
{
  reinterpret_cast<const object_id_wrapper&>(obj).encode(bl);
}

void shard_info_wrapper::set_object(const ScrubMap::object& object)
{
  for (auto attr : object.attrs) {
    bufferlist bl;
    bl.push_back(attr.second);
    attrs.insert(std::make_pair(attr.first, std::move(bl)));
  }
  size = object.size;
  if (object.omap_digest_present) {
    omap_digest_present = true;
    omap_digest = object.omap_digest;
  }
  if (object.digest_present) {
    data_digest_present = true;
    data_digest = object.digest;
  }
  if (object.read_error) {
    errors |= SHARD_READ_ERR;
  }
  if (object.stat_error) {
    errors |= SHARD_STAT_ERR;
  }
}

void shard_info_wrapper::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(errors, bl);
  if (has_shard_missing()) {
    return;
  }
  ::encode(attrs, bl);
  ::encode(size, bl);
  ::encode(omap_digest_present, bl);
  ::encode(omap_digest, bl);
  ::encode(data_digest_present, bl);
  ::encode(data_digest, bl);
  ENCODE_FINISH(bl);
}

void shard_info_wrapper::decode(bufferlist::iterator& bp)
{
  DECODE_START(1, bp);
  ::decode(errors, bp);
  if (has_shard_missing()) {
    return;
  }
  ::decode(attrs, bp);
  ::decode(size, bp);
  ::decode(omap_digest_present, bp);
  ::decode(omap_digest, bp);
  ::decode(data_digest_present, bp);
  ::decode(data_digest, bp);
  DECODE_FINISH(bp);
}

inconsistent_obj_wrapper::inconsistent_obj_wrapper(const hobject_t& hoid)
  : inconsistent_obj_t{librados::object_id_t{hoid.oid.name,
                                 hoid.nspace,
                                 hoid.get_key(), hoid.snap}}
{}

void inconsistent_obj_wrapper::add_shard(const pg_shard_t& pgs,
                                         const shard_info_wrapper& shard)
{
  errors |= shard.errors;
  shards[pgs.osd] = shard;
}

void
inconsistent_obj_wrapper::set_auth_missing(const hobject_t& hoid,
                                           const map<pg_shard_t, ScrubMap*>& maps)
{
  errors |= (err_t::SHARD_MISSING |
             err_t::SHARD_READ_ERR |
             err_t::OMAP_DIGEST_MISMATCH |
             err_t::DATA_DIGEST_MISMATCH |
             err_t::ATTR_MISMATCH);
  for (auto pg_map : maps) {
    auto oid_object = pg_map.second->objects.find(hoid);
    shard_info_wrapper shard;
    if (oid_object == pg_map.second->objects.end()) {
      shard.set_missing();
    } else {
      shard.set_object(oid_object->second);
    }
      shards[pg_map.first.osd] = shard;
  }
}

namespace librados {
  static void encode(const shard_info_t& shard, bufferlist& bl)
  {
    reinterpret_cast<const shard_info_wrapper&>(shard).encode(bl);
  }
}

void inconsistent_obj_wrapper::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(errors, bl);
  ::encode(object, bl);
  ::encode(shards, bl);
  ENCODE_FINISH(bl);
}

void inconsistent_obj_wrapper::decode(bufferlist::iterator& bp)
{
  DECODE_START(1, bp);
  ::decode(errors, bp);
  ::decode(object, bp);
  ::decode(shards, bp);
  DECODE_FINISH(bp);
}

inconsistent_snapset_wrapper::inconsistent_snapset_wrapper(const hobject_t& hoid)
  : inconsistent_snapset_t{object_id_t{hoid.oid.name,
                                       hoid.nspace,
                                       hoid.get_key(),
                                       hoid.snap}}
{}

using inc_snapset_t = inconsistent_snapset_t;

void inconsistent_snapset_wrapper::set_headless()
{
  errors |= inc_snapset_t::HEADLESS_CLONE;
}

void inconsistent_snapset_wrapper::set_ss_attr_missing()
{
  errors |= inc_snapset_t::SNAPSET_MISSING;
}

void inconsistent_snapset_wrapper::set_oi_attr_missing()
{
  errors |= inc_snapset_t::OI_MISSING;
}

void inconsistent_snapset_wrapper::set_ss_attr_corrupted()
{
  errors |= inc_snapset_t::SNAPSET_CORRUPTED;
}

void inconsistent_snapset_wrapper::set_oi_attr_corrupted()
{
  errors |= inc_snapset_t::OI_CORRUPTED;
}

void inconsistent_snapset_wrapper::set_clone_missing(snapid_t snap)
{
  errors |= inc_snapset_t::CLONE_MISSING;
  missing.push_back(snap);
}

void inconsistent_snapset_wrapper::set_clone(snapid_t snap)
{
  errors |= inc_snapset_t::EXTRA_CLONES;
  clones.push_back(snap);
}

void inconsistent_snapset_wrapper::set_snapset_mismatch()
{
  errors |= inc_snapset_t::SNAP_MISMATCH;
}

void inconsistent_snapset_wrapper::set_head_mismatch()
{
  errors |= inc_snapset_t::HEAD_MISMATCH;
}

void inconsistent_snapset_wrapper::set_size_mismatch()
{
  errors |= inc_snapset_t::SIZE_MISMATCH;
}

void inconsistent_snapset_wrapper::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(errors, bl);
  ::encode(object, bl);
  ::encode(clones, bl);
  ::encode(missing, bl);
  ENCODE_FINISH(bl);
}

void inconsistent_snapset_wrapper::decode(bufferlist::iterator& bp)
{
  DECODE_START(1, bp);
  ::decode(errors, bp);
  ::decode(object, bp);
  ::decode(clones, bp);
  ::decode(missing, bp);
  DECODE_FINISH(bp);
}

void scrub_ls_arg_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(interval, bl);
  ::encode(get_snapsets, bl);
  ::encode(start_after.name, bl);
  ::encode(start_after.nspace, bl);
  ::encode(start_after.snap, bl);
  ::encode(max_return, bl);
  ENCODE_FINISH(bl);
}

void scrub_ls_arg_t::decode(bufferlist::iterator& bp)
{
  DECODE_START(1, bp);
  ::decode(interval, bp);
  ::decode(get_snapsets, bp);
  ::decode(start_after.name, bp);
  ::decode(start_after.nspace, bp);
  ::decode(start_after.snap, bp);
  ::decode(max_return, bp);
  DECODE_FINISH(bp);
}

void scrub_ls_result_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(interval, bl);
  ::encode(vals, bl);
  ENCODE_FINISH(bl);
}

void scrub_ls_result_t::decode(bufferlist::iterator& bp)
{
  DECODE_START(1, bp);
  ::decode(interval, bp);
  ::decode(vals, bp);
  DECODE_FINISH(bp);
}
