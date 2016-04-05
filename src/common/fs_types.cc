
#include "include/fs_types.h"
#include "common/Formatter.h"
#include "include/ceph_features.h"

void dump(const ceph_file_layout& l, Formatter *f)
{
  f->dump_unsigned("stripe_unit", l.fl_stripe_unit);
  f->dump_unsigned("stripe_count", l.fl_stripe_count);
  f->dump_unsigned("object_size", l.fl_object_size);
  if (l.fl_cas_hash)
    f->dump_unsigned("cas_hash", l.fl_cas_hash);
  if (l.fl_object_stripe_unit)
    f->dump_unsigned("object_stripe_unit", l.fl_object_stripe_unit);
  if (l.fl_pg_pool)
    f->dump_unsigned("pg_pool", l.fl_pg_pool);
}

void dump(const ceph_dir_layout& l, Formatter *f)
{
  f->dump_unsigned("dir_hash", l.dl_dir_hash);
}


// file_layout_t

bool file_layout_t::is_valid() const
{
  /* stripe unit, object size must be non-zero, 64k increment */
  if (!stripe_unit || (stripe_unit & (CEPH_MIN_STRIPE_UNIT-1)))
    return false;
  if (!object_size || (object_size & (CEPH_MIN_STRIPE_UNIT-1)))
    return false;
  /* object size must be a multiple of stripe unit */
  if (object_size < stripe_unit || object_size % stripe_unit)
    return false;
  /* stripe count must be non-zero */
  if (!stripe_count)
    return false;
  return true;
}

void file_layout_t::from_legacy(const ceph_file_layout& fl)
{
  stripe_unit = fl.fl_stripe_unit;
  stripe_count = fl.fl_stripe_count;
  object_size = fl.fl_object_size;
  pool_id = (int32_t)fl.fl_pg_pool;
  // in the legacy encoding, a zeroed structure was the default and
  // would have pool 0 instead of -1.
  if (pool_id == 0 && stripe_unit == 0 && stripe_count == 0 && object_size == 0)
    pool_id = -1;
  pool_ns.clear();
}

void file_layout_t::to_legacy(ceph_file_layout *fl) const
{
  fl->fl_stripe_unit = stripe_unit;
  fl->fl_stripe_count = stripe_count;
  fl->fl_object_size = object_size;
  fl->fl_cas_hash = 0;
  fl->fl_object_stripe_unit = 0;
  fl->fl_unused = 0;
  // in the legacy encoding, pool 0 was undefined.
  if (pool_id >= 0)
    fl->fl_pg_pool = pool_id;
  else
    fl->fl_pg_pool = 0;
}

void file_layout_t::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_FS_FILE_LAYOUT_V2) == 0) {
    ceph_file_layout fl;
    assert((stripe_unit & 0xff) == 0);  // first byte must be 0
    to_legacy(&fl);
    ::encode(fl, bl);
    return;
  }

  ENCODE_START(2, 2, bl);
  ::encode(stripe_unit, bl);
  ::encode(stripe_count, bl);
  ::encode(object_size, bl);
  ::encode(pool_id, bl);
  ::encode(pool_ns, bl);
  ENCODE_FINISH(bl);
}

void file_layout_t::decode(bufferlist::iterator& p)
{
  if (*p == 0) {
    ceph_file_layout fl;
    ::decode(fl, p);
    from_legacy(fl);
    return;
  }
  DECODE_START(2, p);
  ::decode(stripe_unit, p);
  ::decode(stripe_count, p);
  ::decode(object_size, p);
  ::decode(pool_id, p);
  ::decode(pool_ns, p);
  DECODE_FINISH(p);
}

void file_layout_t::dump(Formatter *f) const
{
  f->dump_unsigned("stripe_unit", stripe_unit);
  f->dump_unsigned("stripe_count", stripe_count);
  f->dump_unsigned("object_size", object_size);
  f->dump_unsigned("pool_id", pool_id);
  f->dump_string("pool_ns", pool_ns);
}

void file_layout_t::generate_test_instances(list<file_layout_t*>& o)
{
  o.push_back(new file_layout_t);
  o.push_back(new file_layout_t);
  o.back()->stripe_unit = 4096;
  o.back()->stripe_count = 16;
  o.back()->object_size = 1048576;
  o.back()->pool_id = 3;
  o.back()->pool_ns = "myns";
}
