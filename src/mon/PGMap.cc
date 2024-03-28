// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string.hpp>

#include "include/rados.h"
#include "PGMap.h"

#define dout_subsys ceph_subsys_mon
#include "common/debug.h"
#include "common/Clock.h"
#include "common/Formatter.h"
#include "global/global_context.h"
#include "include/ceph_features.h"
#include "include/stringify.h"

#include "osd/osd_types.h"
#include "osd/OSDMap.h"
#include <boost/range/adaptor/reversed.hpp>

#define dout_context g_ceph_context

using std::list;
using std::make_pair;
using std::map;
using std::pair;
using std::ostream;
using std::ostringstream;
using std::set;
using std::string;
using std::stringstream;
using std::vector;
using std::tuple;

using ceph::bufferlist;
using ceph::fixed_u_to_string;
using ceph::common::cmd_getval;
using ceph::common::cmd_getval_or;
using ceph::common::cmd_putval;

MEMPOOL_DEFINE_OBJECT_FACTORY(PGMapDigest, pgmap_digest, pgmap);
MEMPOOL_DEFINE_OBJECT_FACTORY(PGMap, pgmap, pgmap);
MEMPOOL_DEFINE_OBJECT_FACTORY(PGMap::Incremental, pgmap_inc, pgmap);


// ---------------------
// PGMapDigest

void PGMapDigest::encode(bufferlist& bl, uint64_t features) const
{
  // NOTE: see PGMap::encode_digest
  uint8_t v = 5;
  assert(HAVE_FEATURE(features, SERVER_NAUTILUS));
  ENCODE_START(v, 1, bl);
  encode(num_pg, bl);
  encode(num_pg_active, bl);
  encode(num_pg_unknown, bl);
  encode(num_osd, bl);
  encode(pg_pool_sum, bl, features);
  encode(pg_sum, bl, features);
  encode(osd_sum, bl, features);
  encode(num_pg_by_state, bl);
  encode(num_pg_by_osd, bl);
  encode(num_pg_by_pool, bl);
  encode(osd_last_seq, bl);
  encode(per_pool_sum_delta, bl, features);
  encode(per_pool_sum_deltas_stamps, bl);
  encode(pg_sum_delta, bl, features);
  encode(stamp_delta, bl);
  encode(avail_space_by_rule, bl);
  encode(purged_snaps, bl);
  encode(osd_sum_by_class, bl, features);
  encode(max_raw_used_by_rule, bl);
  ENCODE_FINISH(bl);
}

void PGMapDigest::decode(bufferlist::const_iterator& p)
{
  DECODE_START(4, p);
  assert(struct_v >= 4);
  decode(num_pg, p);
  decode(num_pg_active, p);
  decode(num_pg_unknown, p);
  decode(num_osd, p);
  decode(pg_pool_sum, p);
  decode(pg_sum, p);
  decode(osd_sum, p);
  decode(num_pg_by_state, p);
  decode(num_pg_by_osd, p);
  decode(num_pg_by_pool, p);
  decode(osd_last_seq, p);
  decode(per_pool_sum_delta, p);
  decode(per_pool_sum_deltas_stamps, p);
  decode(pg_sum_delta, p);
  decode(stamp_delta, p);
  decode(avail_space_by_rule, p);
  decode(purged_snaps, p);
  decode(osd_sum_by_class, p);
  max_raw_used_by_rule.clear();
  if (struct_v > 4) {
    decode(max_raw_used_by_rule, p);
  }
  DECODE_FINISH(p);
}

void PGMapDigest::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("num_pg", num_pg);
  f->dump_unsigned("num_pg_active", num_pg_active);
  f->dump_unsigned("num_pg_unknown", num_pg_unknown);
  f->dump_unsigned("num_osd", num_osd);
  f->dump_object("pool_sum", pg_sum);
  f->dump_object("osd_sum", osd_sum);

  f->open_object_section("osd_sum_by_class");
  for (auto& i : osd_sum_by_class) {
    f->dump_object(i.first.c_str(), i.second);
  }
  f->close_section();

  f->open_array_section("pool_stats");
  for (auto& p : pg_pool_sum) {
    f->open_object_section("pool_stat");
    f->dump_int("poolid", p.first);
    auto q = num_pg_by_pool.find(p.first);
    if (q != num_pg_by_pool.end())
      f->dump_unsigned("num_pg", q->second);
    p.second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("osd_stats");
  int i = 0;
  // TODO: this isn't really correct since we can dump non-existent OSDs
  // I dunno what osd_last_seq is set to in that case...
  for (auto& p : osd_last_seq) {
    f->open_object_section("osd_stat");
    f->dump_int("osd", i);
    f->dump_unsigned("seq", p);
    f->close_section();
    ++i;
  }
  f->close_section();
  f->open_array_section("num_pg_by_state");
  for (auto& p : num_pg_by_state) {
    f->open_object_section("count");
    f->dump_string("state", pg_state_string(p.first));
    f->dump_unsigned("num", p.second);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("num_pg_by_osd");
  for (auto& p : num_pg_by_osd) {
    f->open_object_section("count");
    f->dump_unsigned("osd", p.first);
    f->dump_unsigned("num_primary_pg", p.second.primary);
    f->dump_unsigned("num_acting_pg", p.second.acting);
    f->dump_unsigned("num_up_not_acting_pg", p.second.up_not_acting);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("purged_snaps");
  for (auto& j : purged_snaps) {
    f->open_object_section("pool");
    f->dump_int("pool", j.first);
    f->open_object_section("purged_snaps");
    for (auto i = j.second.begin(); i != j.second.end(); ++i) {
      f->open_object_section("interval");
      f->dump_stream("start") << i.get_start();
      f->dump_stream("length") << i.get_len();
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
}

void PGMapDigest::generate_test_instances(list<PGMapDigest*>& ls)
{
  ls.push_back(new PGMapDigest);
}

inline std::string percentify(const float& a) {
  std::stringstream ss;
  if (a < 0.01)
    ss << "0";
  else
    ss << std::fixed << std::setprecision(2) << a;
  return ss.str();
}

void PGMapDigest::print_summary(ceph::Formatter *f, ostream *out) const
{
  if (f)
    f->open_array_section("pgs_by_state");

  // list is descending numeric order (by count)
  std::multimap<int,uint64_t> state_by_count;  // count -> state
  for (auto p = num_pg_by_state.begin();
       p != num_pg_by_state.end();
       ++p) {
    state_by_count.insert(make_pair(p->second, p->first));
  }
  if (f) {
    for (auto p = state_by_count.rbegin();
         p != state_by_count.rend();
         ++p)
    {
      f->open_object_section("pgs_by_state_element");
      f->dump_string("state_name", pg_state_string(p->second));
      f->dump_unsigned("count", p->first);
      f->close_section();
    }
  }
  if (f)
    f->close_section();

  if (f) {
    f->dump_unsigned("num_pgs", num_pg);
    f->dump_unsigned("num_pools", pg_pool_sum.size());
    f->dump_unsigned("num_objects", pg_sum.stats.sum.num_objects);
    f->dump_unsigned("data_bytes", pg_sum.stats.sum.num_bytes);
    f->dump_unsigned("bytes_used", osd_sum.statfs.get_used_raw());
    f->dump_unsigned("bytes_avail", osd_sum.statfs.available);
    f->dump_unsigned("bytes_total", osd_sum.statfs.total);
  } else {
    *out << "    pools:   " << pg_pool_sum.size() << " pools, "
         << num_pg << " pgs\n";
    *out << "    objects: " << si_u_t(pg_sum.stats.sum.num_objects) << " objects, "
         << byte_u_t(pg_sum.stats.sum.num_bytes) << "\n";
    *out << "    usage:   "
         << byte_u_t(osd_sum.statfs.get_used_raw()) << " used, "
         << byte_u_t(osd_sum.statfs.available) << " / "
         << byte_u_t(osd_sum.statfs.total) << " avail\n";
    *out << "    pgs:     ";
  }

  bool pad = false;

  if (num_pg_unknown > 0) {
    float p = (float)num_pg_unknown / (float)num_pg;
    if (f) {
      f->dump_float("unknown_pgs_ratio", p);
    } else {
      char b[20];
      snprintf(b, sizeof(b), "%.3lf", p * 100.0);
      *out << b << "% pgs unknown\n";
      pad = true;
    }
  }

  int num_pg_inactive = num_pg - num_pg_active - num_pg_unknown;
  if (num_pg_inactive > 0) {
    float p = (float)num_pg_inactive / (float)num_pg;
    if (f) {
      f->dump_float("inactive_pgs_ratio", p);
    } else {
      if (pad) {
        *out << "             ";
      }
      char b[20];
      snprintf(b, sizeof(b), "%.3f", p * 100.0);
      *out << b << "% pgs not active\n";
      pad = true;
    }
  }

  list<string> sl;
  overall_recovery_summary(f, &sl);
  if (!f && !sl.empty()) {
    for (auto p = sl.begin(); p != sl.end(); ++p) {
      if (pad) {
        *out << "             ";
      }
      *out << *p << "\n";
      pad = true;
    }
  }
  sl.clear();

  if (!f) {
    unsigned max_width = 1;
    for (auto p = state_by_count.rbegin(); p != state_by_count.rend(); ++p)
    {
      std::stringstream ss;
      ss << p->first;
      max_width = std::max<size_t>(ss.str().size(), max_width);
    }

    for (auto p = state_by_count.rbegin(); p != state_by_count.rend(); ++p)
    {
      if (pad) {
        *out << "             ";
      }
      pad = true;
      out->setf(std::ios::left);
      *out << std::setw(max_width) << p->first
           << " " << pg_state_string(p->second) << "\n";
      out->unsetf(std::ios::left);
    }
  }

  ostringstream ss_rec_io;
  overall_recovery_rate_summary(f, &ss_rec_io);
  ostringstream ss_client_io;
  overall_client_io_rate_summary(f, &ss_client_io);
  ostringstream ss_cache_io;
  overall_cache_io_rate_summary(f, &ss_cache_io);

  if (!f && (ss_client_io.str().length() || ss_rec_io.str().length()
             || ss_cache_io.str().length())) {
    *out << "\n \n";
    *out << "  io:\n";
  }

  if (!f && ss_client_io.str().length())
    *out << "    client:   " << ss_client_io.str() << "\n";
  if (!f && ss_rec_io.str().length())
    *out << "    recovery: " << ss_rec_io.str() << "\n";
  if (!f && ss_cache_io.str().length())
    *out << "    cache:    " << ss_cache_io.str() << "\n";
}

void PGMapDigest::print_oneline_summary(ceph::Formatter *f, ostream *out) const
{
  std::stringstream ss;

  if (f)
    f->open_array_section("num_pg_by_state");
  for (auto p = num_pg_by_state.begin();
       p != num_pg_by_state.end();
       ++p) {
    if (f) {
      f->open_object_section("state");
      f->dump_string("name", pg_state_string(p->first));
      f->dump_unsigned("num", p->second);
      f->close_section();
    }
    if (p != num_pg_by_state.begin())
      ss << ", ";
    ss << p->second << " " << pg_state_string(p->first);
  }
  if (f)
    f->close_section();

  string states = ss.str();
  if (out)
    *out << num_pg << " pgs: "
         << states << "; "
         << byte_u_t(pg_sum.stats.sum.num_bytes) << " data, "
         << byte_u_t(osd_sum.statfs.get_used()) << " used, "
         << byte_u_t(osd_sum.statfs.available) << " / "
         << byte_u_t(osd_sum.statfs.total) << " avail";
  if (f) {
    f->dump_unsigned("num_pgs", num_pg);
    f->dump_unsigned("num_bytes", pg_sum.stats.sum.num_bytes);
    f->dump_int("total_bytes", osd_sum.statfs.total);
    f->dump_int("total_avail_bytes", osd_sum.statfs.available);
    f->dump_int("total_used_bytes", osd_sum.statfs.get_used());
    f->dump_int("total_used_raw_bytes", osd_sum.statfs.get_used_raw());
  }

  // make non-negative; we can get negative values if osds send
  // uncommitted stats and then "go backward" or if they are just
  // buggy/wrong.
  pool_stat_t pos_delta = pg_sum_delta;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_rd ||
      pos_delta.stats.sum.num_wr) {
    if (out)
      *out << "; ";
    if (pos_delta.stats.sum.num_rd) {
      int64_t rd = (pos_delta.stats.sum.num_rd_kb << 10) / (double)stamp_delta;
      if (out)
	*out << byte_u_t(rd) << "/s rd, ";
      if (f)
	f->dump_unsigned("read_bytes_sec", rd);
    }
    if (pos_delta.stats.sum.num_wr) {
      int64_t wr = (pos_delta.stats.sum.num_wr_kb << 10) / (double)stamp_delta;
      if (out)
	*out << byte_u_t(wr) << "/s wr, ";
      if (f)
	f->dump_unsigned("write_bytes_sec", wr);
    }
    int64_t iops = (pos_delta.stats.sum.num_rd + pos_delta.stats.sum.num_wr) / (double)stamp_delta;
    if (out)
      *out << si_u_t(iops) << " op/s";
    if (f)
      f->dump_unsigned("io_sec", iops);
  }

  list<string> sl;
  overall_recovery_summary(f, &sl);
  if (out)
    for (auto p = sl.begin(); p != sl.end(); ++p)
      *out << "; " << *p;
  std::stringstream ssr;
  overall_recovery_rate_summary(f, &ssr);
  if (out && ssr.str().length())
    *out << "; " << ssr.str() << " recovering";
}

void PGMapDigest::get_recovery_stats(
    double *misplaced_ratio,
    double *degraded_ratio,
    double *inactive_pgs_ratio,
    double *unknown_pgs_ratio) const
{
  if (pg_sum.stats.sum.num_objects_degraded &&
      pg_sum.stats.sum.num_object_copies > 0) {
    *degraded_ratio = (double)pg_sum.stats.sum.num_objects_degraded /
      (double)pg_sum.stats.sum.num_object_copies;
  } else {
    *degraded_ratio = 0;
  }
  if (pg_sum.stats.sum.num_objects_misplaced &&
      pg_sum.stats.sum.num_object_copies > 0) {
    *misplaced_ratio = (double)pg_sum.stats.sum.num_objects_misplaced /
      (double)pg_sum.stats.sum.num_object_copies;
  } else {
    *misplaced_ratio = 0;
  }
  if (num_pg > 0) {
    int num_pg_inactive = num_pg - num_pg_active - num_pg_unknown;
    *inactive_pgs_ratio = (double)num_pg_inactive / (double)num_pg;
    *unknown_pgs_ratio = (double)num_pg_unknown / (double)num_pg;
 } else {
    *inactive_pgs_ratio = 0;
    *unknown_pgs_ratio = 0;
  }
}

void PGMapDigest::recovery_summary(ceph::Formatter *f, list<string> *psl,
                             const pool_stat_t& pool_sum) const
{
  if (pool_sum.stats.sum.num_objects_degraded && pool_sum.stats.sum.num_object_copies > 0) {
    double pc = (double)pool_sum.stats.sum.num_objects_degraded /
                (double)pool_sum.stats.sum.num_object_copies * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    if (f) {
      f->dump_unsigned("degraded_objects", pool_sum.stats.sum.num_objects_degraded);
      f->dump_unsigned("degraded_total", pool_sum.stats.sum.num_object_copies);
      f->dump_float("degraded_ratio", pc / 100.0);
    } else {
      ostringstream ss;
      ss << pool_sum.stats.sum.num_objects_degraded
         << "/" << pool_sum.stats.sum.num_object_copies << " objects degraded (" << b << "%)";
      psl->push_back(ss.str());
    }
  }
  if (pool_sum.stats.sum.num_objects_misplaced && pool_sum.stats.sum.num_object_copies > 0) {
    double pc = (double)pool_sum.stats.sum.num_objects_misplaced /
                (double)pool_sum.stats.sum.num_object_copies * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    if (f) {
      f->dump_unsigned("misplaced_objects", pool_sum.stats.sum.num_objects_misplaced);
      f->dump_unsigned("misplaced_total", pool_sum.stats.sum.num_object_copies);
      f->dump_float("misplaced_ratio", pc / 100.0);
    } else {
      ostringstream ss;
      ss << pool_sum.stats.sum.num_objects_misplaced
         << "/" << pool_sum.stats.sum.num_object_copies << " objects misplaced (" << b << "%)";
      psl->push_back(ss.str());
    }
  }
  if (pool_sum.stats.sum.num_objects_unfound && pool_sum.stats.sum.num_objects) {
    double pc = (double)pool_sum.stats.sum.num_objects_unfound /
                (double)pool_sum.stats.sum.num_objects * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    if (f) {
      f->dump_unsigned("unfound_objects", pool_sum.stats.sum.num_objects_unfound);
      f->dump_unsigned("unfound_total", pool_sum.stats.sum.num_objects);
      f->dump_float("unfound_ratio", pc / 100.0);
    } else {
      ostringstream ss;
      ss << pool_sum.stats.sum.num_objects_unfound
         << "/" << pool_sum.stats.sum.num_objects << " objects unfound (" << b << "%)";
      psl->push_back(ss.str());
    }
  }
}

void PGMapDigest::recovery_rate_summary(ceph::Formatter *f, ostream *out,
                                  const pool_stat_t& delta_sum,
                                  utime_t delta_stamp) const
{
  // make non-negative; we can get negative values if osds send
  // uncommitted stats and then "go backward" or if they are just
  // buggy/wrong.
  pool_stat_t pos_delta = delta_sum;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_objects_recovered ||
      pos_delta.stats.sum.num_bytes_recovered ||
      pos_delta.stats.sum.num_keys_recovered) {
    int64_t objps = pos_delta.stats.sum.num_objects_recovered / (double)delta_stamp;
    int64_t bps = pos_delta.stats.sum.num_bytes_recovered / (double)delta_stamp;
    int64_t kps = pos_delta.stats.sum.num_keys_recovered / (double)delta_stamp;
    if (f) {
      f->dump_int("recovering_objects_per_sec", objps);
      f->dump_int("recovering_bytes_per_sec", bps);
      f->dump_int("recovering_keys_per_sec", kps);
      f->dump_int("num_objects_recovered", pos_delta.stats.sum.num_objects_recovered);
      f->dump_int("num_bytes_recovered", pos_delta.stats.sum.num_bytes_recovered);
      f->dump_int("num_keys_recovered", pos_delta.stats.sum.num_keys_recovered);
    } else {
      *out << byte_u_t(bps) << "/s";
      if (pos_delta.stats.sum.num_keys_recovered)
	*out << ", " << si_u_t(kps) << " keys/s";
      *out << ", " << si_u_t(objps) << " objects/s";
    }
  }
}

void PGMapDigest::overall_recovery_rate_summary(ceph::Formatter *f, ostream *out) const
{
  recovery_rate_summary(f, out, pg_sum_delta, stamp_delta);
}

void PGMapDigest::overall_recovery_summary(ceph::Formatter *f, list<string> *psl) const
{
  recovery_summary(f, psl, pg_sum);
}

void PGMapDigest::pool_recovery_rate_summary(ceph::Formatter *f, ostream *out,
                                       uint64_t poolid) const
{
  auto p = per_pool_sum_delta.find(poolid);
  if (p == per_pool_sum_delta.end())
    return;

  auto ts = per_pool_sum_deltas_stamps.find(p->first);
  ceph_assert(ts != per_pool_sum_deltas_stamps.end());
  recovery_rate_summary(f, out, p->second.first, ts->second);
}

void PGMapDigest::pool_recovery_summary(ceph::Formatter *f, list<string> *psl,
                                  uint64_t poolid) const
{
  auto p = pg_pool_sum.find(poolid);
  if (p == pg_pool_sum.end())
    return;

  recovery_summary(f, psl, p->second);
}

void PGMapDigest::client_io_rate_summary(ceph::Formatter *f, ostream *out,
                                   const pool_stat_t& delta_sum,
                                   utime_t delta_stamp) const
{
  pool_stat_t pos_delta = delta_sum;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_rd ||
      pos_delta.stats.sum.num_wr) {
    if (pos_delta.stats.sum.num_rd) {
      int64_t rd = (pos_delta.stats.sum.num_rd_kb << 10) / (double)delta_stamp;
      if (f) {
	f->dump_int("read_bytes_sec", rd);
      } else {
	*out << byte_u_t(rd) << "/s rd, ";
      }
    }
    if (pos_delta.stats.sum.num_wr) {
      int64_t wr = (pos_delta.stats.sum.num_wr_kb << 10) / (double)delta_stamp;
      if (f) {
	f->dump_int("write_bytes_sec", wr);
      } else {
	*out << byte_u_t(wr) << "/s wr, ";
      }
    }
    int64_t iops_rd = pos_delta.stats.sum.num_rd / (double)delta_stamp;
    int64_t iops_wr = pos_delta.stats.sum.num_wr / (double)delta_stamp;
    if (f) {
      f->dump_int("read_op_per_sec", iops_rd);
      f->dump_int("write_op_per_sec", iops_wr);
    } else {
      *out << si_u_t(iops_rd) << " op/s rd, " << si_u_t(iops_wr) << " op/s wr";
    }
  }
}

void PGMapDigest::overall_client_io_rate_summary(ceph::Formatter *f, ostream *out) const
{
  client_io_rate_summary(f, out, pg_sum_delta, stamp_delta);
}

void PGMapDigest::pool_client_io_rate_summary(ceph::Formatter *f, ostream *out,
                                        uint64_t poolid) const
{
  auto p = per_pool_sum_delta.find(poolid);
  if (p == per_pool_sum_delta.end())
    return;

  auto ts = per_pool_sum_deltas_stamps.find(p->first);
  ceph_assert(ts != per_pool_sum_deltas_stamps.end());
  client_io_rate_summary(f, out, p->second.first, ts->second);
}

void PGMapDigest::cache_io_rate_summary(ceph::Formatter *f, ostream *out,
                                  const pool_stat_t& delta_sum,
                                  utime_t delta_stamp) const
{
  pool_stat_t pos_delta = delta_sum;
  pos_delta.floor(0);
  bool have_output = false;

  if (pos_delta.stats.sum.num_flush) {
    int64_t flush = (pos_delta.stats.sum.num_flush_kb << 10) / (double)delta_stamp;
    if (f) {
      f->dump_int("flush_bytes_sec", flush);
    } else {
      *out << byte_u_t(flush) << "/s flush";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_evict) {
    int64_t evict = (pos_delta.stats.sum.num_evict_kb << 10) / (double)delta_stamp;
    if (f) {
      f->dump_int("evict_bytes_sec", evict);
    } else {
      if (have_output)
	*out << ", ";
      *out << byte_u_t(evict) << "/s evict";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_promote) {
    int64_t promote = pos_delta.stats.sum.num_promote / (double)delta_stamp;
    if (f) {
      f->dump_int("promote_op_per_sec", promote);
    } else {
      if (have_output)
	*out << ", ";
      *out << si_u_t(promote) << " op/s promote";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_flush_mode_low) {
    if (f) {
      f->dump_int("num_flush_mode_low", pos_delta.stats.sum.num_flush_mode_low);
    } else {
      if (have_output)
	*out << ", ";
      *out << si_u_t(pos_delta.stats.sum.num_flush_mode_low) << " PGs flushing";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_flush_mode_high) {
    if (f) {
      f->dump_int("num_flush_mode_high", pos_delta.stats.sum.num_flush_mode_high);
    } else {
      if (have_output)
	*out << ", ";
      *out << si_u_t(pos_delta.stats.sum.num_flush_mode_high) << " PGs flushing (high)";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_evict_mode_some) {
    if (f) {
      f->dump_int("num_evict_mode_some", pos_delta.stats.sum.num_evict_mode_some);
    } else {
      if (have_output)
	*out << ", ";
      *out << si_u_t(pos_delta.stats.sum.num_evict_mode_some) << " PGs evicting";
      have_output = true;
    }
  }
  if (pos_delta.stats.sum.num_evict_mode_full) {
    if (f) {
      f->dump_int("num_evict_mode_full", pos_delta.stats.sum.num_evict_mode_full);
    } else {
      if (have_output)
	*out << ", ";
      *out << si_u_t(pos_delta.stats.sum.num_evict_mode_full) << " PGs evicting (full)";
    }
  }
}

void PGMapDigest::overall_cache_io_rate_summary(ceph::Formatter *f, ostream *out) const
{
  cache_io_rate_summary(f, out, pg_sum_delta, stamp_delta);
}

void PGMapDigest::pool_cache_io_rate_summary(ceph::Formatter *f, ostream *out,
                                       uint64_t poolid) const
{
  auto p = per_pool_sum_delta.find(poolid);
  if (p == per_pool_sum_delta.end())
    return;

  auto ts = per_pool_sum_deltas_stamps.find(p->first);
  ceph_assert(ts != per_pool_sum_deltas_stamps.end());
  cache_io_rate_summary(f, out, p->second.first, ts->second);
}

ceph_statfs PGMapDigest::get_statfs(OSDMap &osdmap,
				    std::optional<int64_t> data_pool) const
{
  ceph_statfs statfs;
  bool filter = false;
  object_stat_sum_t sum;

  if (data_pool) {
    auto i = pg_pool_sum.find(*data_pool);
    if (i != pg_pool_sum.end()) {
      sum = i->second.stats.sum;
      filter = true;
    }
  }

  if (filter) {
    statfs.kb_used = (sum.num_bytes >> 10);
    statfs.kb_avail = get_pool_free_space(osdmap, *data_pool) >> 10;
    statfs.num_objects = sum.num_objects;
    statfs.kb = statfs.kb_used + statfs.kb_avail;
  } else {
    // these are in KB.
    statfs.kb = osd_sum.statfs.kb();
    statfs.kb_used = osd_sum.statfs.kb_used_raw();
    statfs.kb_avail = osd_sum.statfs.kb_avail();
    statfs.num_objects = pg_sum.stats.sum.num_objects;
  }

  return statfs;
}

void PGMapDigest::dump_pool_stats_full(
  const OSDMap &osd_map,
  stringstream *ss,
  ceph::Formatter *f,
  bool verbose) const
{
  TextTable tbl;

  if (f) {
    f->open_array_section("pools");
  } else {
    tbl.define_column("POOL", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("ID", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("PGS", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("STORED", TextTable::RIGHT, TextTable::RIGHT);
    if (verbose) {
      tbl.define_column("(DATA)", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("(OMAP)", TextTable::RIGHT, TextTable::RIGHT);
    }
    tbl.define_column("OBJECTS", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("USED", TextTable::RIGHT, TextTable::RIGHT);
    if (verbose) {
      tbl.define_column("(DATA)", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("(OMAP)", TextTable::RIGHT, TextTable::RIGHT);
    }
    tbl.define_column("%USED", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("MAX RAW USED", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("MAX AVAIL", TextTable::RIGHT, TextTable::RIGHT);

    if (verbose) {
      tbl.define_column("QUOTA OBJECTS", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("QUOTA BYTES", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("DIRTY", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("USED COMPR", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("UNDER COMPR", TextTable::RIGHT, TextTable::RIGHT);
    }
  }

  for (auto p = osd_map.get_pools().begin();
       p != osd_map.get_pools().end(); ++p) {
    int64_t pool_id = p->first;
    if ((pool_id < 0) || (pg_pool_sum.count(pool_id) == 0))
      continue;

    const string& pool_name = osd_map.get_pool_name(pool_id);
    auto pool_pg_num = osd_map.get_pg_num(pool_id);
    const pool_stat_t &stat = pg_pool_sum.at(pool_id);

    const pg_pool_t *pool = osd_map.get_pg_pool(pool_id);
    int ruleno = pool->get_crush_rule();
    int64_t avail = get_rule_avail(ruleno);
    if (avail < 0)
      avail = 0;
    if (f) {
      f->open_object_section("pool");
      f->dump_string("name", pool_name);
      f->dump_int("id", pool_id);
      f->open_object_section("stats");
    } else {
      tbl << pool_name
          << pool_id
          << pool_pg_num;
    }
    float raw_used_rate = osd_map.pool_raw_used_rate(pool_id);
    bool per_pool = use_per_pool_stats();
    bool per_pool_omap = use_per_pool_omap_stats();
    int64_t max_used_osd = -1;
    float max_used_rate = 0.0;
    auto it = max_raw_used_by_rule.find(pool->get_crush_rule());
    if (it != max_raw_used_by_rule.end()) {
      max_used_osd = it->second.first;
      max_used_rate = it->second.second;;
    }

    dump_object_stat_sum(tbl, f, stat, avail, raw_used_rate,
                         max_used_osd, max_used_rate,
                         verbose, per_pool,
			 per_pool_omap, pool);
    if (f) {
      f->close_section();  // stats
      f->close_section();  // pool
    } else {
      tbl << TextTable::endrow;
    }
  }
  if (f)
    f->close_section();
  else {
    ceph_assert(ss != nullptr);
    *ss << "--- POOLS ---\n";
    *ss << tbl;
  }
}

void PGMapDigest::dump_cluster_stats(stringstream *ss,
				     ceph::Formatter *f,
				     bool verbose) const
{
  if (f) {
    f->open_object_section("stats");
    f->dump_int("total_bytes", osd_sum.statfs.total);
    f->dump_int("total_avail_bytes", osd_sum.statfs.available);
    f->dump_int("total_used_bytes", osd_sum.statfs.get_used());
    f->dump_int("total_used_raw_bytes", osd_sum.statfs.get_used_raw());
    f->dump_float("total_used_raw_ratio", osd_sum.statfs.get_used_raw_ratio());
    f->dump_unsigned("num_osds", osd_sum.num_osds);
    f->dump_unsigned("num_per_pool_osds", osd_sum.num_per_pool_osds);
    f->dump_unsigned("num_per_pool_omap_osds", osd_sum.num_per_pool_omap_osds);
    f->close_section();
    f->open_object_section("stats_by_class");
    for (auto& i : osd_sum_by_class) {
      f->open_object_section(i.first.c_str());
      f->dump_int("total_bytes", i.second.statfs.total);
      f->dump_int("total_avail_bytes", i.second.statfs.available);
      f->dump_int("total_used_bytes", i.second.statfs.get_used());
      f->dump_int("total_used_raw_bytes", i.second.statfs.get_used_raw());
      f->dump_float("total_used_raw_ratio",
		    i.second.statfs.get_used_raw_ratio());
      f->close_section();
    }
    f->close_section();
  } else {
    ceph_assert(ss != nullptr);
    TextTable tbl;
    tbl.define_column("CLASS", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("SIZE", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("AVAIL", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("USED", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("RAW USED", TextTable::RIGHT, TextTable::RIGHT);
    tbl.define_column("%RAW USED", TextTable::RIGHT, TextTable::RIGHT);


    for (auto& i : osd_sum_by_class) {
      tbl << i.first;
      tbl << stringify(byte_u_t(i.second.statfs.total))
	  << stringify(byte_u_t(i.second.statfs.available))
	  << stringify(byte_u_t(i.second.statfs.get_used()))
	  << stringify(byte_u_t(i.second.statfs.get_used_raw()))
	  << percentify(i.second.statfs.get_used_raw_ratio()*100.0)
	  << TextTable::endrow;
    }
    tbl << "TOTAL";
    tbl << stringify(byte_u_t(osd_sum.statfs.total))
        << stringify(byte_u_t(osd_sum.statfs.available))
        << stringify(byte_u_t(osd_sum.statfs.get_used()))
        << stringify(byte_u_t(osd_sum.statfs.get_used_raw()))
	<< percentify(osd_sum.statfs.get_used_raw_ratio()*100.0)
	<< TextTable::endrow;

    *ss << "--- RAW STORAGE ---\n";
    *ss << tbl;
  }
}

void PGMapDigest::dump_object_stat_sum(
  TextTable &tbl, ceph::Formatter *f,
  const pool_stat_t &pool_stat, uint64_t avail, float raw_used_rate,
  int64_t max_raw_used_osd, float max_raw_used_rate,
  bool verbose, bool per_pool, bool per_pool_omap,
  const pg_pool_t *pool)
{
  const object_stat_sum_t &sum = pool_stat.stats.sum;
  const store_statfs_t statfs = pool_stat.store_stats;

  if (sum.num_object_copies > 0) {
    raw_used_rate *= (float)(sum.num_object_copies - sum.num_objects_degraded) / sum.num_object_copies;
  }

  uint64_t used_data_bytes = pool_stat.get_allocated_data_bytes(per_pool);
  uint64_t used_omap_bytes = pool_stat.get_allocated_omap_bytes(per_pool_omap);
  uint64_t used_bytes = used_data_bytes + used_omap_bytes;

  float used = 0.0;
  // note avail passed in is raw_avail, calc raw_used here.
  if (avail) {
    used = used_bytes;
    used /= used + avail;
  } else if (used_bytes) {
    used = 1.0;
  }
  auto avail_res = raw_used_rate ? avail / raw_used_rate : 0;
  // an approximation for actually stored user data
  auto stored_data_normalized = pool_stat.get_user_data_bytes(
    raw_used_rate, per_pool);
  auto stored_omap_normalized = pool_stat.get_user_omap_bytes(
    raw_used_rate, per_pool_omap);
  auto stored_normalized = stored_data_normalized + stored_omap_normalized;
  // same, amplied by replication or EC
  auto stored_raw = stored_normalized * raw_used_rate;

  if (f) {
    f->dump_int("stored", stored_normalized);
    if (verbose) {
      f->dump_int("stored_data", stored_data_normalized);
      f->dump_int("stored_omap", stored_omap_normalized);
    }
    f->dump_int("objects", sum.num_objects);
    f->dump_int("kb_used", shift_round_up(used_bytes, 10));
    f->dump_int("bytes_used", used_bytes);
    if (verbose) {
      f->dump_int("data_bytes_used", used_data_bytes);
      f->dump_int("omap_bytes_used", used_omap_bytes);
    }
    f->dump_float("percent_used", used);
    f->dump_int("max_raw_used_osd", max_raw_used_osd);
    f->dump_float("max_raw_used_rate", max_raw_used_rate);
    f->dump_unsigned("max_avail", avail_res);
    if (verbose) {
      f->dump_int("quota_objects", pool->quota_max_objects);
      f->dump_int("quota_bytes", pool->quota_max_bytes);
      if (pool->is_tier()) {
        f->dump_int("dirty", sum.num_objects_dirty);
      } else {
        f->dump_int("dirty", 0);
      }
      f->dump_int("rd", sum.num_rd);
      f->dump_int("rd_bytes", sum.num_rd_kb * 1024ull);
      f->dump_int("wr", sum.num_wr);
      f->dump_int("wr_bytes", sum.num_wr_kb * 1024ull);
      f->dump_int("compress_bytes_used", statfs.data_compressed_allocated);
      f->dump_int("compress_under_bytes", statfs.data_compressed_original);
      // Stored by user amplified by replication
      f->dump_int("stored_raw", stored_raw);
      f->dump_unsigned("avail_raw", avail);
    }
  } else {
    stringstream max_raw_used;
    if (max_raw_used_osd >= 0) {
      max_raw_used << "OSD." << max_raw_used_osd << "/" << percentify(max_raw_used_rate * 100) << "%";
    } else {
      max_raw_used << "N/A";
    }

    tbl << stringify(byte_u_t(stored_normalized));
    if (verbose) {
      tbl << stringify(byte_u_t(stored_data_normalized));
      tbl << stringify(byte_u_t(stored_omap_normalized));
    }
    tbl << stringify(si_u_t(sum.num_objects));
    tbl << stringify(byte_u_t(used_bytes));
    if (verbose) {
      tbl << stringify(byte_u_t(used_data_bytes));
      tbl << stringify(byte_u_t(used_omap_bytes));
    }
    tbl << percentify(used*100);
    tbl << max_raw_used.str();
    tbl << stringify(byte_u_t(avail_res));
    if (verbose) {
      if (pool->quota_max_objects == 0)
        tbl << "N/A";
      else
        tbl << stringify(si_u_t(pool->quota_max_objects));
      if (pool->quota_max_bytes == 0)
        tbl << "N/A";
      else
        tbl << stringify(byte_u_t(pool->quota_max_bytes));
      if (pool->is_tier()) {
        tbl << stringify(si_u_t(sum.num_objects_dirty));
      } else {
        tbl << "N/A";
      }
      tbl << stringify(byte_u_t(statfs.data_compressed_allocated));
      tbl << stringify(byte_u_t(statfs.data_compressed_original));
    }
  }
}

int64_t PGMapDigest::get_pool_free_space(const OSDMap &osd_map,
                                         int64_t poolid) const
{
  const pg_pool_t *pool = osd_map.get_pg_pool(poolid);
  int ruleno = pool->get_crush_rule();
  int64_t avail;
  avail = get_rule_avail(ruleno);
  if (avail < 0)
    avail = 0;

  return avail / osd_map.pool_raw_used_rate(poolid);
}

tuple<int64_t, int64_t, double> PGMap::get_rule_stats(const OSDMap& osdmap,
                                                      int ruleno) const
{
  map<int,float> wm;
  int r = osdmap.crush->get_rule_weight_osd_map(ruleno, &wm);
  if (r < 0) {
    return std::make_tuple(r, -1, 0.0);
  }
  if (wm.empty()) {
    return std::make_tuple(0, -1, 0.0);
  }
  float fratio = osdmap.get_full_ratio();

  int64_t min = -1;
  int64_t max_raw_used_osd = -1;
  double max_raw_used_rate = 0.0;
  for (auto p = wm.begin(); p != wm.end(); ++p) {
    auto osd_info = osd_stat.find(p->first);
    if (osd_info != osd_stat.end()) {
      if (osd_info->second.statfs.total == 0 || p->second == 0) {
	// osd must be out, hence its stats have been zeroed
	// (unless we somehow managed to have a disk with size 0...)
	//
	// (p->second == 0), if osd weight is 0, no need to
	// calculate proj below.
	continue;
      }
      auto& statfs = osd_info->second.statfs;
      double unusable = (double)statfs.kb() *
	(1.0 - fratio);
      double avail = std::max(0.0, (double)statfs.kb_avail() - unusable);
      avail *= 1024.0;
      int64_t proj = (int64_t)(avail / (double)p->second);
      if (min < 0 || proj < min) {
	min = proj;
      }
      double raw_used_rate = (double)statfs.kb_used() / statfs.kb();
      if (raw_used_rate > max_raw_used_rate || max_raw_used_osd < 0) {
        max_raw_used_osd = p->first;
        max_raw_used_rate = raw_used_rate;
      }
    } else {
      if (osdmap.is_up(p->first)) {
        // This is a level 4 rather than an error, because we might have
        // only just started, and not received the first stats message yet.
        dout(4) << "OSD " << p->first << " is up, but has no stats" << dendl;
      }
    }
  }
  return std::make_tuple(min, max_raw_used_osd, max_raw_used_rate);
}

void PGMap::update_by_rules_stats(const OSDMap& osdmap)
{
  avail_space_by_rule.clear();
  for (auto p : osdmap.get_pools()) {
    int64_t pool_id = p.first;
    if ((pool_id < 0) || (pg_pool_sum.count(pool_id) == 0))
      continue;
    const pg_pool_t *pool = osdmap.get_pg_pool(pool_id);
    int ruleno = pool->get_crush_rule();
    if (avail_space_by_rule.count(ruleno) == 0) {
      auto [avail, max_used_osd, max_used_rate] = get_rule_stats(osdmap, ruleno);
      avail_space_by_rule[ruleno] = avail;
      max_raw_used_by_rule[ruleno] = std::make_pair(max_used_osd, max_used_rate);
    }
  }
}

// ---------------------
// PGMap

void PGMap::Incremental::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("osdmap_epoch", osdmap_epoch);
  f->dump_unsigned("pg_scan_epoch", pg_scan);

  f->open_array_section("pg_stat_updates");
  for (auto p = pg_stat_updates.begin(); p != pg_stat_updates.end(); ++p) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << p->first;
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_stat_updates");
  for (auto p = osd_stat_updates.begin(); p != osd_stat_updates.end(); ++p) {
    f->open_object_section("osd_stat");
    f->dump_int("osd", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("pool_statfs_updates");
  for (auto p = pool_statfs_updates.begin(); p != pool_statfs_updates.end(); ++p) {
    f->open_object_section("pool_statfs");
    f->dump_stream("poolid/osd") << p->first;
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_stat_removals");
  for (auto p = osd_stat_rm.begin(); p != osd_stat_rm.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();

  f->open_array_section("pg_removals");
  for (auto p = pg_remove.begin(); p != pg_remove.end(); ++p)
    f->dump_stream("pgid") << *p;
  f->close_section();
}

void PGMap::Incremental::generate_test_instances(list<PGMap::Incremental*>& o)
{
  o.push_back(new Incremental);
  o.push_back(new Incremental);
  o.back()->version = 1;
  o.back()->stamp = utime_t(123,345);
  o.push_back(new Incremental);
  o.back()->version = 2;
  o.back()->pg_stat_updates[pg_t(1,2)] = pg_stat_t();
  o.back()->osd_stat_updates[5] = osd_stat_t();
  o.push_back(new Incremental);
  o.back()->version = 3;
  o.back()->osdmap_epoch = 1;
  o.back()->pg_scan = 2;
  o.back()->pg_stat_updates[pg_t(4,5)] = pg_stat_t();
  o.back()->osd_stat_updates[6] = osd_stat_t();
  o.back()->pg_remove.insert(pg_t(1,2));
  o.back()->osd_stat_rm.insert(5);
  o.back()->pool_statfs_updates[std::make_pair(1234,4)] = store_statfs_t();
}

// --

void PGMap::apply_incremental(CephContext *cct, const Incremental& inc)
{
  ceph_assert(inc.version == version+1);
  version++;

  pool_stat_t pg_sum_old = pg_sum;
  mempool::pgmap::unordered_map<int32_t, pool_stat_t> pg_pool_sum_old;
  pg_pool_sum_old = pg_pool_sum;

  for (auto p = inc.pg_stat_updates.begin();
       p != inc.pg_stat_updates.end();
       ++p) {
    const pg_t &update_pg(p->first);
    auto update_pool = update_pg.pool();
    const pg_stat_t &update_stat(p->second);

    auto pg_stat_iter = pg_stat.find(update_pg);
    pool_stat_t &pool_sum_ref = pg_pool_sum[update_pool];
    if (pg_stat_iter == pg_stat.end()) {
      pg_stat.insert(make_pair(update_pg, update_stat));
    } else {
      stat_pg_sub(update_pg, pg_stat_iter->second);
      pool_sum_ref.sub(pg_stat_iter->second);
      pg_stat_iter->second = update_stat;
    }
    stat_pg_add(update_pg, update_stat);
    pool_sum_ref.add(update_stat);
  }

  for (auto p = inc.pool_statfs_updates.begin();
       p != inc.pool_statfs_updates.end();
       ++p) {
    auto update_pool = p->first.first;
    auto update_osd =  p->first.second;
    auto& statfs_inc = p->second;

    auto pool_statfs_iter =
      pool_statfs.find(std::make_pair(update_pool, update_osd));
    if (pg_pool_sum.count(update_pool)) {
      pool_stat_t &pool_sum_ref = pg_pool_sum[update_pool];
      if (pool_statfs_iter == pool_statfs.end()) {
        pool_statfs.emplace(std::make_pair(update_pool, update_osd), statfs_inc);
      } else {
        pool_sum_ref.sub(pool_statfs_iter->second);
        pool_statfs_iter->second = statfs_inc;
      }
      pool_sum_ref.add(statfs_inc);
    }
  }

  for (auto p = inc.get_osd_stat_updates().begin();
       p != inc.get_osd_stat_updates().end();
       ++p) {
    int osd = p->first;
    const osd_stat_t &new_stats(p->second);

    auto t = osd_stat.find(osd);
    if (t == osd_stat.end()) {
      osd_stat.insert(make_pair(osd, new_stats));
    } else {
      stat_osd_sub(t->first, t->second);
      t->second = new_stats;
    }
    stat_osd_add(osd, new_stats);
  }
  set<int64_t> deleted_pools;
  for (auto p = inc.pg_remove.begin();
       p != inc.pg_remove.end();
       ++p) {
    const pg_t &removed_pg(*p);
    auto s = pg_stat.find(removed_pg);
    bool pool_erased = false;
    if (s != pg_stat.end()) {
      pool_erased = stat_pg_sub(removed_pg, s->second);

      // decrease pool stats if pg was removed
      auto pool_stats_it = pg_pool_sum.find(removed_pg.pool());
      if (pool_stats_it != pg_pool_sum.end()) {
        pool_stats_it->second.sub(s->second);
      }

      pg_stat.erase(s);
      if (pool_erased) {
        deleted_pools.insert(removed_pg.pool());
      }
    }
  }

  for (auto p = inc.get_osd_stat_rm().begin();
       p != inc.get_osd_stat_rm().end();
       ++p) {
    auto t = osd_stat.find(*p);
    if (t != osd_stat.end()) {
      stat_osd_sub(t->first, t->second);
      osd_stat.erase(t);
    }
    for (auto i = pool_statfs.begin();  i != pool_statfs.end();) {
      if (i->first.second == *p) {
	pg_pool_sum[i->first.first].sub(i->second);
	i = pool_statfs.erase(i);
      } else {
        ++i;
      }
    }
  }

  // skip calculating delta while sum was not synchronized
  if (!stamp.is_zero() && !pg_sum_old.stats.sum.is_zero()) {
    utime_t delta_t;
    delta_t = inc.stamp;
    delta_t -= stamp;
    // calculate a delta, and average over the last 2 deltas.
    pool_stat_t d = pg_sum;
    d.stats.sub(pg_sum_old.stats);
    pg_sum_deltas.push_back(make_pair(d, delta_t));
    stamp_delta += delta_t;
    pg_sum_delta.stats.add(d.stats);
    auto smooth_intervals =
      cct ? cct->_conf.get_val<uint64_t>("mon_stat_smooth_intervals") : 1;
    while (pg_sum_deltas.size() > smooth_intervals) {
      pg_sum_delta.stats.sub(pg_sum_deltas.front().first.stats);
      stamp_delta -= pg_sum_deltas.front().second;
      pg_sum_deltas.pop_front();
    }
  }
  stamp = inc.stamp;

  update_pool_deltas(cct, inc.stamp, pg_pool_sum_old);

  for (auto p : deleted_pools) {
    if (cct)
      dout(20) << " deleted pool " << p << dendl;
    deleted_pool(p);
  }

  if (inc.osdmap_epoch)
    last_osdmap_epoch = inc.osdmap_epoch;
  if (inc.pg_scan)
    last_pg_scan = inc.pg_scan;
}

void PGMap::calc_stats()
{
  num_pg = 0;
  num_pg_active = 0;
  num_pg_unknown = 0;
  num_osd = 0;
  pg_pool_sum.clear();
  num_pg_by_pool.clear();
  pg_by_osd.clear();
  pg_sum = pool_stat_t();
  osd_sum = osd_stat_t();
  osd_sum_by_class.clear();
  num_pg_by_state.clear();
  num_pg_by_pool_state.clear();
  num_pg_by_osd.clear();

  for (auto p = pg_stat.begin();
       p != pg_stat.end();
       ++p) {
    auto pg = p->first;
    stat_pg_add(pg, p->second);
    pg_pool_sum[pg.pool()].add(p->second);
  }
  for (auto p = pool_statfs.begin();
       p != pool_statfs.end();
       ++p) {
    auto pool = p->first.first;
    pg_pool_sum[pool].add(p->second);
  }
  for (auto p = osd_stat.begin();
       p != osd_stat.end();
       ++p)
    stat_osd_add(p->first, p->second);
}

void PGMap::stat_pg_add(const pg_t &pgid, const pg_stat_t &s,
                        bool sameosds)
{
  auto pool = pgid.pool();
  pg_sum.add(s);

  num_pg++;
  num_pg_by_state[s.state]++;
  num_pg_by_pool_state[pgid.pool()][s.state]++;
  num_pg_by_pool[pool]++;

  if ((s.state & PG_STATE_CREATING) &&
      s.parent_split_bits == 0) {
    creating_pgs.insert(pgid);
    if (s.acting_primary >= 0) {
      creating_pgs_by_osd_epoch[s.acting_primary][s.mapping_epoch].insert(pgid);
    }
  }

  if (s.state & PG_STATE_ACTIVE) {
    ++num_pg_active;
  }
  if (s.state == 0) {
    ++num_pg_unknown;
  }

  if (sameosds)
    return;

  for (auto p = s.blocked_by.begin();
       p != s.blocked_by.end();
       ++p) {
    ++blocked_by_sum[*p];
  }

  for (auto p = s.acting.begin(); p != s.acting.end(); ++p) {
    pg_by_osd[*p].insert(pgid);
    num_pg_by_osd[*p].acting++;
  }
  for (auto p = s.up.begin(); p != s.up.end(); ++p) {
    auto& t = pg_by_osd[*p];
    if (t.find(pgid) == t.end()) {
      t.insert(pgid);
      num_pg_by_osd[*p].up_not_acting++;
    }
  }

  if (s.up_primary >= 0) {
    num_pg_by_osd[s.up_primary].primary++;
  }
}

bool PGMap::stat_pg_sub(const pg_t &pgid, const pg_stat_t &s,
                        bool sameosds)
{
  bool pool_erased = false;
  pg_sum.sub(s);

  num_pg--;
  int end = --num_pg_by_state[s.state];
  ceph_assert(end >= 0);
  if (end == 0)
    num_pg_by_state.erase(s.state);
  if (--num_pg_by_pool_state[pgid.pool()][s.state] == 0) {
    num_pg_by_pool_state[pgid.pool()].erase(s.state);
  }
  end = --num_pg_by_pool[pgid.pool()];
  if (end == 0) {
    pool_erased = true;
  }

  if ((s.state & PG_STATE_CREATING) &&
      s.parent_split_bits == 0) {
    creating_pgs.erase(pgid);
    if (s.acting_primary >= 0) {
      map<epoch_t,set<pg_t> >& r = creating_pgs_by_osd_epoch[s.acting_primary];
      r[s.mapping_epoch].erase(pgid);
      if (r[s.mapping_epoch].empty())
	r.erase(s.mapping_epoch);
      if (r.empty())
	creating_pgs_by_osd_epoch.erase(s.acting_primary);
    }
  }

  if (s.state & PG_STATE_ACTIVE) {
    --num_pg_active;
  }
  if (s.state == 0) {
    --num_pg_unknown;
  }

  if (sameosds)
    return pool_erased;

  for (auto p = s.blocked_by.begin();
       p != s.blocked_by.end();
       ++p) {
    auto q = blocked_by_sum.find(*p);
    ceph_assert(q != blocked_by_sum.end());
    --q->second;
    if (q->second == 0)
      blocked_by_sum.erase(q);
  }

  set<int32_t> actingset;
  for (auto p = s.acting.begin(); p != s.acting.end(); ++p) {
    actingset.insert(*p);
    auto& oset = pg_by_osd[*p];
    oset.erase(pgid);
    if (oset.empty())
      pg_by_osd.erase(*p);
    auto it = num_pg_by_osd.find(*p);
    if (it != num_pg_by_osd.end() && it->second.acting > 0)
      it->second.acting--;
  }
  for (auto p = s.up.begin(); p != s.up.end(); ++p) {
    auto& oset = pg_by_osd[*p];
    oset.erase(pgid);
    if (oset.empty())
      pg_by_osd.erase(*p);
    if (actingset.count(*p))
      continue;
    auto it = num_pg_by_osd.find(*p);
    if (it != num_pg_by_osd.end() && it->second.up_not_acting > 0)
      it->second.up_not_acting--;
  }

  if (s.up_primary >= 0) {
    auto it = num_pg_by_osd.find(s.up_primary);
    if (it != num_pg_by_osd.end() && it->second.primary > 0)
      it->second.primary--;
  }
  return pool_erased;
}

void PGMap::calc_purged_snaps()
{
  purged_snaps.clear();
  set<int64_t> unknown;
  for (auto& i : pg_stat) {
    if (i.second.state == 0) {
      unknown.insert(i.first.pool());
      purged_snaps.erase(i.first.pool());
      continue;
    } else if (unknown.count(i.first.pool())) {
      continue;
    }
    auto j = purged_snaps.find(i.first.pool());
    if (j == purged_snaps.end()) {
      // base case
      purged_snaps[i.first.pool()] = i.second.purged_snaps;
    } else {
      j->second.intersection_of(i.second.purged_snaps);
    }
  }
}

void PGMap::calc_osd_sum_by_class(const OSDMap& osdmap)
{
  osd_sum_by_class.clear();
  for (auto& i : osd_stat) {
    const char *class_name = osdmap.crush->get_item_class(i.first);
    if (class_name) {
      osd_sum_by_class[class_name].add(i.second);
    }
  }
}

void PGMap::stat_osd_add(int osd, const osd_stat_t &s)
{
  num_osd++;
  osd_sum.add(s);
  if (osd >= (int)osd_last_seq.size()) {
    osd_last_seq.resize(osd + 1);
  }
  osd_last_seq[osd] = s.seq;
}

void PGMap::stat_osd_sub(int osd, const osd_stat_t &s)
{
  num_osd--;
  osd_sum.sub(s);
  ceph_assert(osd < (int)osd_last_seq.size());
  osd_last_seq[osd] = 0;
}

void PGMap::encode_digest(const OSDMap& osdmap,
			  bufferlist& bl, uint64_t features)
{
  update_by_rules_stats(osdmap);
  calc_osd_sum_by_class(osdmap);
  calc_purged_snaps();
  PGMapDigest::encode(bl, features);
}

void PGMap::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(8, 8, bl);
  encode(version, bl);
  encode(pg_stat, bl);
  encode(osd_stat, bl, features);
  encode(last_osdmap_epoch, bl);
  encode(last_pg_scan, bl);
  encode(stamp, bl);
  encode(pool_statfs, bl, features);
  ENCODE_FINISH(bl);
}

void PGMap::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(8, bl);
  decode(version, bl);
  decode(pg_stat, bl);
  decode(osd_stat, bl);
  decode(last_osdmap_epoch, bl);
  decode(last_pg_scan, bl);
  decode(stamp, bl);
  decode(pool_statfs, bl);
  DECODE_FINISH(bl);

  calc_stats();
}

void PGMap::dump(ceph::Formatter *f, bool with_net) const
{
  dump_basic(f);
  dump_pg_stats(f, false);
  dump_pool_stats(f);
  dump_osd_stats(f, with_net);
}

void PGMap::dump_basic(ceph::Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("last_osdmap_epoch", last_osdmap_epoch);
  f->dump_unsigned("last_pg_scan", last_pg_scan);

  f->open_object_section("pg_stats_sum");
  pg_sum.dump(f);
  f->close_section();

  f->open_object_section("osd_stats_sum");
  osd_sum.dump(f);
  f->close_section();

  dump_delta(f);
}

void PGMap::dump_delta(ceph::Formatter *f) const
{
  f->open_object_section("pg_stats_delta");
  pg_sum_delta.dump(f);
  f->dump_stream("stamp_delta") << stamp_delta;
  f->close_section();
}

void PGMap::dump_pg_stats(ceph::Formatter *f, bool brief) const
{
  f->open_array_section("pg_stats");
  for (auto i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << i->first;
    if (brief)
      i->second.dump_brief(f);
    else
      i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_pg_progress(ceph::Formatter *f) const
{
  f->open_object_section("pgs");
  for (auto& i : pg_stat) {
    std::string n = stringify(i.first);
    f->open_object_section(n.c_str());
    f->dump_int("num_bytes_recovered", i.second.stats.sum.num_bytes_recovered);
    f->dump_int("num_bytes", i.second.stats.sum.num_bytes);
    f->dump_unsigned("reported_epoch", i.second.reported_epoch);
    f->dump_string("state", pg_state_string(i.second.state));
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_pool_stats(ceph::Formatter *f) const
{
  f->open_array_section("pool_stats");
  for (auto p = pg_pool_sum.begin();
       p != pg_pool_sum.end();
       ++p) {
    f->open_object_section("pool_stat");
    f->dump_int("poolid", p->first);
    auto q = num_pg_by_pool.find(p->first);
    if (q != num_pg_by_pool.end())
      f->dump_unsigned("num_pg", q->second);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_osd_stats(ceph::Formatter *f, bool with_net) const
{
  f->open_array_section("osd_stats");
  for (auto q = osd_stat.begin();
       q != osd_stat.end();
       ++q) {
    f->open_object_section("osd_stat");
    f->dump_int("osd", q->first);
    q->second.dump(f, with_net);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("pool_statfs");
  for (auto& p : pool_statfs) {
    f->open_object_section("item");
    f->dump_int("poolid", p.first.first);
    f->dump_int("osd", p.first.second);
    p.second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_osd_ping_times(ceph::Formatter *f) const
{
  f->open_array_section("osd_ping_times");
  for (const auto& [osd, stat] : osd_stat) {
    f->open_object_section("osd_ping_time");
    f->dump_int("osd", osd);
    stat.dump_ping_time(f);
    f->close_section();
  }
  f->close_section();
}

// note: dump_pg_stats_plain() is static
void PGMap::dump_pg_stats_plain(
  ostream& ss,
  const mempool::pgmap::unordered_map<pg_t, pg_stat_t>& pg_stats,
  bool brief)
{
  TextTable tab;

  if (brief){
    tab.define_column("PG_STAT", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("STATE", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UP", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UP_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("ACTING", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("ACTING_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
  }
  else {
    tab.define_column("PG_STAT", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISSING_ON_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DEGRADED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISPLACED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UNFOUND", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("BYTES", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("OMAP_BYTES*", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("OMAP_KEYS*", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LOG", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LOG_DUPS", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DISK_LOG", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("STATE", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("STATE_STAMP", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("VERSION", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("REPORTED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UP", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UP_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("ACTING", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("ACTING_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LAST_SCRUB", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("SCRUB_STAMP", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LAST_DEEP_SCRUB", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DEEP_SCRUB_STAMP", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("SNAPTRIMQ_LEN", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LAST_SCRUB_DURATION", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("SCRUB_SCHEDULING", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("OBJECTS_SCRUBBED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("OBJECTS_TRIMMED", TextTable::LEFT, TextTable::RIGHT);
  }

  for (const auto& [pg, st] : pg_stats) {
    if (brief) {
      tab << pg
          << pg_state_string(st.state)
          << st.up
          << st.up_primary
          << st.acting
          << st.acting_primary
          << TextTable::endrow;
    } else {
      ostringstream reported;
      reported << st.reported_epoch << ":" << st.reported_seq;

      tab << pg
          << st.stats.sum.num_objects
          << st.stats.sum.num_objects_missing_on_primary
          << st.stats.sum.num_objects_degraded
          << st.stats.sum.num_objects_misplaced
          << st.stats.sum.num_objects_unfound
          << st.stats.sum.num_bytes
          << st.stats.sum.num_omap_bytes
          << st.stats.sum.num_omap_keys
          << st.log_size
          << st.log_dups_size
          << st.ondisk_log_size
          << pg_state_string(st.state)
          << st.last_change
          << st.version
          << reported.str()
          << pg_vector_string(st.up)
          << st.up_primary
          << pg_vector_string(st.acting)
          << st.acting_primary
          << st.last_scrub
          << st.last_scrub_stamp
          << st.last_deep_scrub
          << st.last_deep_scrub_stamp
          << st.snaptrimq_len
          << st.last_scrub_duration
          << st.dump_scrub_schedule()
          << st.objects_scrubbed
          << st.objects_trimmed
          << TextTable::endrow;
    }
  }

  ss << tab;
}

void PGMap::dump(ostream& ss) const
{
  dump_basic(ss);
  dump_pg_stats(ss, false);
  dump_pool_stats(ss, false);
  dump_pg_sum_stats(ss, false);
  dump_osd_stats(ss);
}

void PGMap::dump_basic(ostream& ss) const
{
  ss << "version " << version << std::endl;
  ss << "stamp " << stamp << std::endl;
  ss << "last_osdmap_epoch " << last_osdmap_epoch << std::endl;
  ss << "last_pg_scan " << last_pg_scan << std::endl;
}

void PGMap::dump_pg_stats(ostream& ss, bool brief) const
{
  dump_pg_stats_plain(ss, pg_stat, brief);
}

void PGMap::dump_pool_stats(ostream& ss, bool header) const
{
  TextTable tab;

  if (header) {
    tab.define_column("POOLID", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISSING_ON_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DEGRADED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISPLACED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UNFOUND", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("BYTES", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("OMAP_BYTES*", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("OMAP_KEYS*", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LOG", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DISK_LOG", TextTable::LEFT, TextTable::RIGHT);
  } else {
    tab.define_column("", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
  }

  for (auto p = pg_pool_sum.begin();
       p != pg_pool_sum.end();
       ++p) {
    tab << p->first
        << p->second.stats.sum.num_objects
        << p->second.stats.sum.num_objects_missing_on_primary
        << p->second.stats.sum.num_objects_degraded
        << p->second.stats.sum.num_objects_misplaced
        << p->second.stats.sum.num_objects_unfound
        << p->second.stats.sum.num_bytes
        << p->second.stats.sum.num_omap_bytes
        << p->second.stats.sum.num_omap_keys
        << p->second.log_size
        << p->second.ondisk_log_size
        << TextTable::endrow;
  }

  ss << tab;
}

void PGMap::dump_pg_sum_stats(ostream& ss, bool header) const
{
  TextTable tab;

  if (header) {
    tab.define_column("PG_STAT", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISSING_ON_PRIMARY", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DEGRADED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("MISPLACED", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("UNFOUND", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("BYTES", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("OMAP_BYTES*", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("OMAP_KEYS*", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("LOG", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("DISK_LOG", TextTable::LEFT, TextTable::RIGHT);
  } else {
    tab.define_column("", TextTable::LEFT, TextTable::LEFT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
    tab.define_column("", TextTable::LEFT, TextTable::RIGHT);
  };

  tab << "sum"
      << pg_sum.stats.sum.num_objects
      << pg_sum.stats.sum.num_objects_missing_on_primary
      << pg_sum.stats.sum.num_objects_degraded
      << pg_sum.stats.sum.num_objects_misplaced
      << pg_sum.stats.sum.num_objects_unfound
      << pg_sum.stats.sum.num_bytes
      << pg_sum.stats.sum.num_omap_bytes
      << pg_sum.stats.sum.num_omap_keys
      << pg_sum.log_size
      << pg_sum.ondisk_log_size
      << TextTable::endrow;

  ss << tab;
}

void PGMap::dump_osd_stats(ostream& ss) const
{
  TextTable tab;

  tab.define_column("OSD_STAT", TextTable::LEFT, TextTable::LEFT);
  tab.define_column("USED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("AVAIL", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("USED_RAW", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("TOTAL", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("HB_PEERS", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("PG_SUM", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("PRIMARY_PG_SUM", TextTable::LEFT, TextTable::RIGHT);

  for (auto p = osd_stat.begin();
       p != osd_stat.end();
       ++p) {
    tab << p->first
        << byte_u_t(p->second.statfs.get_used())
        << byte_u_t(p->second.statfs.available)
        << byte_u_t(p->second.statfs.get_used_raw())
        << byte_u_t(p->second.statfs.total)
        << p->second.hb_peers
        << get_num_pg_by_osd(p->first)
        << get_num_primary_pg_by_osd(p->first)
        << TextTable::endrow;
  }

  tab << "sum"
      << byte_u_t(osd_sum.statfs.get_used())
      << byte_u_t(osd_sum.statfs.available)
      << byte_u_t(osd_sum.statfs.get_used_raw())
      << byte_u_t(osd_sum.statfs.total)
      << TextTable::endrow;

  ss << tab;
}

void PGMap::dump_osd_sum_stats(ostream& ss) const
{
  TextTable tab;

  tab.define_column("OSD_STAT", TextTable::LEFT, TextTable::LEFT);
  tab.define_column("USED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("AVAIL", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("USED_RAW", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("TOTAL", TextTable::LEFT, TextTable::RIGHT);

  tab << "sum"
      << byte_u_t(osd_sum.statfs.get_used())
      << byte_u_t(osd_sum.statfs.available)
      << byte_u_t(osd_sum.statfs.get_used_raw())
      << byte_u_t(osd_sum.statfs.total)
      << TextTable::endrow;

  ss << tab;
}

void PGMap::get_stuck_stats(
  int types, const utime_t cutoff,
  mempool::pgmap::unordered_map<pg_t, pg_stat_t>& stuck_pgs) const
{
  ceph_assert(types != 0);
  for (auto i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    utime_t val = cutoff; // don't care about >= cutoff so that is infinity

    if ((types & STUCK_INACTIVE) && !(i->second.state & PG_STATE_ACTIVE)) {
      if (i->second.last_active < val)
	val = i->second.last_active;
    }

    if ((types & STUCK_UNCLEAN) && !(i->second.state & PG_STATE_CLEAN)) {
      if (i->second.last_clean < val)
	val = i->second.last_clean;
    }

    if ((types & STUCK_DEGRADED) && (i->second.state & PG_STATE_DEGRADED)) {
      if (i->second.last_undegraded < val)
	val = i->second.last_undegraded;
    }

    if ((types & STUCK_UNDERSIZED) && (i->second.state & PG_STATE_UNDERSIZED)) {
      if (i->second.last_fullsized < val)
	val = i->second.last_fullsized;
    }

    if ((types & STUCK_STALE) && (i->second.state & PG_STATE_STALE)) {
      if (i->second.last_unstale < val)
	val = i->second.last_unstale;
    }

    if ((types & STUCK_PEERING) && (i->second.state & PG_STATE_PEERING)) {
      if (i->second.last_peered < val)
	val = i->second.last_peered;
    }
    // val is now the earliest any of the requested stuck states began
    if (val < cutoff) {
      stuck_pgs[i->first] = i->second;
    }
  }
}

void PGMap::dump_stuck(ceph::Formatter *f, int types, utime_t cutoff) const
{
  mempool::pgmap::unordered_map<pg_t, pg_stat_t> stuck_pg_stats;
  get_stuck_stats(types, cutoff, stuck_pg_stats);
  f->open_array_section("stuck_pg_stats");
  for (auto i = stuck_pg_stats.begin();
       i != stuck_pg_stats.end();
       ++i) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << i->first;
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_stuck_plain(ostream& ss, int types, utime_t cutoff) const
{
  mempool::pgmap::unordered_map<pg_t, pg_stat_t> stuck_pg_stats;
  get_stuck_stats(types, cutoff, stuck_pg_stats);
  if (!stuck_pg_stats.empty())
    dump_pg_stats_plain(ss, stuck_pg_stats, true);
}

int PGMap::dump_stuck_pg_stats(
  stringstream &ds,
  ceph::Formatter *f,
  int threshold,
  vector<string>& args) const
{
  int stuck_types = 0;

  for (auto i = args.begin(); i != args.end(); ++i) {
    if (*i == "inactive")
      stuck_types |= PGMap::STUCK_INACTIVE;
    else if (*i == "unclean")
      stuck_types |= PGMap::STUCK_UNCLEAN;
    else if (*i == "undersized")
      stuck_types |= PGMap::STUCK_UNDERSIZED;
    else if (*i == "degraded")
      stuck_types |= PGMap::STUCK_DEGRADED;
    else if (*i == "stale")
      stuck_types |= PGMap::STUCK_STALE;
    else if (*i == "peering")
      stuck_types |= PGMap::STUCK_PEERING;
    else {
      ds << "Unknown type: " << *i << std::endl;
      return -EINVAL;
    }
  }

  utime_t now(ceph_clock_now());
  utime_t cutoff = now - utime_t(threshold, 0);

  if (!f) {
    dump_stuck_plain(ds, stuck_types, cutoff);
  } else {
    dump_stuck(f, stuck_types, cutoff);
    f->flush(ds);
  }

  return 0;
}

void PGMap::dump_osd_perf_stats(ceph::Formatter *f) const
{
  f->open_array_section("osd_perf_infos");
  for (auto i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    f->open_object_section("osd");
    f->dump_int("id", i->first);
    {
      f->open_object_section("perf_stats");
      i->second.os_perf_stat.dump(f);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}
void PGMap::print_osd_perf_stats(std::ostream *ss) const
{
  TextTable tab;
  tab.define_column("osd", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("commit_latency(ms)", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("apply_latency(ms)", TextTable::LEFT, TextTable::RIGHT);
  for (auto i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    tab << i->first;
    tab << i->second.os_perf_stat.os_commit_latency_ns / 1000000ull;
    tab << i->second.os_perf_stat.os_apply_latency_ns / 1000000ull;
    tab << TextTable::endrow;
  }
  (*ss) << tab;
}

void PGMap::dump_osd_blocked_by_stats(ceph::Formatter *f) const
{
  f->open_array_section("osd_blocked_by_infos");
  for (auto i = blocked_by_sum.begin();
       i != blocked_by_sum.end();
       ++i) {
    f->open_object_section("osd");
    f->dump_int("id", i->first);
    f->dump_int("num_blocked", i->second);
    f->close_section();
  }
  f->close_section();
}
void PGMap::print_osd_blocked_by_stats(std::ostream *ss) const
{
  TextTable tab;
  tab.define_column("osd", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("num_blocked", TextTable::LEFT, TextTable::RIGHT);
  for (auto i = blocked_by_sum.begin();
       i != blocked_by_sum.end();
       ++i) {
    tab << i->first;
    tab << i->second;
    tab << TextTable::endrow;
  }
  (*ss) << tab;
}


/**
 * update aggregated delta
 *
 * @param cct               ceph context
 * @param ts                Timestamp for the stats being delta'ed
 * @param old_pool_sum      Previous stats sum
 * @param last_ts           Last timestamp for pool
 * @param result_pool_sum   Resulting stats
 * @param result_pool_delta Resulting pool delta
 * @param result_ts_delta   Resulting timestamp delta
 * @param delta_avg_list    List of last N computed deltas, used to average
 */
void PGMap::update_delta(
  CephContext *cct,
  const utime_t ts,
  const pool_stat_t& old_pool_sum,
  utime_t *last_ts,
  const pool_stat_t& current_pool_sum,
  pool_stat_t *result_pool_delta,
  utime_t *result_ts_delta,
  mempool::pgmap::list<pair<pool_stat_t,utime_t> > *delta_avg_list)
{
  /* @p ts is the timestamp we want to associate with the data
   * in @p old_pool_sum, and on which we will base ourselves to
   * calculate the delta, stored in 'delta_t'.
   */
  utime_t delta_t;
  delta_t = ts;         // start with the provided timestamp
  delta_t -= *last_ts;  // take the last timestamp we saw
  *last_ts = ts;        // @p ts becomes the last timestamp we saw

  // adjust delta_t, quick start if there is no update in a long period
  delta_t = std::min(delta_t,
                    utime_t(2 * (cct ? cct->_conf->mon_delta_reset_interval : 10), 0));

  // calculate a delta, and average over the last 6 deltas by default.
  /* start by taking a copy of our current @p result_pool_sum, and by
   * taking out the stats from @p old_pool_sum.  This generates a stats
   * delta.  Stash this stats delta in @p delta_avg_list, along with the
   * timestamp delta for these results.
   */
  pool_stat_t d = current_pool_sum;
  d.stats.sub(old_pool_sum.stats);

  /* Aggregate current delta, and take out the last seen delta (if any) to
   * average it out.
   * Skip calculating delta while sum was not synchronized.
   */
  if(!old_pool_sum.stats.sum.is_zero()) {
    delta_avg_list->push_back(make_pair(d,delta_t));
    *result_ts_delta += delta_t;
    result_pool_delta->stats.add(d.stats);
  }
  size_t s = cct ? cct->_conf.get_val<uint64_t>("mon_stat_smooth_intervals") : 1;
  while (delta_avg_list->size() > s) {
    result_pool_delta->stats.sub(delta_avg_list->front().first.stats);
    *result_ts_delta -= delta_avg_list->front().second;
    delta_avg_list->pop_front();
  }
}

/**
 * Update a given pool's deltas
 *
 * @param cct           Ceph Context
 * @param ts            Timestamp for the stats being delta'ed
 * @param pool          Pool's id
 * @param old_pool_sum  Previous stats sum
 */
void PGMap::update_one_pool_delta(
  CephContext *cct,
  const utime_t ts,
  const int64_t pool,
  const pool_stat_t& old_pool_sum)
{
  if (per_pool_sum_deltas.count(pool) == 0) {
    ceph_assert(per_pool_sum_deltas_stamps.count(pool) == 0);
    ceph_assert(per_pool_sum_delta.count(pool) == 0);
  }

  auto& sum_delta = per_pool_sum_delta[pool];

  update_delta(cct, ts, old_pool_sum, &sum_delta.second, pg_pool_sum[pool],
               &sum_delta.first, &per_pool_sum_deltas_stamps[pool],
               &per_pool_sum_deltas[pool]);
}

/**
 * Update pools' deltas
 *
 * @param cct               CephContext
 * @param ts                Timestamp for the stats being delta'ed
 * @param pg_pool_sum_old   Map of pool stats for delta calcs.
 */
void PGMap::update_pool_deltas(
  CephContext *cct, const utime_t ts,
  const mempool::pgmap::unordered_map<int32_t,pool_stat_t>& pg_pool_sum_old)
{
  for (auto it = pg_pool_sum_old.begin();
       it != pg_pool_sum_old.end(); ++it) {
    update_one_pool_delta(cct, ts, it->first, it->second);
  }
}

void PGMap::clear_delta()
{
  pg_sum_delta = pool_stat_t();
  pg_sum_deltas.clear();
  stamp_delta = utime_t();
}

void PGMap::generate_test_instances(list<PGMap*>& o)
{
  o.push_back(new PGMap);
  list<Incremental*> inc;
  Incremental::generate_test_instances(inc);
  delete inc.front();
  inc.pop_front();
  while (!inc.empty()) {
    PGMap *pmp = new PGMap();
    *pmp = *o.back();
    o.push_back(pmp);
    o.back()->apply_incremental(NULL, *inc.front());
    delete inc.front();
    inc.pop_front();
  }
}

void PGMap::get_filtered_pg_stats(uint64_t state, int64_t poolid, int64_t osdid,
                                  bool primary, set<pg_t>& pgs) const
{
  for (auto i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    if ((poolid >= 0) && (poolid != i->first.pool()))
      continue;
    if ((osdid >= 0) && !(i->second.is_acting_osd(osdid,primary)))
      continue;
    if (state == (uint64_t)-1 ||                 // "all"
	(i->second.state & state) ||             // matches a state bit
	(state == 0 && i->second.state == 0)) {  // matches "unknown" (== 0)
      pgs.insert(i->first);
    }
  }
}

void PGMap::dump_filtered_pg_stats(ceph::Formatter *f, set<pg_t>& pgs) const
{
  f->open_array_section("pg_stats");
  for (auto i = pgs.begin(); i != pgs.end(); ++i) {
    const pg_stat_t& st = pg_stat.at(*i);
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << *i;
    st.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_filtered_pg_stats(ostream& ss, set<pg_t>& pgs) const
{
  TextTable tab;
  utime_t now = ceph_clock_now();

  tab.define_column("PG", TextTable::LEFT, TextTable::LEFT);
  tab.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("DEGRADED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("MISPLACED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("UNFOUND", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("BYTES", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("OMAP_BYTES*", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("OMAP_KEYS*", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("LOG", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("LOG_DUPS", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("STATE", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("SINCE", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("VERSION", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("REPORTED", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("UP", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("ACTING", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("SCRUB_STAMP", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("DEEP_SCRUB_STAMP", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("LAST_SCRUB_DURATION", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("SCRUB_SCHEDULING", TextTable::LEFT, TextTable::LEFT);

  for (auto i = pgs.begin(); i != pgs.end(); ++i) {
    const pg_stat_t& st = pg_stat.at(*i);

    ostringstream reported;
    reported << st.reported_epoch << ":" << st.reported_seq;

    ostringstream upstr, actingstr;
    upstr << pg_vector_string(st.up) << 'p' << st.up_primary;
    actingstr << pg_vector_string(st.acting) << 'p' << st.acting_primary;
    tab << *i
        << st.stats.sum.num_objects
        << st.stats.sum.num_objects_degraded
        << st.stats.sum.num_objects_misplaced
        << st.stats.sum.num_objects_unfound
        << st.stats.sum.num_bytes
        << st.stats.sum.num_omap_bytes
        << st.stats.sum.num_omap_keys
        << st.log_size
        << st.log_dups_size
        << pg_state_string(st.state)
        << utimespan_str(now - st.last_change)
        << st.version
        << reported.str()
        << upstr.str()
        << actingstr.str()
        << st.last_scrub_stamp
        << st.last_deep_scrub_stamp
        << st.last_scrub_duration
        << st.dump_scrub_schedule()
      << TextTable::endrow;
  }

  ss << tab;
}

void PGMap::dump_pool_stats_and_io_rate(int64_t poolid, const OSDMap &osd_map,
                                        ceph::Formatter *f,
                                        stringstream *rs) const {
  const string& pool_name = osd_map.get_pool_name(poolid);
  if (f) {
    f->open_object_section("pool");
    f->dump_string("pool_name", pool_name.c_str());
    f->dump_int("pool_id", poolid);
    f->open_object_section("recovery");
  }
  list<string> sl;
  stringstream tss;
  pool_recovery_summary(f, &sl, poolid);
  if (!f && !sl.empty()) {
    for (auto &p : sl)
      tss << "  " << p << "\n";
  }
  if (f) {
    f->close_section(); // object section recovery
    f->open_object_section("recovery_rate");
  }
  ostringstream rss;
  pool_recovery_rate_summary(f, &rss, poolid);
  if (!f && !rss.str().empty())
    tss << "  recovery io " << rss.str() << "\n";
  if (f) {
    f->close_section(); // object section recovery_rate
    f->open_object_section("client_io_rate");
  }
  rss.clear();
  rss.str("");
  pool_client_io_rate_summary(f, &rss, poolid);
  if (!f && !rss.str().empty())
    tss << "  client io " << rss.str() << "\n";
  // dump cache tier IO rate for cache pool
  const pg_pool_t *pool = osd_map.get_pg_pool(poolid);
  if (pool->is_tier()) {
    if (f) {
      f->close_section(); // object section client_io_rate
      f->open_object_section("cache_io_rate");
    }
    rss.clear();
    rss.str("");
    pool_cache_io_rate_summary(f, &rss, poolid);
    if (!f && !rss.str().empty())
      tss << "  cache tier io " << rss.str() << "\n";
  }
  if (f) {
    f->close_section(); // object section cache_io_rate
    f->close_section(); // object section pool
  } else {
    *rs << "pool " << pool_name << " id " << poolid << "\n";
    if (!tss.str().empty())
      *rs << tss.str() << "\n";
    else
      *rs << "  nothing is going on\n\n";
  }
}

// Get crush parentage for an osd (skip root)
set<std::string> PGMap::osd_parentage(const OSDMap& osdmap, int id) const
{
  set<std::string> reporters_by_subtree;
  auto reporter_subtree_level = g_conf().get_val<string>("mon_osd_reporter_subtree_level");

  auto loc = osdmap.crush->get_full_location(id);
  for (auto& [parent_bucket_type, parent_id] : loc) {
    // Should we show the root?  Might not be too informative like "default"
    if (parent_bucket_type != "root" &&
        parent_bucket_type != reporter_subtree_level) {
      reporters_by_subtree.insert(parent_id);
    }
  }
  return reporters_by_subtree;
}

void PGMap::get_health_checks(
  CephContext *cct,
  const OSDMap& osdmap,
  health_check_map_t *checks) const
{
  utime_t now = ceph_clock_now();
  const auto max = cct->_conf.get_val<uint64_t>("mon_health_max_detail");
  const auto& pools = osdmap.get_pools();

  typedef enum pg_consequence_t {
    UNAVAILABLE = 1,   // Client IO to the pool may block
    DEGRADED = 2,      // Fewer than the requested number of replicas are present
    BACKFILL_FULL = 3, // Backfill is blocked for space considerations
                       // This may or may not be a deadlock condition.
    DAMAGED = 4,        // The data may be missing or inconsistent on disk and
                       //  requires repair
    RECOVERY_FULL = 5  // Recovery is blocked because OSDs are full
  } pg_consequence_t;

  // For a given PG state, how should it be reported at the pool level?
  class PgStateResponse {
    public:
    pg_consequence_t consequence;
    typedef std::function< utime_t(const pg_stat_t&) > stuck_cb;
    stuck_cb stuck_since;
    bool invert;

    PgStateResponse(const pg_consequence_t& c, stuck_cb&& s)
      : consequence(c), stuck_since(std::move(s)), invert(false)
    {
    }

    PgStateResponse(const pg_consequence_t& c, stuck_cb&& s, bool i)
      : consequence(c), stuck_since(std::move(s)), invert(i)
    {
    }
  };

  // Record the PG state counts that contributed to a reported pool state
  class PgCauses {
    public:
    // Map of PG_STATE_* to number of pgs in that state.
    std::map<unsigned, unsigned> states;

    // List of all PG IDs that had a state contributing
    // to this health condition.
    std::set<pg_t> pgs;

    std::map<pg_t, std::string> pg_messages;
  };

  // Map of PG state to how to respond to it
  std::map<unsigned, PgStateResponse> state_to_response = {
    // Immediate reports
    { PG_STATE_INCONSISTENT,     {DAMAGED,     {}} },
    { PG_STATE_INCOMPLETE,       {UNAVAILABLE, {}} },
    { PG_STATE_SNAPTRIM_ERROR,   {DAMAGED,     {}} },
    { PG_STATE_RECOVERY_UNFOUND, {DAMAGED,     {}} },
    { PG_STATE_BACKFILL_UNFOUND, {DAMAGED,     {}} },
    { PG_STATE_BACKFILL_TOOFULL, {BACKFILL_FULL, {}} },
    { PG_STATE_RECOVERY_TOOFULL, {RECOVERY_FULL, {}} },
    { PG_STATE_DEGRADED,         {DEGRADED,    {}} },
    { PG_STATE_DOWN,             {UNAVAILABLE, {}} },
    // Delayed (wait until stuck) reports
    { PG_STATE_PEERING,          {UNAVAILABLE, [](const pg_stat_t &p){return p.last_peered;}    } },
    { PG_STATE_UNDERSIZED,       {DEGRADED,    [](const pg_stat_t &p){return p.last_fullsized;} } },
    { PG_STATE_STALE,            {UNAVAILABLE, [](const pg_stat_t &p){return p.last_unstale;}   } },
    // Delayed and inverted reports
    { PG_STATE_ACTIVE,           {UNAVAILABLE, [](const pg_stat_t &p){return p.last_active;}, true} }
  };

  // Specialized state printer that takes account of inversion of
  // ACTIVE, CLEAN checks.
  auto state_name = [](const uint64_t &state) {
    // Special cases for the states that are inverted checks
    if (state == PG_STATE_CLEAN) {
      return std::string("unclean");
    } else if (state == PG_STATE_ACTIVE) {
      return std::string("inactive");
    } else {
      return pg_state_string(state);
    }
  };

  // Map of what is wrong to information about why, implicitly also stores
  // the list of what is wrong.
  std::map<pg_consequence_t, PgCauses> detected;

  // Optimisation: trim down the number of checks to apply based on
  // the summary counters
  std::map<unsigned, PgStateResponse> possible_responses;
  for (const auto &i : num_pg_by_state) {
    for (const auto &j : state_to_response) {
      if (!j.second.invert) {
        // Check for normal tests by seeing if any pgs have the flag
        if (i.first & j.first) {
          possible_responses.insert(j);
        }
      }
    }
  }

  for (const auto &j : state_to_response) {
    if (j.second.invert) {
      // Check for inverted tests by seeing if not-all pgs have the flag
      const auto &found = num_pg_by_state.find(j.first);
      if (found == num_pg_by_state.end() || found->second != num_pg) {
        possible_responses.insert(j);
      }
    }
  }

  utime_t cutoff = now - utime_t(cct->_conf.get_val<int64_t>("mon_pg_stuck_threshold"), 0);
  // Loop over all PGs, if there are any possibly-unhealthy states in there
  if (!possible_responses.empty()) {
    for (const auto& i : pg_stat) {
      const auto &pg_id = i.first;
      const auto &pg_info = i.second;

      for (const auto &j : state_to_response) {
        const auto &pg_response_state = j.first;
        const auto &pg_response = j.second;

        // Apply the state test
        if (!(bool(pg_info.state & pg_response_state) != pg_response.invert)) {
          continue;
        }

        // Apply stuckness test if needed
        if (pg_response.stuck_since) {
          // Delayed response, check for stuckness
          utime_t last_whatever = pg_response.stuck_since(pg_info);
          if (last_whatever.is_zero() &&
            pg_info.last_change >= cutoff) {
            // still moving, ignore
            continue;
          } else if (last_whatever >= cutoff) {
            // Not stuck enough, ignore.
            continue;
          } else {

          }
        }

        auto &causes = detected[pg_response.consequence];
        causes.states[pg_response_state]++;
        causes.pgs.insert(pg_id);

        // Don't bother composing detail string if we have already recorded
        // too many
        if (causes.pg_messages.size() > max) {
          continue;
        }

        std::ostringstream ss;
        if (pg_response.stuck_since) {
          utime_t since = pg_response.stuck_since(pg_info);
          ss << "pg " << pg_id << " is stuck " << state_name(pg_response_state);
          if (since == utime_t()) {
            ss << " since forever";
          } else {
            utime_t dur = now - since;
            ss << " for " << utimespan_str(dur);
          }
          ss << ", current state " << pg_state_string(pg_info.state)
             << ", last acting " << pg_vector_string(pg_info.acting);
        } else {
          ss << "pg " << pg_id << " is "
             << pg_state_string(pg_info.state);
          ss << ", acting " << pg_vector_string(pg_info.acting);
          if (pg_info.stats.sum.num_objects_unfound) {
            ss << ", " << pg_info.stats.sum.num_objects_unfound
               << " unfound";
          }
        }

        if (pg_info.state & PG_STATE_INCOMPLETE) {
          const pg_pool_t *pi = osdmap.get_pg_pool(pg_id.pool());
          if (pi && pi->min_size > 1) {
            ss << " (reducing pool "
               << osdmap.get_pool_name(pg_id.pool())
               << " min_size from " << (int)pi->min_size
               << " may help; search ceph.com/docs for 'incomplete')";
          }
        }

        causes.pg_messages[pg_id] = ss.str();
      }
    }
  } else {
    dout(10) << __func__ << " skipping loop over PGs: counters look OK" << dendl;
  }

  for (const auto &i : detected) {
    std::string health_code;
    health_status_t sev;
    std::string summary;
    switch(i.first) {
      case UNAVAILABLE:
        health_code = "PG_AVAILABILITY";
        sev = HEALTH_WARN;
        summary = "Reduced data availability: ";
        break;
      case DEGRADED:
        health_code = "PG_DEGRADED";
        summary = "Degraded data redundancy: ";
        sev = HEALTH_WARN;
        break;
      case BACKFILL_FULL:
        health_code = "PG_BACKFILL_FULL";
        summary = "Low space hindering backfill (add storage if this doesn't resolve itself): ";
        sev = HEALTH_WARN;
        break;
      case DAMAGED:
        health_code = "PG_DAMAGED";
        summary = "Possible data damage: ";
        sev = HEALTH_ERR;
        break;
      case RECOVERY_FULL:
        health_code = "PG_RECOVERY_FULL";
        summary = "Full OSDs blocking recovery: ";
        sev = HEALTH_ERR;
        break;
      default:
        ceph_abort();
    }

    if (i.first == DEGRADED) {
      if (pg_sum.stats.sum.num_objects_degraded &&
          pg_sum.stats.sum.num_object_copies > 0) {
        double pc = (double)pg_sum.stats.sum.num_objects_degraded /
          (double)pg_sum.stats.sum.num_object_copies * (double)100.0;
        char b[20];
        snprintf(b, sizeof(b), "%.3lf", pc);
        ostringstream ss;
        ss << pg_sum.stats.sum.num_objects_degraded
           << "/" << pg_sum.stats.sum.num_object_copies << " objects degraded ("
           << b << "%)";

        // Throw in a comma for the benefit of the following PG counts
        summary += ss.str() + ", ";
      }
    }

    // Compose summary message saying how many PGs in what states led
    // to this health check failing
    std::vector<std::string> pg_msgs;
    int64_t count = 0;
    for (const auto &j : i.second.states) {
      std::ostringstream msg;
      msg << j.second << (j.second > 1 ? " pgs " : " pg ") << state_name(j.first);
      pg_msgs.push_back(msg.str());
      count += j.second;
    }
    summary += joinify(pg_msgs.begin(), pg_msgs.end(), std::string(", "));

    health_check_t *check = &checks->add(
        health_code,
        sev,
        summary,
	count);

    // Compose list of PGs contributing to this health check failing
    for (const auto &j : i.second.pg_messages) {
      check->detail.push_back(j.second);
    }
  }

  // OSD_SCRUB_ERRORS
  if (pg_sum.stats.sum.num_scrub_errors) {
    ostringstream ss;
    ss << pg_sum.stats.sum.num_scrub_errors << " scrub errors";
    checks->add("OSD_SCRUB_ERRORS", HEALTH_ERR, ss.str(),
		pg_sum.stats.sum.num_scrub_errors);
  }

  // LARGE_OMAP_OBJECTS
  if (pg_sum.stats.sum.num_large_omap_objects) {
    list<string> detail;
    for (auto &pool : pools) {
      const string& pool_name = osdmap.get_pool_name(pool.first);
      auto it2 = pg_pool_sum.find(pool.first);
      if (it2 == pg_pool_sum.end()) {
        continue;
      }
      const pool_stat_t *pstat = &it2->second;
      if (pstat == nullptr) {
        continue;
      }
      const object_stat_sum_t& sum = pstat->stats.sum;
      if (sum.num_large_omap_objects) {
        stringstream ss;
        ss << sum.num_large_omap_objects << " large objects found in pool "
           << "'" << pool_name << "'";
        detail.push_back(ss.str());
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << pg_sum.stats.sum.num_large_omap_objects << " large omap objects";
      auto& d = checks->add("LARGE_OMAP_OBJECTS", HEALTH_WARN, ss.str(),
			    pg_sum.stats.sum.num_large_omap_objects);
      stringstream tip;
      tip << "Search the cluster log for 'Large omap object found' for more "
          << "details.";
      detail.push_back(tip.str());
      d.detail.swap(detail);
    }
  }

  // CACHE_POOL_NEAR_FULL
  {
    list<string> detail;
    unsigned num_pools = 0;
    for (auto& p : pools) {
      if ((!p.second.target_max_objects && !p.second.target_max_bytes) ||
	  !pg_pool_sum.count(p.first)) {
	continue;
      }
      bool nearfull = false;
      const string& name = osdmap.get_pool_name(p.first);
      const pool_stat_t& st = get_pg_pool_sum_stat(p.first);
      uint64_t ratio = p.second.cache_target_full_ratio_micro +
	((1000000 - p.second.cache_target_full_ratio_micro) *
	 cct->_conf->mon_cache_target_full_warn_ratio);
      if (p.second.target_max_objects &&
	  (uint64_t)(st.stats.sum.num_objects -
		     st.stats.sum.num_objects_hit_set_archive) >
	  p.second.target_max_objects * (ratio / 1000000.0)) {
	ostringstream ss;
	ss << "cache pool '" << name << "' with "
	   << si_u_t(st.stats.sum.num_objects)
	   << " objects at/near target max "
	   << si_u_t(p.second.target_max_objects) << " objects";
	detail.push_back(ss.str());
	nearfull = true;
      }
      if (p.second.target_max_bytes &&
	  (uint64_t)(st.stats.sum.num_bytes -
		     st.stats.sum.num_bytes_hit_set_archive) >
	  p.second.target_max_bytes * (ratio / 1000000.0)) {
	ostringstream ss;
	ss << "cache pool '" << name
	   << "' with " << byte_u_t(st.stats.sum.num_bytes)
	   << " at/near target max "
	   << byte_u_t(p.second.target_max_bytes);
	detail.push_back(ss.str());
	nearfull = true;
      }
      if (nearfull) {
	++num_pools;
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << num_pools << " cache pools at or near target size";
      auto& d = checks->add("CACHE_POOL_NEAR_FULL", HEALTH_WARN, ss.str(),
			    num_pools);
      d.detail.swap(detail);
    }
  }

  // TOO_FEW_PGS
  unsigned num_in = osdmap.get_num_in_osds();
  auto sum_pg_up = std::max(static_cast<size_t>(pg_sum.up), pg_stat.size());
  const auto min_pg_per_osd =
    cct->_conf.get_val<uint64_t>("mon_pg_warn_min_per_osd");
  if (num_in && min_pg_per_osd > 0 && osdmap.get_pools().size() > 0) {
    auto per = sum_pg_up / num_in;
    if (per < min_pg_per_osd && per) {
      ostringstream ss;
      ss << "too few PGs per OSD (" << per
	 << " < min " << min_pg_per_osd << ")";
      checks->add("TOO_FEW_PGS", HEALTH_WARN, ss.str(),
		  min_pg_per_osd - per);
    }
  }

  // TOO_MANY_PGS
  auto max_pg_per_osd = cct->_conf.get_val<uint64_t>("mon_max_pg_per_osd");
  if (num_in && max_pg_per_osd > 0) {
    auto per = sum_pg_up / num_in;
    if (per > max_pg_per_osd) {
      ostringstream ss;
      ss << "too many PGs per OSD (" << per
	 << " > max " << max_pg_per_osd << ")";
      checks->add("TOO_MANY_PGS", HEALTH_WARN, ss.str(),
		  per - max_pg_per_osd);
    }
  }

  // TOO_FEW_OSDS
  auto warn_too_few_osds = cct->_conf.get_val<bool>("mon_warn_on_too_few_osds");
  auto osd_pool_default_size = cct->_conf.get_val<uint64_t>("osd_pool_default_size");
  if (warn_too_few_osds && osdmap.get_num_osds() < osd_pool_default_size) {
    ostringstream ss;
    ss << "OSD count " << osdmap.get_num_osds()
	 << " < osd_pool_default_size " << osd_pool_default_size;
    checks->add("TOO_FEW_OSDS", HEALTH_WARN, ss.str(),
		osd_pool_default_size - osdmap.get_num_osds());
  }

  // SLOW_PING_TIME
  // Convert milliseconds to microseconds
  auto warn_slow_ping_time = cct->_conf.get_val<double>("mon_warn_on_slow_ping_time") * 1000;
  auto grace = cct->_conf.get_val<int64_t>("osd_heartbeat_grace");
  if (warn_slow_ping_time == 0) {
    double ratio = cct->_conf.get_val<double>("mon_warn_on_slow_ping_ratio");
    warn_slow_ping_time = grace;
    warn_slow_ping_time *= 1000000 * ratio; // Seconds of grace to microseconds at ratio
  }
  if (warn_slow_ping_time > 0) {

    struct mon_ping_item_t {
      uint32_t pingtime;
      int from;
      int to;
      bool improving;

      bool operator<(const mon_ping_item_t& rhs) const {
        if (pingtime < rhs.pingtime)
          return true;
        if (pingtime > rhs.pingtime)
          return false;
        if (from < rhs.from)
          return true;
        if (from > rhs.from)
          return false;
        return to < rhs.to;
      }
    };

    list<string> detail_back;
    list<string> detail_front;
    list<string> detail;
    set<mon_ping_item_t> back_sorted, front_sorted;
    for (auto i : osd_stat) {
      for (auto j : i.second.hb_pingtime) {

	// Maybe source info is old
	if (now.sec() - j.second.last_update > grace * 60)
	  continue;

	mon_ping_item_t back;
	back.pingtime = std::max(j.second.back_pingtime[0], j.second.back_pingtime[1]);
	back.pingtime = std::max(back.pingtime, j.second.back_pingtime[2]);
	back.from = i.first;
	back.to = j.first;
	if (back.pingtime > warn_slow_ping_time) {
	  back.improving = (j.second.back_pingtime[0] < j.second.back_pingtime[1]
			    && j.second.back_pingtime[1] < j.second.back_pingtime[2]);
	  back_sorted.emplace(back);
	}

	mon_ping_item_t front;
	front.pingtime = std::max(j.second.front_pingtime[0], j.second.front_pingtime[1]);
	front.pingtime = std::max(front.pingtime, j.second.front_pingtime[2]);
	front.from = i.first;
	front.to = j.first;
	if (front.pingtime > warn_slow_ping_time) {
	  front.improving = (j.second.front_pingtime[0] < j.second.front_pingtime[1]
			     && j.second.front_pingtime[1] < j.second.back_pingtime[2]);
	  front_sorted.emplace(front);
	}
      }
      if (i.second.num_shards_repaired >
		      cct->_conf.get_val<uint64_t>("mon_osd_warn_num_repaired")) {
        ostringstream ss;
	ss << "osd." << i.first << " had " << i.second.num_shards_repaired << " reads repaired";
        detail.push_back(ss.str());
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << "Too many repaired reads on " << detail.size() << " OSDs";
      auto& d = checks->add("OSD_TOO_MANY_REPAIRS", HEALTH_WARN, ss.str(),
		      detail.size());
      d.detail.swap(detail);
    }
    int max_detail = 10;
    for (auto &sback : boost::adaptors::reverse(back_sorted)) {
      ostringstream ss;
      if (max_detail == 0) {
	ss << "Truncated long network list.  Use ceph daemon mgr.# dump_osd_network for more information";
        detail_back.push_back(ss.str());
        break;
      }
      max_detail--;
      ss << "Slow OSD heartbeats on back from osd." << sback.from
	 << " [" << osd_parentage(osdmap, sback.from) << "]"
         << (osdmap.is_down(sback.from) ? " (down)" : "")
	 << " to osd." << sback.to
	 << " [" << osd_parentage(osdmap, sback.to) << "]"
         << (osdmap.is_down(sback.to) ? " (down)" : "")
	 << " " << fixed_u_to_string(sback.pingtime, 3) << " msec"
	 << (sback.improving ? " possibly improving" : "");
      detail_back.push_back(ss.str());
    }
    max_detail = 10;
    for (auto &sfront : boost::adaptors::reverse(front_sorted)) {
      ostringstream ss;
      if (max_detail == 0) {
	ss << "Truncated long network list.  Use ceph daemon mgr.# dump_osd_network for more information";
        detail_front.push_back(ss.str());
        break;
      }
      max_detail--;
      // Get crush parentage for each osd
      ss << "Slow OSD heartbeats on front from osd." << sfront.from
	 << " [" << osd_parentage(osdmap, sfront.from) << "]"
         << (osdmap.is_down(sfront.from) ? " (down)" : "")
         << " to osd." << sfront.to
	 << " [" << osd_parentage(osdmap, sfront.to) << "]"
         << (osdmap.is_down(sfront.to) ? " (down)" : "")
	 << " " << fixed_u_to_string(sfront.pingtime, 3) << " msec"
	 << (sfront.improving ? " possibly improving" : "");
      detail_front.push_back(ss.str());
    }
    if (detail_back.size() != 0) {
      ostringstream ss;
      ss << "Slow OSD heartbeats on back (longest "
	 << fixed_u_to_string(back_sorted.rbegin()->pingtime, 3) << "ms)";
      auto& d = checks->add("OSD_SLOW_PING_TIME_BACK", HEALTH_WARN, ss.str(),
		      back_sorted.size());
      d.detail.swap(detail_back);
    }
    if (detail_front.size() != 0) {
      ostringstream ss;
      ss << "Slow OSD heartbeats on front (longest "
	 << fixed_u_to_string(front_sorted.rbegin()->pingtime, 3) << "ms)";
      auto& d = checks->add("OSD_SLOW_PING_TIME_FRONT", HEALTH_WARN, ss.str(),
		      front_sorted.size());
      d.detail.swap(detail_front);
    }
  }

  // SMALLER_PGP_NUM
  // MANY_OBJECTS_PER_PG
  if (!pg_stat.empty()) {
    list<string> pgp_detail, many_detail;
    const auto mon_pg_warn_min_objects =
      cct->_conf.get_val<int64_t>("mon_pg_warn_min_objects");
    const auto mon_pg_warn_min_pool_objects =
      cct->_conf.get_val<int64_t>("mon_pg_warn_min_pool_objects");
    const auto mon_pg_warn_max_object_skew =
      cct->_conf.get_val<double>("mon_pg_warn_max_object_skew");
    for (auto p = pg_pool_sum.begin();
         p != pg_pool_sum.end();
         ++p) {
      const pg_pool_t *pi = osdmap.get_pg_pool(p->first);
      if (!pi)
	continue;   // in case osdmap changes haven't propagated to PGMap yet
      const string& name = osdmap.get_pool_name(p->first);
      // NOTE: we use pg_num_target and pgp_num_target for the purposes of
      // the warnings.  If the cluster is failing to converge on the target
      // values that is a separate issue!
      if (pi->get_pg_num_target() > pi->get_pgp_num_target() &&
	  !(name.find(".DELETED") != string::npos &&
	    cct->_conf->mon_fake_pool_delete)) {
	ostringstream ss;
	ss << "pool " << name << " pg_num "
	   << pi->get_pg_num_target()
	   << " > pgp_num " << pi->get_pgp_num_target();
	pgp_detail.push_back(ss.str());
      }
      int average_objects_per_pg = pg_sum.stats.sum.num_objects / pg_stat.size();
      if (average_objects_per_pg > 0 &&
          pg_sum.stats.sum.num_objects >= mon_pg_warn_min_objects &&
          p->second.stats.sum.num_objects >= mon_pg_warn_min_pool_objects) {
	int objects_per_pg = p->second.stats.sum.num_objects /
	  pi->get_pg_num_target();
	float ratio = (float)objects_per_pg / (float)average_objects_per_pg;
	if (mon_pg_warn_max_object_skew > 0 &&
	    ratio > mon_pg_warn_max_object_skew) {
	  ostringstream ss;
	  if (pi->pg_autoscale_mode != pg_pool_t::pg_autoscale_mode_t::ON) {
	      ss << "pool " << name << " objects per pg ("
		 << objects_per_pg << ") is more than " << ratio
		 << " times cluster average ("
		 << average_objects_per_pg << ")";
	      many_detail.push_back(ss.str());
	  }
	}
      }
    }
    if (!pgp_detail.empty()) {
      ostringstream ss;
      ss << pgp_detail.size() << " pools have pg_num > pgp_num";
      auto& d = checks->add("SMALLER_PGP_NUM", HEALTH_WARN, ss.str(),
			    pgp_detail.size());
      d.detail.swap(pgp_detail);
    }
    if (!many_detail.empty()) {
      ostringstream ss;
      ss << many_detail.size() << " pools have many more objects per pg than"
	 << " average";
      auto& d = checks->add("MANY_OBJECTS_PER_PG", HEALTH_WARN, ss.str(),
			    many_detail.size());
      d.detail.swap(many_detail);
    }
  }

  // POOL_FULL
  // POOL_NEAR_FULL
  {
    float warn_threshold = (float)g_conf().get_val<int64_t>("mon_pool_quota_warn_threshold")/100;
    float crit_threshold = (float)g_conf().get_val<int64_t>("mon_pool_quota_crit_threshold")/100;
    list<string> full_detail, nearfull_detail;
    unsigned full_pools = 0, nearfull_pools = 0;
    for (auto it : pools) {
      auto it2 = pg_pool_sum.find(it.first);
      if (it2 == pg_pool_sum.end()) {
	continue;
      }
      const pool_stat_t *pstat = &it2->second;
      const object_stat_sum_t& sum = pstat->stats.sum;
      const string& pool_name = osdmap.get_pool_name(it.first);
      const pg_pool_t &pool = it.second;
      bool full = false, nearfull = false;
      if (pool.quota_max_objects > 0) {
	stringstream ss;
	if ((uint64_t)sum.num_objects >= pool.quota_max_objects) {
	} else if (crit_threshold > 0 &&
		   sum.num_objects >= pool.quota_max_objects*crit_threshold) {
	  ss << "pool '" << pool_name
	     << "' has " << sum.num_objects << " objects"
	     << " (max " << pool.quota_max_objects << ")";
	  full_detail.push_back(ss.str());
	  full = true;
	} else if (warn_threshold > 0 &&
		   sum.num_objects >= pool.quota_max_objects*warn_threshold) {
	  ss << "pool '" << pool_name
	     << "' has " << sum.num_objects << " objects"
	     << " (max " << pool.quota_max_objects << ")";
	  nearfull_detail.push_back(ss.str());
	  nearfull = true;
	}
      }
      if (pool.quota_max_bytes > 0) {
	stringstream ss;
	if ((uint64_t)sum.num_bytes >= pool.quota_max_bytes) {
	} else if (crit_threshold > 0 &&
		   sum.num_bytes >= pool.quota_max_bytes*crit_threshold) {
	  ss << "pool '" << pool_name
	     << "' has " << byte_u_t(sum.num_bytes)
	     << " (max " << byte_u_t(pool.quota_max_bytes) << ")";
	  full_detail.push_back(ss.str());
	  full = true;
	} else if (warn_threshold > 0 &&
		   sum.num_bytes >= pool.quota_max_bytes*warn_threshold) {
	  ss << "pool '" << pool_name
	     << "' has " << byte_u_t(sum.num_bytes)
	     << " (max " << byte_u_t(pool.quota_max_bytes) << ")";
	  nearfull_detail.push_back(ss.str());
	  nearfull = true;
	}
      }
      if (full) {
	++full_pools;
      }
      if (nearfull) {
	++nearfull_pools;
      }
    }
    if (full_pools) {
      ostringstream ss;
      ss << full_pools << " pools full";
      auto& d = checks->add("POOL_FULL", HEALTH_ERR, ss.str(), full_pools);
      d.detail.swap(full_detail);
    }
    if (nearfull_pools) {
      ostringstream ss;
      ss << nearfull_pools << " pools nearfull";
      auto& d = checks->add("POOL_NEAR_FULL", HEALTH_WARN, ss.str(), nearfull_pools);
      d.detail.swap(nearfull_detail);
    }
  }

  // OBJECT_MISPLACED
  if (pg_sum.stats.sum.num_objects_misplaced &&
      pg_sum.stats.sum.num_object_copies > 0 &&
      cct->_conf->mon_warn_on_misplaced) {
    double pc = (double)pg_sum.stats.sum.num_objects_misplaced /
      (double)pg_sum.stats.sum.num_object_copies * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    ostringstream ss;
    ss << pg_sum.stats.sum.num_objects_misplaced
       << "/" << pg_sum.stats.sum.num_object_copies << " objects misplaced ("
       << b << "%)";
    checks->add("OBJECT_MISPLACED", HEALTH_WARN, ss.str(),
		pg_sum.stats.sum.num_objects_misplaced);
  }

  // OBJECT_UNFOUND
  if (pg_sum.stats.sum.num_objects_unfound &&
      pg_sum.stats.sum.num_objects) {
    double pc = (double)pg_sum.stats.sum.num_objects_unfound /
      (double)pg_sum.stats.sum.num_objects * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    ostringstream ss;
    ss << pg_sum.stats.sum.num_objects_unfound
       << "/" << pg_sum.stats.sum.num_objects << " objects unfound (" << b << "%)";
    auto& d = checks->add("OBJECT_UNFOUND", HEALTH_WARN, ss.str(),
			  pg_sum.stats.sum.num_objects_unfound);

    for (auto& p : pg_stat) {
      if (p.second.stats.sum.num_objects_unfound) {
	ostringstream ss;
	ss << "pg " << p.first
	   << " has " << p.second.stats.sum.num_objects_unfound
	   << " unfound objects";
	d.detail.push_back(ss.str());
	if (d.detail.size() > max) {
	  d.detail.push_back("(additional pgs left out for brevity)");
	  break;
	}
      }
    }
  }

  // REQUEST_SLOW
  // REQUEST_STUCK
  // SLOW_OPS unifies them in mimic.
  if (osdmap.require_osd_release < ceph_release_t::mimic &&
      cct->_conf->mon_osd_warn_op_age > 0 &&
      !osd_sum.op_queue_age_hist.h.empty() &&
      osd_sum.op_queue_age_hist.upper_bound() / 1000.0 >
      cct->_conf->mon_osd_warn_op_age) {
    list<string> warn_detail, error_detail;
    unsigned warn = 0, error = 0;
    float err_age =
      cct->_conf->mon_osd_warn_op_age * cct->_conf->mon_osd_err_op_age_ratio;
    const pow2_hist_t& h = osd_sum.op_queue_age_hist;
    for (unsigned i = h.h.size() - 1; i > 0; --i) {
      float ub = (float)(1 << i) / 1000.0;
      if (ub < cct->_conf->mon_osd_warn_op_age)
	break;
      if (h.h[i]) {
	ostringstream ss;
	ss << h.h[i] << " ops are blocked > " << ub << " sec";
	if (ub > err_age) {
	  error += h.h[i];
	  error_detail.push_back(ss.str());
	} else {
	  warn += h.h[i];
	  warn_detail.push_back(ss.str());
	}
      }
    }

    map<float,set<int>> warn_osd_by_max; // max -> osds
    map<float,set<int>> error_osd_by_max; // max -> osds
    if (!warn_detail.empty() || !error_detail.empty()) {
      for (auto& p : osd_stat) {
	const pow2_hist_t& h = p.second.op_queue_age_hist;
	for (unsigned i = h.h.size() - 1; i > 0; --i) {
	  float ub = (float)(1 << i) / 1000.0;
	  if (ub < cct->_conf->mon_osd_warn_op_age)
	    break;
	  if (h.h[i]) {
	    if (ub > err_age) {
	      error_osd_by_max[ub].insert(p.first);
	    } else {
	      warn_osd_by_max[ub].insert(p.first);
	    }
	    break;
	  }
	}
      }
    }

    if (!warn_detail.empty()) {
      ostringstream ss;
      ss << warn << " slow requests are blocked > "
	 << cct->_conf->mon_osd_warn_op_age << " sec";
      auto& d = checks->add("REQUEST_SLOW", HEALTH_WARN, ss.str(), warn);
      d.detail.swap(warn_detail);
      int left = max;
      for (auto& p : warn_osd_by_max) {
	ostringstream ss;
	if (p.second.size() > 1) {
	  ss << "osds " << p.second
             << " have blocked requests > " << p.first << " sec";
	} else {
	  ss << "osd." << *p.second.begin()
             << " has blocked requests > " << p.first << " sec";
	}
	d.detail.push_back(ss.str());
	if (--left == 0) {
	  break;
	}
      }
    }
    if (!error_detail.empty()) {
      ostringstream ss;
      ss << error << " stuck requests are blocked > "
	 << err_age << " sec";
      auto& d = checks->add("REQUEST_STUCK", HEALTH_ERR, ss.str(), error);
      d.detail.swap(error_detail);
      int left = max;
      for (auto& p : error_osd_by_max) {
	ostringstream ss;
	if (p.second.size() > 1) {
	  ss << "osds " << p.second
             << " have stuck requests > " << p.first << " sec";
	} else {
	  ss << "osd." << *p.second.begin()
             << " has stuck requests > " << p.first << " sec";
	}
	d.detail.push_back(ss.str());
	if (--left == 0) {
	  break;
	}
      }
    }
  }

  // OBJECT_STORE_WARN
  if (osd_sum.os_alerts.size()) {
    map<string, pair<size_t, list<string>>> os_alerts_sum;

    for (auto& a : osd_sum.os_alerts) {
      int left = max;
      string s0 = " osd.";
      s0 += stringify(a.first);
      for (auto& aa : a.second) {
        string s(s0);
        s += " ";
        s += aa.second;
        auto it = os_alerts_sum.find(aa.first);
        if (it == os_alerts_sum.end()) {
          list<string> d;
          d.emplace_back(s);
          os_alerts_sum.emplace(aa.first, std::make_pair(1, d));
        } else {
          auto& p = it->second;
          ++p.first;
          p.second.emplace_back(s);
        }
	if (--left == 0) {
	  break;
	}
      }
    }

    for (auto& asum : os_alerts_sum) {
      string summary = stringify(asum.second.first) + " OSD(s)";
      if (asum.first == "BLUEFS_SPILLOVER") {
	summary += " experiencing BlueFS spillover";
      } else if (asum.first == "BLUESTORE_NO_COMPRESSION") {
	summary += " have broken BlueStore compression";
      } else if (asum.first == "BLUESTORE_LEGACY_STATFS") {
	summary += " reporting legacy (not per-pool) BlueStore stats";
      } else if (asum.first == "BLUESTORE_DISK_SIZE_MISMATCH") {
	summary += " have dangerous mismatch between BlueStore block device and free list sizes";
      } else if (asum.first == "BLUESTORE_NO_PER_PG_OMAP") {
	summary += " reporting legacy (not per-pg) BlueStore omap";
      } else if (asum.first == "BLUESTORE_NO_PER_POOL_OMAP") {
	summary += " reporting legacy (not per-pool) BlueStore omap usage stats";
      } else if (asum.first == "BLUESTORE_SPURIOUS_READ_ERRORS") {
        summary += " have spurious read errors";
      }

      auto& d = checks->add(asum.first, HEALTH_WARN, summary, asum.second.first);
      for (auto& s : asum.second.second) {
        d.detail.push_back(s);
      }
    }
  }
  // PG_NOT_SCRUBBED
  // PG_NOT_DEEP_SCRUBBED
  if (cct->_conf->mon_warn_pg_not_scrubbed_ratio ||
        cct->_conf->mon_warn_pg_not_deep_scrubbed_ratio) {
    list<string> detail, deep_detail;
    int detail_max = max, deep_detail_max = max;
    int detail_more = 0, deep_detail_more = 0;
    int detail_total = 0, deep_detail_total = 0;
    for (auto& p : pg_stat) {
      int64_t pnum =  p.first.pool();
      auto pool = osdmap.get_pg_pool(pnum);
      if (!pool)
        continue;
      if (cct->_conf->mon_warn_pg_not_scrubbed_ratio) {
        double scrub_max_interval = 0;
        pool->opts.get(pool_opts_t::SCRUB_MAX_INTERVAL, &scrub_max_interval);
        if (scrub_max_interval <= 0) {
          scrub_max_interval = cct->_conf->osd_scrub_max_interval;
        }
        const double age = (cct->_conf->mon_warn_pg_not_scrubbed_ratio * scrub_max_interval) +
          scrub_max_interval;
        utime_t cutoff = now;
        cutoff -= age;
        if (p.second.last_scrub_stamp < cutoff) {
          if (detail_max > 0) {
            ostringstream ss;
            ss << "pg " << p.first << " not scrubbed since "
               << p.second.last_scrub_stamp;
            detail.push_back(ss.str());
            --detail_max;
          } else {
            ++detail_more;
          }
          ++detail_total;
        }
      }
      if (cct->_conf->mon_warn_pg_not_deep_scrubbed_ratio) {
        double deep_scrub_interval = 0;
        pool->opts.get(pool_opts_t::DEEP_SCRUB_INTERVAL, &deep_scrub_interval);
        if (deep_scrub_interval <= 0) {
          deep_scrub_interval = cct->_conf->osd_deep_scrub_interval;
        }
        double deep_age = (cct->_conf->mon_warn_pg_not_deep_scrubbed_ratio * deep_scrub_interval) +
          deep_scrub_interval;
        utime_t deep_cutoff = now;
        deep_cutoff -= deep_age;
        if (p.second.last_deep_scrub_stamp < deep_cutoff) {
          if (deep_detail_max > 0) {
            ostringstream ss;
            ss << "pg " << p.first << " not deep-scrubbed since "
               << p.second.last_deep_scrub_stamp;
            deep_detail.push_back(ss.str());
            --deep_detail_max;
          } else {
            ++deep_detail_more;
          }
          ++deep_detail_total;
        }
      }
    }
    if (detail_total) {
      ostringstream ss;
      ss << detail_total << " pgs not scrubbed in time";
      auto& d = checks->add("PG_NOT_SCRUBBED", HEALTH_WARN, ss.str(), detail_total);

      if (!detail.empty()) {
        d.detail.swap(detail);

        if (detail_more) {
          ostringstream ss;
          ss << detail_more << " more pgs... ";
          d.detail.push_back(ss.str());
        }
      }
    }
    if (deep_detail_total) {
      ostringstream ss;
      ss << deep_detail_total << " pgs not deep-scrubbed in time";
      auto& d = checks->add("PG_NOT_DEEP_SCRUBBED", HEALTH_WARN, ss.str(),
			    deep_detail_total);

      if (!deep_detail.empty()) {
        d.detail.swap(deep_detail);

        if (deep_detail_more) {
          ostringstream ss;
          ss << deep_detail_more << " more pgs... ";
          d.detail.push_back(ss.str());
        }
      }
    }
  }

  // POOL_APP
  if (g_conf().get_val<bool>("mon_warn_on_pool_no_app")) {
    list<string> detail;
    for (auto &it : pools) {
      const pg_pool_t &pool = it.second;
      const string& pool_name = osdmap.get_pool_name(it.first);
      // application metadata is not encoded until luminous is minimum
      // required release
      if (pool.application_metadata.empty() && !pool.is_tier()) {
        stringstream ss;
        ss << "application not enabled on pool '" << pool_name << "'";
        detail.push_back(ss.str());
      }
    }
    if (!detail.empty()) {
      ostringstream ss;
      ss << detail.size() << " pool(s) do not have an application enabled";
      auto& d = checks->add("POOL_APP_NOT_ENABLED", HEALTH_WARN, ss.str(),
			    detail.size());
      stringstream tip;
      tip << "use 'ceph osd pool application enable <pool-name> "
          << "<app-name>', where <app-name> is 'cephfs', 'rbd', 'rgw', "
          << "or freeform for custom applications.";
      detail.push_back(tip.str());
      d.detail.swap(detail);
    }
  }

  // PG_SLOW_SNAP_TRIMMING
  if (!pg_stat.empty() && cct->_conf->mon_osd_snap_trim_queue_warn_on > 0) {
    uint32_t snapthreshold = cct->_conf->mon_osd_snap_trim_queue_warn_on;
    uint64_t snaptrimq_exceeded = 0;
    uint32_t longest_queue = 0;
    const pg_t* longest_q_pg = nullptr;
    list<string> detail;

    for (auto& i: pg_stat) {
      uint32_t current_len = i.second.snaptrimq_len;
      if (current_len >= snapthreshold) {
        snaptrimq_exceeded++;
        if (longest_queue <= current_len) {
          longest_q_pg = &i.first;
          longest_queue = current_len;
        }
        if (detail.size() < max - 1) {
          stringstream ss;
          ss << "snap trim queue for pg " << i.first << " at " << current_len;
          detail.push_back(ss.str());
          continue;
        }
        if (detail.size() < max) {
          detail.push_back("...more pgs affected");
          continue;
        }
      }
    }

    if (snaptrimq_exceeded) {
      {
         ostringstream ss;
         ss << "longest queue on pg " << *longest_q_pg << " at " << longest_queue;
         detail.push_back(ss.str());
      }

      stringstream ss;
      ss << "snap trim queue for " << snaptrimq_exceeded << " pg(s) >= " << snapthreshold << " (mon_osd_snap_trim_queue_warn_on)";
      auto& d = checks->add("PG_SLOW_SNAP_TRIMMING", HEALTH_WARN, ss.str(),
			    snaptrimq_exceeded);
      detail.push_back("try decreasing \"osd snap trim sleep\" and/or increasing \"osd pg max concurrent snap trims\".");
      d.detail.swap(detail);
    }
  }
}

void PGMap::print_summary(ceph::Formatter *f, ostream *out) const
{
  if (f) {
    f->open_array_section("pgs_by_pool_state");
    for (auto& i: num_pg_by_pool_state) {
      f->open_object_section("per_pool_pgs_by_state");
      f->dump_int("pool_id", i.first);
      f->open_array_section("pg_state_counts");
      for (auto& j : i.second) {
        f->open_object_section("pg_state_count");
        f->dump_string("state_name", pg_state_string(j.first));
        f->dump_int("count", j.second);
        f->close_section();
      }
      f->close_section();
      f->close_section();
    }
    f->close_section();
  }
  PGMapDigest::print_summary(f, out);
}

int process_pg_map_command(
  const string& orig_prefix,
  const cmdmap_t& orig_cmdmap,
  const PGMap& pg_map,
  const OSDMap& osdmap,
  ceph::Formatter *f,
  stringstream *ss,
  bufferlist *odata)
{
  string prefix = orig_prefix;
  auto cmdmap = orig_cmdmap;

  string omap_stats_note =
      "\n* NOTE: Omap statistics are gathered during deep scrub and "
      "may be inaccurate soon afterwards depending on utilization. See "
      "http://docs.ceph.com/en/latest/dev/placement-group/#omap-statistics "
      "for further details.\n";
  bool omap_stats_note_required = false;

  // perhaps these would be better in the parsing, but it's weird
  bool primary = false;
  if (prefix == "pg dump_json") {
    vector<string> v;
    v.push_back(string("all"));
    cmd_putval(g_ceph_context, cmdmap, "dumpcontents", v);
    prefix = "pg dump";
  } else if (prefix == "pg dump_pools_json") {
    vector<string> v;
    v.push_back(string("pools"));
    cmd_putval(g_ceph_context, cmdmap, "dumpcontents", v);
    prefix = "pg dump";
  } else if (prefix == "pg ls-by-primary") {
    primary = true;
    prefix = "pg ls";
  } else if (prefix == "pg ls-by-osd") {
    prefix = "pg ls";
  } else if (prefix == "pg ls-by-pool") {
    prefix = "pg ls";
    string poolstr;
    cmd_getval(cmdmap, "poolstr", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      *ss << "pool " << poolstr << " does not exist";
      return -ENOENT;
    }
    cmd_putval(g_ceph_context, cmdmap, "pool", pool);
  }

  stringstream ds;
  if (prefix == "pg stat") {
    if (f) {
      f->open_object_section("pg_summary");
      pg_map.print_oneline_summary(f, NULL);
      f->close_section();
      f->flush(ds);
    } else {
      ds << pg_map;
    }
    odata->append(ds);
    return 0;
  }

  if (prefix == "pg getmap") {
    pg_map.encode(*odata);
    *ss << "got pgmap version " << pg_map.version;
    return 0;
  }

  if (prefix == "pg dump") {
    string val;
    vector<string> dumpcontents;
    set<string> what;
    if (cmd_getval(cmdmap, "dumpcontents", dumpcontents)) {
      copy(dumpcontents.begin(), dumpcontents.end(),
           inserter(what, what.end()));
    }
    if (what.empty())
      what.insert("all");
    if (f) {
      if (what.count("all")) {
	f->open_object_section("pg_map");
	pg_map.dump(f);
	f->close_section();
      } else if (what.count("summary") || what.count("sum")) {
	f->open_object_section("pg_map");
	pg_map.dump_basic(f);
	f->close_section();
      } else {
	if (what.count("pools")) {
	  pg_map.dump_pool_stats(f);
	}
	if (what.count("osds")) {
	  pg_map.dump_osd_stats(f);
	}
	if (what.count("pgs")) {
	  pg_map.dump_pg_stats(f, false);
	}
	if (what.count("pgs_brief")) {
	  pg_map.dump_pg_stats(f, true);
	}
	if (what.count("delta")) {
	  f->open_object_section("delta");
	  pg_map.dump_delta(f);
	  f->close_section();
	}
      }
      f->flush(*odata);
    } else {
      if (what.count("all")) {
	pg_map.dump(ds);
        omap_stats_note_required = true;
      } else if (what.count("summary") || what.count("sum")) {
	pg_map.dump_basic(ds);
	pg_map.dump_pg_sum_stats(ds, true);
	pg_map.dump_osd_sum_stats(ds);
        omap_stats_note_required = true;
      } else {
	if (what.count("pgs_brief")) {
	  pg_map.dump_pg_stats(ds, true);
	}
	bool header = true;
	if (what.count("pgs")) {
	  pg_map.dump_pg_stats(ds, false);
	  header = false;
          omap_stats_note_required = true;
	}
	if (what.count("pools")) {
	  pg_map.dump_pool_stats(ds, header);
          omap_stats_note_required = true;
	}
	if (what.count("osds")) {
	  pg_map.dump_osd_stats(ds);
	}
      }
      odata->append(ds);
      if (omap_stats_note_required) {
        odata->append(omap_stats_note);
      }
    }
    *ss << "dumped " << what;
    return 0;
  }

  if (prefix == "pg ls") {
    int64_t osd = -1;
    int64_t pool = -1;
    vector<string>states;
    set<pg_t> pgs;
    cmd_getval(cmdmap, "pool", pool);
    cmd_getval(cmdmap, "osd", osd);
    cmd_getval(cmdmap, "states", states);
    if (pool >= 0 && !osdmap.have_pg_pool(pool)) {
      *ss << "pool " << pool << " does not exist";
      return -ENOENT;
    }
    if (osd >= 0 && !osdmap.is_up(osd)) {
      *ss << "osd " << osd << " is not up";
      return -EAGAIN;
    }
    if (states.empty())
      states.push_back("all");

    uint64_t state = 0;

    while (!states.empty()) {
      string state_str = states.back();

      if (state_str == "all") {
        state = -1;
        break;
      } else {
        auto filter = pg_string_state(state_str);
        if (!filter) {
          *ss << "'" << state_str << "' is not a valid pg state,"
              << " available choices: " << pg_state_string(0xFFFFFFFF);
          return -EINVAL;
        }
        state |= *filter;
      }

      states.pop_back();
    }

    pg_map.get_filtered_pg_stats(state, pool, osd, primary, pgs);

    if (f && !pgs.empty()) {
      pg_map.dump_filtered_pg_stats(f, pgs);
      f->flush(*odata);
    } else if (!pgs.empty()) {
      pg_map.dump_filtered_pg_stats(ds, pgs);
      odata->append(ds);
      odata->append(omap_stats_note);
    }
    return 0;
  }

  if (prefix == "pg dump_stuck") {
    vector<string> stuckop_vec;
    cmd_getval(cmdmap, "stuckops", stuckop_vec);
    if (stuckop_vec.empty())
      stuckop_vec.push_back("unclean");
    const int64_t threshold = cmd_getval_or<int64_t>(
      cmdmap, "threshold",
      g_conf().get_val<int64_t>("mon_pg_stuck_threshold"));

    if (pg_map.dump_stuck_pg_stats(ds, f, (int)threshold, stuckop_vec) < 0) {
      *ss << "failed";
    } else {
      *ss << "ok";
    }
    odata->append(ds);
    return 0;
  }

  if (prefix == "pg debug") {
    const string debugop = cmd_getval_or<string>(
      cmdmap, "debugop",
      "unfound_objects_exist");
    if (debugop == "unfound_objects_exist") {
      bool unfound_objects_exist = false;
      for (const auto& p : pg_map.pg_stat) {
	if (p.second.stats.sum.num_objects_unfound > 0) {
	  unfound_objects_exist = true;
	  break;
	}
      }
      if (unfound_objects_exist)
	ds << "TRUE";
      else
	ds << "FALSE";
      odata->append(ds);
      return 0;
    }
    if (debugop == "degraded_pgs_exist") {
      bool degraded_pgs_exist = false;
      for (const auto& p : pg_map.pg_stat) {
	if (p.second.stats.sum.num_objects_degraded > 0) {
	  degraded_pgs_exist = true;
	  break;
	}
      }
      if (degraded_pgs_exist)
	ds << "TRUE";
      else
	ds << "FALSE";
      odata->append(ds);
      return 0;
    }
  }

  if (prefix == "osd perf") {
    if (f) {
      f->open_object_section("osdstats");
      pg_map.dump_osd_perf_stats(f);
      f->close_section();
      f->flush(ds);
    } else {
      pg_map.print_osd_perf_stats(&ds);
    }
    odata->append(ds);
    return 0;
  }

  if (prefix == "osd blocked-by") {
    if (f) {
      f->open_object_section("osd_blocked_by");
      pg_map.dump_osd_blocked_by_stats(f);
      f->close_section();
      f->flush(ds);
    } else {
      pg_map.print_osd_blocked_by_stats(&ds);
    }
    odata->append(ds);
    return 0;
  }

  return -EOPNOTSUPP;
}

void PGMapUpdater::check_osd_map(
  CephContext *cct,
  const OSDMap& osdmap,
  const PGMap& pgmap,
  PGMap::Incremental *pending_inc)
{
  for (auto& p : pgmap.osd_stat) {
    if (!osdmap.exists(p.first)) {
      // remove osd_stat
      pending_inc->rm_stat(p.first);
    } else if (osdmap.is_out(p.first)) {
      // zero osd_stat
      if (p.second.statfs.total != 0) {
	pending_inc->stat_osd_out(p.first);
      }
    } else if (!osdmap.is_up(p.first)) {
      // zero the op_queue_age_hist
      if (!p.second.op_queue_age_hist.empty()) {
	pending_inc->stat_osd_down_up(p.first, pgmap);
      }
    }
  }

  // deleted pgs (pools)?
  for (auto& p : pgmap.pg_pool_sum) {
    if (!osdmap.have_pg_pool(p.first)) {
      ldout(cct, 10) << __func__ << " pool " << p.first << " gone, removing pgs"
		     << dendl;
      for (auto& q : pgmap.pg_stat) {
	if (q.first.pool() == p.first) {
	  pending_inc->pg_remove.insert(q.first);
	}
      }
      auto q = pending_inc->pg_stat_updates.begin();
      while (q != pending_inc->pg_stat_updates.end()) {
	if (q->first.pool() == p.first) {
	  q = pending_inc->pg_stat_updates.erase(q);
	} else {
	  ++q;
	}
      }
    }
  }

  // new (split or new pool) or merged pgs?
  map<int64_t,unsigned> new_pg_num;
  for (auto& p : osdmap.get_pools()) {
    int64_t poolid = p.first;
    const pg_pool_t& pi = p.second;
    auto q = pgmap.num_pg_by_pool.find(poolid);
    unsigned my_pg_num = 0;
    if (q != pgmap.num_pg_by_pool.end())
      my_pg_num = q->second;
    unsigned pg_num = pi.get_pg_num();
    new_pg_num[poolid] = pg_num;
    if (my_pg_num < pg_num) {
      ldout(cct,10) << __func__ << " pool " << poolid << " pg_num " << pg_num
		    << " > my pg_num " << my_pg_num << dendl;
      for (unsigned ps = my_pg_num; ps < pg_num; ++ps) {
	pg_t pgid(ps, poolid);
	if (pending_inc->pg_stat_updates.count(pgid) == 0) {
	  ldout(cct,20) << __func__ << " adding " << pgid << dendl;
	  pg_stat_t &stats = pending_inc->pg_stat_updates[pgid];
	  stats.last_fresh = osdmap.get_modified();
	  stats.last_active = osdmap.get_modified();
	  stats.last_change = osdmap.get_modified();
	  stats.last_peered = osdmap.get_modified();
	  stats.last_clean = osdmap.get_modified();
	  stats.last_unstale = osdmap.get_modified();
	  stats.last_undegraded = osdmap.get_modified();
	  stats.last_fullsized = osdmap.get_modified();
	  stats.last_scrub_stamp = osdmap.get_modified();
	  stats.last_deep_scrub_stamp = osdmap.get_modified();
	  stats.last_clean_scrub_stamp = osdmap.get_modified();
	}
      }
    } else if (my_pg_num > pg_num) {
      ldout(cct,10) << __func__ << " pool " << poolid << " pg_num " << pg_num
		    << " < my pg_num " << my_pg_num << dendl;
      for (unsigned i = pg_num; i < my_pg_num; ++i) {
	pg_t pgid(i, poolid);
	ldout(cct,20) << __func__ << " removing merged " << pgid << dendl;
	if (pgmap.pg_stat.count(pgid)) {
	  pending_inc->pg_remove.insert(pgid);
	}
	pending_inc->pg_stat_updates.erase(pgid);
      }
    }
  }
  auto i = pending_inc->pg_stat_updates.begin();
  while (i != pending_inc->pg_stat_updates.end()) {
    auto j = new_pg_num.find(i->first.pool());
    if (j == new_pg_num.end() ||
	i->first.ps() >= j->second) {
      ldout(cct,20) << __func__ << " removing pending update to old "
		    << i->first << dendl;
      i = pending_inc->pg_stat_updates.erase(i);
    } else {
      ++i;
    }
  }
}

static void _try_mark_pg_stale(
  const OSDMap& osdmap,
  pg_t pgid,
  const pg_stat_t& cur,
  PGMap::Incremental *pending_inc)
{
  if ((cur.state & PG_STATE_STALE) == 0 &&
      cur.acting_primary != -1 &&
      osdmap.is_down(cur.acting_primary)) {
    pg_stat_t *newstat;
    auto q = pending_inc->pg_stat_updates.find(pgid);
    if (q != pending_inc->pg_stat_updates.end()) {
      if ((q->second.acting_primary == cur.acting_primary) ||
	  ((q->second.state & PG_STATE_STALE) == 0 &&
	   q->second.acting_primary != -1 &&
	   osdmap.is_down(q->second.acting_primary))) {
	newstat = &q->second;
      } else {
	// pending update is no longer down or already stale
	return;
      }
    } else {
      newstat = &pending_inc->pg_stat_updates[pgid];
      *newstat = cur;
    }
    dout(10) << __func__ << " marking pg " << pgid
	     << " stale (acting_primary " << newstat->acting_primary
	     << ")" << dendl;
    newstat->state |= PG_STATE_STALE;
    newstat->last_unstale = ceph_clock_now();
  }

}

void PGMapUpdater::check_down_pgs(
    const OSDMap &osdmap,
    const PGMap &pg_map,
    bool check_all,
    const set<int>& need_check_down_pg_osds,
    PGMap::Incremental *pending_inc)
{
  // if a large number of osds changed state, just iterate over the whole
  // pg map.
  if (need_check_down_pg_osds.size() > (unsigned)osdmap.get_num_osds() *
      g_conf().get_val<double>("mon_pg_check_down_all_threshold")) {
    check_all = true;
  }

  if (check_all) {
    for (const auto& p : pg_map.pg_stat) {
      _try_mark_pg_stale(osdmap, p.first, p.second, pending_inc);
    }
  } else {
    for (auto osd : need_check_down_pg_osds) {
      if (osdmap.is_down(osd)) {
	auto p = pg_map.pg_by_osd.find(osd);
	if (p == pg_map.pg_by_osd.end()) {
	  continue;
	}
	for (auto pgid : p->second) {
	  const pg_stat_t &stat = pg_map.pg_stat.at(pgid);
	  ceph_assert(stat.acting_primary == osd);
	  _try_mark_pg_stale(osdmap, pgid, stat, pending_inc);
	}
      }
    }
  }
}

int reweight::by_utilization(
    const OSDMap &osdmap,
    const PGMap &pgm,
    int oload,
    double max_changef,
    int max_osds,
    bool by_pg, const set<int64_t> *pools,
    bool no_increasing,
    mempool::osdmap::map<int32_t, uint32_t>* new_weights,
    std::stringstream *ss,
    std::string *out_str,
    ceph::Formatter *f)
{
  if (oload <= 100) {
    *ss << "You must give a percentage higher than 100. "
      "The reweighting threshold will be calculated as <average-utilization> "
      "times <input-percentage>. For example, an argument of 200 would "
      "reweight OSDs which are twice as utilized as the average OSD.\n";
    return -EINVAL;
  }

  vector<int> pgs_by_osd(osdmap.get_max_osd());

  // Avoid putting a small number (or 0) in the denominator when calculating
  // average_util
  double average_util;
  if (by_pg) {
    // by pg mapping
    double weight_sum = 0.0;      // sum up the crush weights
    unsigned num_pg_copies = 0;
    int num_osds = 0;
    for (const auto& pg : pgm.pg_stat) {
      if (pools && pools->count(pg.first.pool()) == 0)
	continue;
      for (const auto acting : pg.second.acting) {
        if (!osdmap.exists(acting)) {
          continue;
        }
	if (acting >= (int)pgs_by_osd.size())
	  pgs_by_osd.resize(acting);
	if (pgs_by_osd[acting] == 0) {
          if (osdmap.crush->get_item_weightf(acting) <= 0) {
            //skip if we currently can not identify item
            continue;
          }
	  weight_sum += osdmap.crush->get_item_weightf(acting);
	  ++num_osds;
	}
	++pgs_by_osd[acting];
	++num_pg_copies;
      }
    }

    if (!num_osds || (num_pg_copies / num_osds < g_conf()->mon_reweight_min_pgs_per_osd)) {
      *ss << "Refusing to reweight: we only have " << num_pg_copies
	  << " PGs across " << num_osds << " osds!\n";
      return -EDOM;
    }

    average_util = (double)num_pg_copies / weight_sum;
  } else {
    // by osd utilization
    int num_osd = std::max<size_t>(1, pgm.osd_stat.size());
    if ((uint64_t)pgm.osd_sum.statfs.total / num_osd
	< g_conf()->mon_reweight_min_bytes_per_osd) {
      *ss << "Refusing to reweight: we only have " << pgm.osd_sum.statfs.kb()
	  << " kb across all osds!\n";
      return -EDOM;
    }
    if ((uint64_t)pgm.osd_sum.statfs.get_used_raw() / num_osd
	< g_conf()->mon_reweight_min_bytes_per_osd) {
      *ss << "Refusing to reweight: we only have "
	  << pgm.osd_sum.statfs.kb_used_raw()
	  << " kb used across all osds!\n";
      return -EDOM;
    }

    average_util = (double)pgm.osd_sum.statfs.get_used_raw() /
      (double)pgm.osd_sum.statfs.total;
  }

  // adjust down only if we are above the threshold
  const double overload_util = average_util * (double)oload / 100.0;

  // but aggressively adjust weights up whenever possible.
  const double underload_util = average_util;

  const unsigned max_change = (unsigned)(max_changef * (double)CEPH_OSD_IN);

  ostringstream oss;
  if (f) {
    f->open_object_section("reweight_by_utilization");
    f->dump_int("overload_min", oload);
    f->dump_float("max_change", max_changef);
    f->dump_int("max_change_osds", max_osds);
    f->dump_float("average_utilization", average_util);
    f->dump_float("overload_utilization", overload_util);
  } else {
    oss << "oload " << oload << "\n";
    oss << "max_change " << max_changef << "\n";
    oss << "max_change_osds " << max_osds << "\n";
    oss.precision(4);
    oss << "average_utilization " << std::fixed << average_util << "\n";
    oss << "overload_utilization " << overload_util << "\n";
  }
  int num_changed = 0;

  // precompute util for each OSD
  std::vector<std::pair<int, float> > util_by_osd;
  for (const auto& p : pgm.osd_stat) {
    std::pair<int, float> osd_util;
    osd_util.first = p.first;
    if (by_pg) {
      if (p.first >= (int)pgs_by_osd.size() ||
        pgs_by_osd[p.first] == 0) {
        // skip if this OSD does not contain any pg
        // belonging to the specified pool(s).
        continue;
      }

      if (osdmap.crush->get_item_weightf(p.first) <= 0) {
        // skip if we are unable to locate item.
        continue;
      }

      osd_util.second =
	pgs_by_osd[p.first] / osdmap.crush->get_item_weightf(p.first);
    } else {
      osd_util.second =
	(double)p.second.statfs.get_used_raw() / (double)p.second.statfs.total;
    }
    util_by_osd.push_back(osd_util);
  }

  // sort by absolute deviation from the mean utilization,
  // in descending order.
  std::sort(util_by_osd.begin(), util_by_osd.end(),
    [average_util](std::pair<int, float> l, std::pair<int, float> r) {
      return abs(l.second - average_util) > abs(r.second - average_util);
    }
  );

  if (f)
    f->open_array_section("reweights");

  for (const auto& p : util_by_osd) {
    unsigned weight = osdmap.get_weight(p.first);
    if (weight == 0) {
      // skip if OSD is currently out
      continue;
    }
    float util = p.second;

    if (util >= overload_util) {
      // Assign a lower weight to overloaded OSDs. The current weight
      // is a factor to take into account the original weights,
      // to represent e.g. differing storage capacities
      unsigned new_weight = (unsigned)((average_util / util) * (float)weight);
      if (weight > max_change)
	new_weight = std::max(new_weight, weight - max_change);
      new_weights->insert({p.first, new_weight});
      if (f) {
	f->open_object_section("osd");
	f->dump_int("osd", p.first);
	f->dump_float("weight", (float)weight / (float)CEPH_OSD_IN);
	f->dump_float("new_weight", (float)new_weight / (float)CEPH_OSD_IN);
	f->close_section();
      } else {
        oss << "osd." << p.first << " weight "
            << (float)weight / (float)CEPH_OSD_IN << " -> "
            << (float)new_weight / (float)CEPH_OSD_IN << "\n";
      }
      if (++num_changed >= max_osds)
	break;
    }
    if (!no_increasing && util <= underload_util) {
      // assign a higher weight.. if we can.
      unsigned new_weight = (unsigned)((average_util / util) * (float)weight);
      new_weight = std::min(new_weight, weight + max_change);
      if (new_weight > CEPH_OSD_IN)
	new_weight = CEPH_OSD_IN;
      if (new_weight > weight) {
	new_weights->insert({p.first, new_weight});
        oss << "osd." << p.first << " weight "
            << (float)weight / (float)CEPH_OSD_IN << " -> "
            << (float)new_weight / (float)CEPH_OSD_IN << "\n";
	if (++num_changed >= max_osds)
	  break;
      }
    }
  }
  if (f) {
    f->close_section();
  }

  OSDMap newmap;
  newmap.deepish_copy_from(osdmap);
  OSDMap::Incremental newinc;
  newinc.fsid = newmap.get_fsid();
  newinc.epoch = newmap.get_epoch() + 1;
  newinc.new_weight = *new_weights;
  newmap.apply_incremental(newinc);

  osdmap.summarize_mapping_stats(&newmap, pools, out_str, f);

  if (f) {
    f->close_section();
  } else {
    *out_str += "\n";
    *out_str += oss.str();
  }
  return num_changed;
}
