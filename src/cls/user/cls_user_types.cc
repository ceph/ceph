
#include "cls/user/cls_user_types.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"


void cls_user_stats::dump(Formatter *f) const
{
  f->dump_int("total_entries", total_entries);
  f->dump_int("total_bytes", total_bytes);
  f->dump_int("total_bytes_rounded", total_bytes_rounded);
}

void cls_user_header::dump(Formatter *f) const
{
  encode_json("stats", stats, f);
  encode_json("last_stats_sync", last_stats_sync, f);
  encode_json("last_stats_update", last_stats_update, f);
}
