#include "CreatingPGs.h"

void CreatingPGs::add(const pg_t& pgid, const pg_stat_t &s)
{
  if ((s.state & PG_STATE_CREATING) &&
      s.parent_split_bits == 0) {
    creating_pgs.insert(pgid);
    if (s.acting_primary >= 0) {
      creating_pgs_by_osd_epoch[s.acting_primary][s.mapping_epoch].insert(pgid);
    }
  }
}

void CreatingPGs::sub(const pg_t& pgid, const pg_stat_t &s)
{
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
}
