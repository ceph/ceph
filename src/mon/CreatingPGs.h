#include <map>
#include <set>

#include "osd/osd_types.h"

class CreatingPGs
{
  std::set<pg_t> creating_pgs;
  std::map<int,map<epoch_t,set<pg_t> > > creating_pgs_by_osd_epoch;

public:
  void add(const pg_t& pgid, const pg_stat_t &s);
  void sub(const pg_t& pgid, const pg_stat_t &s);
  size_t size() const {
    return creating_pgs.size();
  }
  bool contains(const pg_t& pgid) const {
    return creating_pgs.count(pgid);
  }
  template<typename Func>
  void with_pgs_of_osd(int osd, epoch_t next, Func&& func) const {
    auto pgs = creating_pgs_by_osd_epoch.find(osd);
    if (pgs == creating_pgs_by_osd_epoch.end())
      return;
    assert(!pgs->second.empty());
    for (auto epoch_pgs = pgs->second.lower_bound(next);
         epoch_pgs != pgs->second.end();
         ++epoch_pgs) {
      std::forward<Func>(func)(epoch_pgs->first, epoch_pgs->second);
    }
  }
  template<typename Func> void with_pg(Func&& func) const {
    for (auto& pg : creating_pgs) {
      std::forward<Func>(func)(pg);
    }
  }
};
