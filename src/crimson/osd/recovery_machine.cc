#include "recovery_machine.h"
#include "recovery_state.h"
#include "pg.h"

namespace recovery
{
void Machine::send_notify(pg_shard_t to,
                          const pg_notify_t& info,
                          const PastIntervals& pi)
{
  state.context.notifies[to.osd].emplace_back(info, pi);
}

void Machine::send_query(pg_shard_t to,
                         const pg_query_t& query)
{
  spg_t pgid{pg.get_info().pgid.pgid, to.shard};
  state.context.queries[to.osd].emplace(pgid, query);
}

recovery::Context* Machine::get_context()
{
  return &state.context;
}

}
