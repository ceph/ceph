#include "recovery_state.h"
#include "recovery_states.h"
#include "messages/MOSDPGLog.h" // MLogRec needs this

namespace recovery {

State::State(PG& pg)
  : machine{*this, pg}
{
    machine.initiate();
}

Context State::handle_event(const boost::statechart::event_base& evt)
{
  machine.process_event(evt);
  Context pending;
  std::swap(pending, context);
  return pending;
}
}
