#include "MonOpRequest.h"
#include "mon/Monitor.h"

MonOpRequest::~MonOpRequest()
{
        request->put();
        // certain ops may not have a session (e.g., AUTH or PING)
        if (session)
                // if only ref count == 1, then we need to cal remove_session
                if ( session->get_nref() == 1 ) {
                        Monitor::get_instance()->remove_session(session);
                }
        session->put();
}
