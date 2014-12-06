
#include "include/types.h"
#include "Messenger.h"

#include "msg/simple/SimpleMessenger.h"
#include "msg/async/AsyncMessenger.h"

Messenger *Messenger::create(CephContext *cct, const string &type,
                             entity_name_t name, string lname,
                             uint64_t nonce)
{
  int r = -1;
  if (type == "random")
    r = rand() % 2;
  if (r == 0 || type == "simple")
    return new SimpleMessenger(cct, name, lname, nonce);
  else if (r == 1 || type == "async")
    return new AsyncMessenger(cct, name, lname, nonce);
  lderr(cct) << "unrecognized ms_type '" << cct->_conf->ms_type << "'" << dendl;
  return NULL;
}
