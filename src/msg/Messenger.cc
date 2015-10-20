
#include "include/types.h"
#include "Messenger.h"

#include "SimpleMessenger.h"

Messenger *Messenger::create_client_messenger(CephContext *cct, string lname)
{
  uint64_t nonce = 0;
  get_random_bytes((char*)&nonce, sizeof(nonce));
  return Messenger::create(cct, entity_name_t::CLIENT(), lname, nonce);
}

Messenger *Messenger::create(CephContext *cct,
			     entity_name_t name,
			     string lname,
			     uint64_t nonce)
{
  return new SimpleMessenger(cct, name, lname, nonce);
}
