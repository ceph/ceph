
#include "include/types.h"
#include "Messenger.h"

#include "SimpleMessenger.h"

Messenger *Messenger::create(CephContext *cct,
			     entity_name_t name,
			     uint64_t nonce)
{
  return new SimpleMessenger(cct, name, nonce);
}
