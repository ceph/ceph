#include <errno.h>
#include <map>

#include "config.h"
#include "include/str_list.h"

#include "Crypto.h"
#include "auth/RotatingKeyRing.h"
#include "auth/KeyRing.h"

#define DOUT_SUBSYS auth
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "auth: "


bool RotatingKeyRing::need_new_secrets()
{
  Mutex::Locker l(lock);
  return secrets.need_new_secrets();
}
bool RotatingKeyRing::need_new_secrets(utime_t now)
{
  Mutex::Locker l(lock);
  return secrets.need_new_secrets(now);
}

void RotatingKeyRing::set_secrets(RotatingSecrets& s)
{
  Mutex::Locker l(lock);
  secrets = s;
  dump_rotating();
}

void RotatingKeyRing::dump_rotating()
{
  dout(10) << "dump_rotating:" << dendl;
  for (map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.secrets.begin();
       iter != secrets.secrets.end();
       ++iter)
    dout(10) << " id " << iter->first << " " << iter->second << dendl;
}

bool RotatingKeyRing::get_secret(EntityName& name, CryptoKey& secret)
{
  Mutex::Locker l(lock);
  return keyring->get_secret(name, secret);
}

bool RotatingKeyRing::get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret)
{
  Mutex::Locker l(lock);

  if (service_id != this->service_id) {
    dout(0) << "do not have service " << ceph_entity_type_name(service_id)
	    << ", i am " << ceph_entity_type_name(this->service_id) << dendl;
    return false;
  }

  map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.secrets.find(secret_id);
  if (iter == secrets.secrets.end()) {
    dout(0) << "could not find secret_id=" << secret_id << dendl;
    dump_rotating();
    return false;
  }

  secret = iter->second.key;
  return true;
}
