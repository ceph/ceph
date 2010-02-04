#include <errno.h>
#include <map>

#include "config.h"
#include "include/str_list.h"

#include "Crypto.h"
#include "auth/RotatingKeyRing.h"

#define DOUT_SUBSYS auth
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "auth: "


bool RotatingKeyRing::need_new_secrets()
{
  Mutex::Locker l(lock);
  return secrets.need_new_secrets();
}

void RotatingKeyRing::set_secrets(RotatingSecrets& s)
{
  Mutex::Locker l(lock);
  secrets = s;
}

void RotatingKeyRing::dump_rotating()
{
  dout(0) << "dump_rotating:" << dendl;
  for (map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.secrets.begin();
       iter != secrets.secrets.end();
       ++iter)
    dout(0) << " id " << iter->first << " " << iter->second << dendl;
}

bool RotatingKeyRing::get_service_secret(uint64_t secret_id, CryptoKey& secret)
{
  Mutex::Locker l(lock);

  map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.secrets.find(secret_id);
  if (iter == secrets.secrets.end()) {
    dout(0) << "could not find secret_id=" << secret_id << dendl;
    dump_rotating();
    return false;
  }

  ExpiringCryptoKey& key = iter->second;
  if (key.expiration > g_clock.now()) {
    secret = key.key;
    return true;
  }
  dout(0) << "secret " << key << " expired!" << dendl;
  return false;
}
