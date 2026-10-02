#include <map>

#include "common/debug.h"
#include "auth/RotatingKeyRing.h"
#include "auth/KeyRing.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "RKR(" << this << ")." << __func__ << " "

RotatingKeyRing::RotatingKeyRing(CephContext *cct_, uint32_t s, KeyRing *kr)
  :
    cct(cct_),
    service_id(s),
    keyring(kr),
    lock{ceph::make_mutex("RotatingKeyRing::lock")}
{
  ldout(cct, 10) << "con" << dendl;
}

RotatingKeyRing::~RotatingKeyRing()
{
  ldout(cct, 10) << "des" << dendl;
}

bool RotatingKeyRing::need_new_secrets() const
{
  std::lock_guard l{lock};
  auto answer = secrets.need_new_secrets();
  ldout(cct, 10) << "need_new_secrets: " << answer << dendl;
  return answer;
}

bool RotatingKeyRing::need_new_secrets(utime_t now) const
{
  std::lock_guard l{lock};
  auto answer = secrets.need_new_secrets(now);
  ldout(cct, 10) << " now=" << now << ": " << answer << dendl;
  return answer;
}

void RotatingKeyRing::set_secrets(RotatingSecrets&& s)
{
  std::lock_guard l{lock};
  ldout(cct, 10) << dendl;
  secrets = std::move(s);
  dump_rotating();
}

void RotatingKeyRing::wipe()
{
  ldout(cct, 10) << dendl;
  secrets.wipe();
}

void RotatingKeyRing::dump_rotating() const
{
  ldout(cct, 10) << "dump_rotating:" << dendl;
  for (auto iter = secrets.secrets.begin();
       iter != secrets.secrets.end();
       ++iter)
    ldout(cct, 10) << " id " << iter->first << " " << iter->second << dendl;
}

bool RotatingKeyRing::get_secret(const EntityName& name, CryptoKey& secret) const
{
  std::lock_guard l{lock};
  ldout(cct, 10) << name << dendl;
  return keyring->get_secret(name, secret);
}

bool RotatingKeyRing::get_service_secret(uint32_t service_id_, uint64_t secret_id,
					 CryptoKey& secret) const
{
  ldout(cct, 30) << __func__ << ": service_id=" << service_id_ << " secret_id=" << secret_id << dendl;

  std::lock_guard l{lock};

  if (service_id_ != this->service_id) {
    ldout(cct, 0) << "do not have service " << ceph_entity_type_name(service_id_)
	    << ", i am " << ceph_entity_type_name(this->service_id) << dendl;
    return false;
  }

  auto iter = secrets.secrets.find(secret_id);
  if (iter == secrets.secrets.end()) {
    ldout(cct, 0) << "could not find secret_id=" << secret_id << dendl;
    dump_rotating();
    return false;
  }

  secret = iter->second.key;
  return true;
}

KeyRing* RotatingKeyRing::get_keyring()
{
  ldout(cct, 10) << dendl;
  return keyring;
}
