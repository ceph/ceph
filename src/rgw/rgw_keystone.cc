// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <fnmatch.h>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/types.h"
#include "include/str_list.h"

#include "rgw_common.h"
#include "rgw_keystone.h"

#define dout_subsys ceph_subsys_rgw

bool KeystoneToken::User::has_role(const string& r) {
  list<Role>::iterator iter;
  for (iter = roles.begin(); iter != roles.end(); ++iter) {
      if (fnmatch(r.c_str(), ((*iter).name.c_str()), 0) == 0) {
        return true;
      }
  }
  return false;
}

int KeystoneToken::parse(CephContext *cct, bufferlist& bl)
{
  JSONParser parser;
  if (!parser.parse(bl.c_str(), bl.length())) {
    ldout(cct, 0) << "Keystone token parse error: malformed json" << dendl;
    return -EINVAL;
  }

  try {
    JSONDecoder::decode_json("access", *this, &parser);
  } catch (JSONDecoder::err& err) {
    ldout(cct, 0) << "Keystone token parse error: " << err.message << dendl;
    return -EINVAL;
  }

  return 0;
}

bool RGWKeystoneTokenCache::find(const string& token_id, KeystoneToken& token)
{
  lock.Lock();
  map<string, token_entry>::iterator iter = tokens.find(token_id);
  if (iter == tokens.end()) {
    lock.Unlock();
    if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_miss);
    return false;
  }

  token_entry& entry = iter->second;
  tokens_lru.erase(entry.lru_iter);

  if (entry.token.expired()) {
    tokens.erase(iter);
    lock.Unlock();
    if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_hit);
    return false;
  }
  token = entry.token;

  tokens_lru.push_front(token_id);
  entry.lru_iter = tokens_lru.begin();

  lock.Unlock();
  if (perfcounter) perfcounter->inc(l_rgw_keystone_token_cache_hit);

  return true;
}

void RGWKeystoneTokenCache::add(const string& token_id, KeystoneToken& token)
{
  lock.Lock();
  map<string, token_entry>::iterator iter = tokens.find(token_id);
  if (iter != tokens.end()) {
    token_entry& e = iter->second;
    tokens_lru.erase(e.lru_iter);
  }

  tokens_lru.push_front(token_id);
  token_entry& entry = tokens[token_id];
  entry.token = token;
  entry.lru_iter = tokens_lru.begin();

  while (tokens_lru.size() > max) {
    list<string>::reverse_iterator riter = tokens_lru.rbegin();
    iter = tokens.find(*riter);
    assert(iter != tokens.end());
    tokens.erase(iter);
    tokens_lru.pop_back();
  }

  lock.Unlock();
}

void RGWKeystoneTokenCache::invalidate(const string& token_id)
{
  Mutex::Locker l(lock);
  map<string, token_entry>::iterator iter = tokens.find(token_id);
  if (iter == tokens.end())
    return;

  ldout(cct, 20) << "invalidating revoked token id=" << token_id << dendl;
  token_entry& e = iter->second;
  tokens_lru.erase(e.lru_iter);
  tokens.erase(iter);
}
