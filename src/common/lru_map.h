#ifndef CEPH_LRU_MAP_H
#define CEPH_LRU_MAP_H

#include <list>
#include <map>
#include "common/Mutex.h"


template <class K, class V>
class lru_map {
  struct entry {
    V value;
    typename std::list<K>::iterator lru_iter;
  };

  std::map<K, entry> tokens;
  std::list<K> tokens_lru;

  Mutex lock;

  size_t max;

public:
  lru_map(int _max) : lock("lru_map"), max(_max) {}
  virtual ~lru_map() {}

  bool find(const K& key, V& value);
  void add(const K& key, V& value);
  void erase(const K& key);
};

template <class K, class V>
bool lru_map<K, V>::find(const K& key, V& value)
{
  lock.Lock();
  typename std::map<K, entry>::iterator iter = tokens.find(key);
  if (iter == tokens.end()) {
    lock.Unlock();
    return false;
  }

  entry& e = iter->second;
  tokens_lru.erase(e.lru_iter);

  value = e.value;

  tokens_lru.push_front(key);
  e.lru_iter = tokens_lru.begin();

  lock.Unlock();

  return true;
}

template <class K, class V>
void lru_map<K, V>::add(const K& key, V& value)
{
  lock.Lock();
  typename std::map<K, entry>::iterator iter = tokens.find(key);
  if (iter != tokens.end()) {
    entry& e = iter->second;
    tokens_lru.erase(e.lru_iter);
  }

  tokens_lru.push_front(key);
  entry& e = tokens[key];
  e.value = value;
  e.lru_iter = tokens_lru.begin();

  while (tokens_lru.size() > max) {
    typename std::list<K>::reverse_iterator riter = tokens_lru.rbegin();
    iter = tokens.find(*riter);
    // assert(iter != tokens.end());
    tokens.erase(iter);
    tokens_lru.pop_back();
  }
  
  lock.Unlock();
}

template <class K, class V>
void lru_map<K, V>::erase(const K& key)
{
  Mutex::Locker l(lock);
  typename std::map<K, entry>::iterator iter = tokens.find(key);
  if (iter == tokens.end())
    return;

  entry& e = iter->second;
  tokens_lru.erase(e.lru_iter);
  tokens.erase(iter);
}

#endif
