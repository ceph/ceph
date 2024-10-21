// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "KeyValueDB.h"
#include "RocksDBStore.h"
#include "include/buffer.h"
#include "include/encoding_string.h"

using std::map;
using std::string;

void KeyValueDB::TransactionImpl::set(
  const std::string &prefix,                      ///< [in] Prefix for keys, or CF name
  const std::map<std::string, ceph::buffer::list> &to_set ///< [in] keys/values to set
) {
  for (auto it = to_set.cbegin(); it != to_set.cend(); ++it)
    set(prefix, it->first, it->second);
}

void KeyValueDB::TransactionImpl::set(
  const std::string &prefix,      ///< [in] prefix, or CF name
  ceph::buffer::list& to_set_bl           ///< [in] encoded key/values to set
) {
  using ceph::decode;
  auto p = std::cbegin(to_set_bl);
  uint32_t num;
  decode(num, p);
  while (num--) {
    std::string key;
    ceph::buffer::list value;
    decode(key, p);
    decode(value, p);
    set(prefix, key, value);
  }
}

void KeyValueDB::TransactionImpl::rmkeys(
  const std::string &prefix,     ///< [in] Prefix or CF to search for
  ceph::buffer::list &keys_bl            ///< [in] Keys to remove
) {
  using ceph::decode;
  auto p = std::cbegin(keys_bl);
  uint32_t num;
  decode(num, p);
  while (num--) {
    std::string key;
    decode(key, p);
    rmkey(prefix, key);
  }
}

void KeyValueDB::TransactionImpl::rmkeys(
  const std::string &prefix,        ///< [in] Prefix/CF to search for
  const std::set<std::string> &keys ///< [in] Keys to remove
) {
  for (auto it = keys.cbegin(); it != keys.cend(); ++it)
    rmkey(prefix, *it);
}

KeyValueDB *KeyValueDB::create(CephContext *cct, const string& type,
			       const string& dir,
			       map<string,string> options,
			       void *p)
{
  if (type == "rocksdb") {
    return new RocksDBStore(cct, dir, options, p);
  }
  return NULL;
}

int KeyValueDB::test_init(const string& type, const string& dir)
{
  if (type == "rocksdb") {
    return RocksDBStore::_test_init(dir);
  }
  return -EINVAL;
}

int KeyValueDB::get(const std::string &prefix, ///< [in] prefix or CF name
		    const std::string &key,    ///< [in] key
		    ceph::buffer::list *value) {       ///< [out] value
  std::set<std::string> ks;
  ks.insert(key);
  std::map<std::string,ceph::buffer::list> om;
  int r = get(prefix, ks, &om);
  if (om.find(key) != om.end()) {
    *value = std::move(om[key]);
  } else {
    *value = ceph::buffer::list();
    r = -ENOENT;
  }
  return r;
}

int KeyValueDB::get(const std::string &prefix,
		    const char *key, size_t keylen,
		    ceph::buffer::list *value) {
  return get(prefix, std::string(key, keylen), value);
}

ceph::buffer::ptr KeyValueDB::IteratorImpl::value_as_ptr() {
  ceph::buffer::list bl = value();
  if (bl.length() == 1) {
    return *bl.buffers().begin();
  } else if (bl.length() == 0) {
    return ceph::buffer::ptr();
  } else {
    ceph_abort();
  }
}

ceph::buffer::ptr KeyValueDB::WholeSpaceIteratorImpl::value_as_ptr() {
  ceph::buffer::list bl = value();
  if (bl.length()) {
    return *bl.buffers().begin();
  } else {
    return ceph::buffer::ptr();
  }
}

ceph::buffer::list KeyValueDB::PrefixIteratorImpl::value() {
  return generic_iter->value();
}

ceph::buffer::ptr KeyValueDB::PrefixIteratorImpl::value_as_ptr() {
  return generic_iter->value_as_ptr();
}
