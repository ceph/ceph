// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "include/encoding.h"
#include "KeyValueDBMemory.h"
#include <map>
#include <set>
#include <tr1/memory>
#include <iostream>

using namespace std;

class MemIterator : public KeyValueDB::IteratorImpl {
  string prefix;
  KeyValueDBMemory *db;

  bool ready;
  map<string, bufferlist>::iterator iter;

public:
  MemIterator(const string &prefix,
	      KeyValueDBMemory *db) :
    prefix(prefix), db(db), ready(false) {}

  int seek_to_first() {
    if (!db->db.count(prefix)) {
      ready = false;
      return 0;
    }
    iter = db->db[prefix].begin();
    ready = true;
    return 0;
  }

  int seek_to_last() {
    if (!db->db.count(prefix)) {
      ready = false;
      return 0;
    } else if (db->db[prefix].size() == 0) {
      iter = db->db[prefix].end();
    } else {
      iter = --db->db[prefix].end();
    }
    ready = true;
    return 0;
  }

  int lower_bound(const string &to) {
    if (!db->db.count(prefix)) {
      ready = false;
      return 0;
    }
    iter = db->db[prefix].lower_bound(to);
    ready = true;
    return 0;
  }

  int upper_bound(const string &after) {
    if (!db->db.count(prefix)) {
      ready = false;
      return 0;
    }
    iter = db->db[prefix].upper_bound(after);
    ready = true;
    return 0;
  }

  bool valid() {
    return ready && iter != db->db[prefix].end();
  }

  bool begin() {
    return ready && iter == db->db[prefix].begin();
  }

  int prev() {
    if (valid() && iter != db->db[prefix].begin())
      iter--;
    return 0;
  }

  int next() {
    if (valid())
      iter++;
    return 0;
  }

  string key() {
    if (valid())
      return iter->first;
    else
      return "";
  }

  bufferlist value() {
    if (valid())
      return iter->second;
    else
      return bufferlist();
  }

  int status() {
    return 0;
  }
};

int KeyValueDBMemory::get(const string &prefix,
			  const std::set<string> &key,
			  map<string, bufferlist> *out) {
  if (!db.count(prefix))
    return 0;

  for (std::set<string>::const_iterator i = key.begin();
       i != key.end();
       ++i) {
    if (db[prefix].count(*i))
      (*out)[*i] = db[prefix][*i];
  }
  return 0;
}

int KeyValueDBMemory::get_keys(const string &prefix,
			       const std::set<string> &key,
			       std::set<string> *out) {
  if (!db.count(prefix))
    return 0;

  for (std::set<string>::const_iterator i = key.begin();
       i != key.end();
       ++i) {
    if (db[prefix].count(*i))
      out->insert(*i);
  }
  return 0;
}

int KeyValueDBMemory::set(const string &prefix,
			  const map<string, bufferlist> &to_set) {
  for (map<string, bufferlist>::const_iterator i = to_set.begin();
       i != to_set.end();
       ++i) {
    bufferlist bl = i->second;
    db[prefix][i->first] = i->second;
  }
  return 0;
}

int KeyValueDBMemory::rmkeys(const string &prefix,
			     const std::set<string> &keys) {
  if (!db.count(prefix))
    return 0;
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    db[prefix].erase(*i);
  }
  return 0;
}

int KeyValueDBMemory::rmkeys_by_prefix(const string &prefix) {
  db.erase(prefix);
  return 0;
}

KeyValueDB::Iterator KeyValueDBMemory::get_iterator(const string &prefix) {
  return tr1::shared_ptr<IteratorImpl>(new MemIterator(prefix, this));
}
