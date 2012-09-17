// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "LevelDBStore.h"

#include <set>
#include <map>
#include <string>
#include <tr1/memory>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/slice.h"
#include <errno.h>
using std::string;

int LevelDBStore::init(ostream &out, bool create_if_missing)
{
  leveldb::Options options;
  options.create_if_missing = create_if_missing;
  leveldb::DB *_db;
  leveldb::Status status = leveldb::DB::Open(options, path, &_db);
  db.reset(_db);
  if (!status.ok()) {
    out << status.ToString() << std::endl;
    return -EINVAL;
  } else
    return 0;
}

void LevelDBStore::LevelDBTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  buffers.push_back(to_set_bl);
  buffers.rbegin()->rebuild();
  bufferlist &bl = *(buffers.rbegin());
  string key = combine_strings(prefix, k);
  keys.push_back(key);
  bat.Delete(leveldb::Slice(*(keys.rbegin())));
  bat.Put(leveldb::Slice(*(keys.rbegin())),
	  leveldb::Slice(bl.c_str(), bl.length()));
}

void LevelDBStore::LevelDBTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  string key = combine_strings(prefix, k);
  keys.push_back(key);
  bat.Delete(leveldb::Slice(*(keys.rbegin())));
}

void LevelDBStore::LevelDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    keys.push_back(key);
    bat.Delete(*(keys.rbegin()));
  }
}

int LevelDBStore::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  KeyValueDB::Iterator it = get_iterator(prefix);
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    it->lower_bound(*i);
    if (it->valid() && it->key() == *i) {
      out->insert(make_pair(*i, it->value()));
    } else if (!it->valid())
      break;
  }
  return 0;
}

string LevelDBStore::combine_strings(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(0);
  out.append(value);
  return out;
}

bufferlist LevelDBStore::to_bufferlist(leveldb::Slice in)
{
  bufferlist bl;
  bl.append(bufferptr(in.data(), in.size()));
  return bl;
}

int LevelDBStore::split_key(leveldb::Slice in, string *prefix, string *key)
{
  string in_prefix = in.ToString();
  size_t prefix_len = in_prefix.find('\0');
  if (prefix_len >= in_prefix.size())
    return -EINVAL;

  if (prefix)
    *prefix = string(in_prefix, 0, prefix_len);
  if (key)
    *key= string(in_prefix, prefix_len + 1);
  return 0;
}
