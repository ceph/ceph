// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "LevelDBStore.h"

#include <set>
#include <map>
#include <string>
#include <tr1/memory>
#include "leveldb/include/leveldb/db.h"
#include "leveldb/include/leveldb/write_batch.h"
#include "leveldb/include/leveldb/slice.h"
#include <errno.h>
using std::string;

int LevelDBStore::init(ostream &out)
{
  leveldb::Options options;
  options.create_if_missing = true;
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
  const std::map<string, bufferlist> &to_set)
{
  for (std::map<string, bufferlist>::const_iterator i = to_set.begin();
       i != to_set.end();
       ++i) {
    buffers.push_back(i->second);
    buffers.rbegin()->rebuild();
    bufferlist &bl = *(buffers.rbegin());
    string key = combine_strings(prefix, i->first);
    keys.push_back(key);
    bat.Delete(leveldb::Slice(*(keys.rbegin())));
    bat.Put(leveldb::Slice(*(keys.rbegin())),
	    leveldb::Slice(bl.c_str(), bl.length()));
  }
}
void LevelDBStore::LevelDBTransactionImpl::rmkeys(const string &prefix,
						  const std::set<string> &to_rm)
{
  for (std::set<string>::const_iterator i = to_rm.begin();
       i != to_rm.end();
       ++i) {
    string key = combine_strings(prefix, *i);
    keys.push_back(key);
    bat.Delete(leveldb::Slice(*(keys.rbegin())));
  }
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
