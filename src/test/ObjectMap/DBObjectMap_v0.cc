// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <iostream>
#include <inttypes.h>
#include "include/buffer.h"
#include <set>
#include <map>
#include <string>
#include <tr1/memory>

#include <string>
#include <vector>
#include <tr1/memory>

#include "os/IndexManager.h"
#include "os/ObjectMap.h"
#include "os/KeyValueDB.h"
#include "DBObjectMap_v0.h"
#include <errno.h>

#include "common/debug.h"
#include "common/config.h"
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "filestore "

const string DBObjectMapv0::USER_PREFIX = "_USER_";
const string DBObjectMapv0::XATTR_PREFIX = "_AXATTR_";
const string DBObjectMapv0::SYS_PREFIX = "_SYS_";
const string DBObjectMapv0::COMPLETE_PREFIX = "_COMPLETE_";
const string DBObjectMapv0::HEADER_KEY = "HEADER";
const string DBObjectMapv0::USER_HEADER_KEY = "USER_HEADER";
const string DBObjectMapv0::LEAF_PREFIX = "_LEAF_";
const string DBObjectMapv0::GLOBAL_STATE_KEY = "HEADER";
const string DBObjectMapv0::REVERSE_LEAF_PREFIX = "_REVLEAF_";

static void append_escaped(const string &in, string *out)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%') {
      out->push_back('%');
      out->push_back('p');
    } else if (*i == '.') {
      out->push_back('%');
      out->push_back('e');
    } else if (*i == '_') {
      out->push_back('%');
      out->push_back('u');
    } else {
      out->push_back(*i);
    }
  }
}

bool DBObjectMapv0::check(std::ostream &out)
{
  bool retval = true;
  map<uint64_t, uint64_t> parent_to_num_children;
  map<uint64_t, uint64_t> parent_to_actual_num_children;
  KeyValueDB::Iterator iter = db->get_iterator(LEAF_PREFIX);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    _Header header;
    bufferlist bl = iter->value();
    while (true) {
      bufferlist::iterator bliter = bl.begin();
      header.decode(bliter);
      if (header.seq != 0)
	parent_to_actual_num_children[header.seq] = header.num_children;
      if (header.parent == 0)
	break;

      if (!parent_to_num_children.count(header.parent))
	parent_to_num_children[header.parent] = 0;
      parent_to_num_children[header.parent]++;
      if (parent_to_actual_num_children.count(header.parent))
	break;

      set<string> to_get;
      map<string, bufferlist> got;
      to_get.insert(HEADER_KEY);
      db->get(sys_parent_prefix(header), to_get, &got);
      if (!got.size()) {
	out << "Missing: seq " << header.parent << std::endl;
	retval = false;
	break;
      } else {
	bl = got.begin()->second;
      }
    }
  }

  for (map<uint64_t, uint64_t>::iterator i = parent_to_num_children.begin();
       i != parent_to_num_children.end();
       parent_to_num_children.erase(i++)) {
    if (!parent_to_actual_num_children.count(i->first))
      continue;
    if (parent_to_actual_num_children[i->first] != i->second) {
      out << "Invalid: seq " << i->first << " recorded children: "
	  << parent_to_actual_num_children[i->first] << " found: "
	  << i->second << std::endl;
      retval = false;
    }
    parent_to_actual_num_children.erase(i->first);
  }
  return retval;
}

string DBObjectMapv0::hobject_key(coll_t c, const hobject_t &hoid)
{
  string out;
  append_escaped(c.to_str(), &out);
  out.push_back('.');
  append_escaped(hoid.oid.name, &out);
  out.push_back('.');
  append_escaped(hoid.get_key(), &out);
  out.push_back('.');

  char snap_with_hash[1000];
  char *t = snap_with_hash;
  char *end = t + sizeof(snap_with_hash);
  if (hoid.snap == CEPH_NOSNAP)
    t += snprintf(t, end - t, ".head");
  else if (hoid.snap == CEPH_SNAPDIR)
    t += snprintf(t, end - t, ".snapdir");
  else
    t += snprintf(t, end - t, ".%llx", (long long unsigned)hoid.snap);
  snprintf(t, end - t, ".%.*X", (int)(sizeof(hoid.hash)*2), hoid.hash);
  out += string(snap_with_hash);
  return out;
}

string DBObjectMapv0::map_header_key(coll_t c, const hobject_t &hoid)
{
  return hobject_key(c, hoid);
}

string DBObjectMapv0::header_key(uint64_t seq)
{
  char buf[100];
  snprintf(buf, sizeof(buf), "%.*" PRId64, (int)(2*sizeof(seq)), seq);
  return string(buf);
}

string DBObjectMapv0::complete_prefix(Header header)
{
  return USER_PREFIX + header_key(header->seq) + COMPLETE_PREFIX;
}

string DBObjectMapv0::user_prefix(Header header)
{
  return USER_PREFIX + header_key(header->seq) + USER_PREFIX;
}

string DBObjectMapv0::sys_prefix(Header header)
{
  return USER_PREFIX + header_key(header->seq) + SYS_PREFIX;
}

string DBObjectMapv0::xattr_prefix(Header header)
{
  return USER_PREFIX + header_key(header->seq) + XATTR_PREFIX;
}

string DBObjectMapv0::sys_parent_prefix(_Header header)
{
  return USER_PREFIX + header_key(header.parent) + SYS_PREFIX;
}

int DBObjectMapv0::DBObjectMapv0IteratorImpl::init()
{
  invalid = false;
  if (ready) {
    return 0;
  }
  assert(!parent_iter);
  if (header->parent) {
    Header parent = map->lookup_parent(header);
    if (!parent) {
      assert(0);
      return -EINVAL;
    }
    parent_iter.reset(new DBObjectMapv0IteratorImpl(map, parent));
  }
  key_iter = map->db->get_iterator(map->user_prefix(header));
  assert(key_iter);
  complete_iter = map->db->get_iterator(map->complete_prefix(header));
  assert(complete_iter);
  cur_iter = key_iter;
  assert(cur_iter);
  ready = true;
  return 0;
}

ObjectMap::ObjectMapIterator DBObjectMapv0::get_iterator(
  const hobject_t &hoid,
  Index index)
{
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return ObjectMapIterator(new EmptyIteratorImpl());
  return _get_iterator(header);
}

int DBObjectMapv0::DBObjectMapv0IteratorImpl::seek_to_first()
{
  init();
  r = 0;
  if (parent_iter) {
    r = parent_iter->seek_to_first();
    if (r < 0)
      return r;
  }
  r = key_iter->seek_to_first();
  if (r < 0)
    return r;
  return adjust();
}

int DBObjectMapv0::DBObjectMapv0IteratorImpl::seek_to_last()
{
  init();
  r = 0;
  if (parent_iter) {
    r = parent_iter->seek_to_last();
    if (r < 0)
      return r;
    if (parent_iter->valid())
      r = parent_iter->next();
    if (r < 0)
      return r;
  }
  r = key_iter->seek_to_last();
  if (r < 0)
    return r;
  if (key_iter->valid())
    r = key_iter->next();
  if (r < 0)
    return r;
  return adjust();
}

int DBObjectMapv0::DBObjectMapv0IteratorImpl::lower_bound(const string &to)
{
  init();
  r = 0;
  if (parent_iter) {
    r = parent_iter->lower_bound(to);
    if (r < 0)
      return r;
  }
  r = key_iter->lower_bound(to);
  if (r < 0)
    return r;
  return adjust();
}

int DBObjectMapv0::DBObjectMapv0IteratorImpl::upper_bound(const string &after)
{
  init();
  r = 0;
  if (parent_iter) {
    r = parent_iter->upper_bound(after);
    if (r < 0)
      return r;
  }
  r = key_iter->upper_bound(after);
  if (r < 0)
    return r;
  return adjust();
}

bool DBObjectMapv0::DBObjectMapv0IteratorImpl::valid()
{
  bool valid = !invalid && ready;
  assert(!valid || cur_iter->valid());
  return valid;
}

bool DBObjectMapv0::DBObjectMapv0IteratorImpl::valid_parent()
{
  if (parent_iter && parent_iter->valid() &&
      (!key_iter->valid() || key_iter->key() > parent_iter->key()))
    return true;
  return false;
}

int DBObjectMapv0::DBObjectMapv0IteratorImpl::next()
{
  assert(cur_iter->valid());
  assert(valid());
  cur_iter->next();
  return adjust();
}

int DBObjectMapv0::DBObjectMapv0IteratorImpl::next_parent()
{
  if (!parent_iter || !parent_iter->valid()) {
    invalid = true;
    return 0;
  }
  r = next();
  if (r < 0)
    return r;
  if (!valid() || on_parent() || !parent_iter->valid())
    return 0;

  return lower_bound(parent_iter->key());
}

int DBObjectMapv0::DBObjectMapv0IteratorImpl::in_complete_region(const string &to_test,
							     string *begin,
							     string *end)
{
  complete_iter->upper_bound(to_test);
  if (complete_iter->valid())
    complete_iter->prev();
  else
    complete_iter->seek_to_last();

  if (!complete_iter->valid())
    return false;

  string _end;
  if (begin)
    *begin = complete_iter->key();
  _end = string(complete_iter->value().c_str());
  if (end)
    *end = _end;
  return (to_test >= complete_iter->key()) && (!_end.size() || _end > to_test);
}

/**
 * Moves parent_iter to the next position both out of the complete_region and 
 * not equal to key_iter.  Then, we set cur_iter to parent_iter if valid and
 * less than key_iter and key_iter otherwise.
 */
int DBObjectMapv0::DBObjectMapv0IteratorImpl::adjust()
{
  string begin, end;
  while (parent_iter && parent_iter->valid()) {
    if (in_complete_region(parent_iter->key(), &begin, &end)) {
      if (end.size() == 0) {
	parent_iter->seek_to_last();
	if (parent_iter->valid())
	  parent_iter->next();
      } else
	parent_iter->lower_bound(end);
    } else if (key_iter->valid() && key_iter->key() == parent_iter->key()) {
      parent_iter->next();
    } else {
      break;
    }
  }
  if (valid_parent()) {
    cur_iter = parent_iter;
  } else if (key_iter->valid()) {
    cur_iter = key_iter;
  } else {
    invalid = true;
  }
  assert(invalid || cur_iter->valid());
  return 0;
}


string DBObjectMapv0::DBObjectMapv0IteratorImpl::key()
{
  return cur_iter->key();
}

bufferlist DBObjectMapv0::DBObjectMapv0IteratorImpl::value()
{
  return cur_iter->value();
}

int DBObjectMapv0::DBObjectMapv0IteratorImpl::status()
{
  return r;
}

int DBObjectMapv0::set_keys(const hobject_t &hoid,
			  Index index,
			  const map<string, bufferlist> &set)
{
  KeyValueDB::Transaction t = db->get_transaction();
  Header header = lookup_create_map_header(index->coll(), hoid, t);
  if (!header)
    return -EINVAL;

  t->set(user_prefix(header), set);

  return db->submit_transaction(t);
}

int DBObjectMapv0::set_header(const hobject_t &hoid,
			    Index index,
			    const bufferlist &bl)
{
  KeyValueDB::Transaction t = db->get_transaction();
  Header header = lookup_create_map_header(index->coll(), hoid, t);
  if (!header)
    return -EINVAL;
  _set_header(header, bl, t);
  return db->submit_transaction(t);
}

void DBObjectMapv0::_set_header(Header header, const bufferlist &bl,
			      KeyValueDB::Transaction t)
{
  map<string, bufferlist> to_set;
  to_set[USER_HEADER_KEY] = bl;
  t->set(sys_prefix(header), to_set);
}

int DBObjectMapv0::get_header(const hobject_t &hoid,
			    Index index,
			    bufferlist *bl)
{
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header) {
    return 0;
  }
  return _get_header(header, bl);
}

int DBObjectMapv0::_get_header(Header header,
			     bufferlist *bl)
{
  int r = 0;
  map<string, bufferlist> out;
  while (r == 0) {
    out.clear();
    set<string> to_get;
    to_get.insert(USER_HEADER_KEY);
    int r = db->get(sys_prefix(header), to_get, &out);
    if (r == 0 && out.size())
      break;
    Header current(header);
    if (!current->parent)
      break;
    header = lookup_parent(current);
  }

  if (r < 0)
    return r;
  if (out.size())
    bl->swap(out.begin()->second);
  return 0;
}

int DBObjectMapv0::clear(const hobject_t &hoid,
		       Index index)
{
  KeyValueDB::Transaction t = db->get_transaction();
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return -ENOENT;
  remove_map_header(index->coll(), hoid, header, t);
  assert(header->num_children > 0);
  header->num_children--;
  int r = _clear(header, t);
  if (r < 0)
    return r;
  return db->submit_transaction(t);
}

int DBObjectMapv0::_clear(Header header,
			KeyValueDB::Transaction t)
{
  while (1) {
    if (header->num_children) {
      set_header(header, t);
      break;
    }
    clear_header(header, t);
    if (!header->parent)
      break;
    Header parent = lookup_parent(header);
    if (!parent) {
      return -EINVAL;
    }
    assert(parent->num_children > 0);
    parent->num_children--;
    header.swap(parent);
  }
  return 0;
}

int DBObjectMapv0::merge_new_complete(Header header,
				    const map<string, string> &new_complete,
				    DBObjectMapv0Iterator iter,
				    KeyValueDB::Transaction t)
{
  KeyValueDB::Iterator complete_iter = db->get_iterator(
    complete_prefix(header)
    );
  map<string, string>::const_iterator i = new_complete.begin();
  set<string> to_remove;
  map<string, bufferlist> to_add;

  string begin, end;
  int r = 0;
  while (i != new_complete.end()) {
    string new_begin = i->first;
    string new_end = i->second;
    r = iter->in_complete_region(new_begin, &begin, &end);
    if (r < 0)
      return r;
    if (r) {
      to_remove.insert(begin);
      new_begin = begin;
    }
    ++i;
    while (i != new_complete.end()) {
      if (!new_end.size() || i->first <= new_end) {
	if (!new_end.size() && i->second > new_end) {
	  new_end = i->second;
	}
	++i;
	continue;
      }

      r = iter->in_complete_region(new_end, &begin, &end);
      if (r < 0)
	return r;
      if (r) {
	to_remove.insert(begin);
	new_end = end;
	continue;
      }
      break;
    }
    bufferlist bl;
    bl.append(bufferptr(new_end.c_str(), new_end.size() + 1));
    to_add.insert(make_pair(new_begin, bl));
  }
  t->rmkeys(complete_prefix(header), to_remove);
  t->set(complete_prefix(header), to_add);
  return 0;
}

int DBObjectMapv0::copy_up_header(Header header,
				KeyValueDB::Transaction t)
{
  bufferlist bl;
  int r = _get_header(header, &bl);
  if (r < 0)
    return r;

  _set_header(header, bl, t);
  return 0;
}

int DBObjectMapv0::need_parent(DBObjectMapv0Iterator iter)
{
  int r = iter->seek_to_first();
  if (r < 0)
    return r;

  if (!iter->valid())
    return 0;

  string begin, end;
  if (iter->in_complete_region(iter->key(), &begin, &end) && end == "") {
    return 0;
  }
  return 1;
}

int DBObjectMapv0::rm_keys(const hobject_t &hoid,
			 Index index,
			 const set<string> &to_clear)
{
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return -ENOENT;
  KeyValueDB::Transaction t = db->get_transaction();
  t->rmkeys(user_prefix(header), to_clear);
  if (!header->parent) {
    return db->submit_transaction(t);
  }

  // Copy up keys from parent around to_clear
  int keep_parent;
  {
    DBObjectMapv0Iterator iter = _get_iterator(header);
    iter->seek_to_first();
    map<string, string> new_complete;
    map<string, bufferlist> to_write;
    unsigned copied = 0;
    for(set<string>::const_iterator i = to_clear.begin();
	i != to_clear.end();
      ) {
      copied = 0;
      iter->lower_bound(*i);
      ++i;
      if (!iter->valid())
	break;
      string begin = iter->key();
      if (!iter->on_parent())
	iter->next_parent();
      if (new_complete.size() && new_complete.rbegin()->second == begin) {
	begin = new_complete.rbegin()->first;
      }
      while (iter->valid() && copied < 20) {
	if (!to_clear.count(iter->key()))
	  to_write[iter->key()].append(iter->value());
	if (i != to_clear.end() && *i <= iter->key()) {
	  ++i;
	  copied = 0;
	}

	iter->next_parent();
	copied++;
      }
      if (iter->valid()) {
	new_complete[begin] = iter->key();
      } else {
	new_complete[begin] = "";
	break;
      }
    }
    t->set(user_prefix(header), to_write);
    merge_new_complete(header, new_complete, iter, t);
    keep_parent = need_parent(iter);
    if (keep_parent < 0)
      return keep_parent;
  }
  if (!keep_parent) {
    copy_up_header(header, t);
    Header parent = lookup_parent(header);
    if (!parent)
      return -EINVAL;
    parent->num_children--;
    _clear(parent, t);
    header->parent = 0;
    set_header(header, t);
    t->rmkeys_by_prefix(complete_prefix(header));
  }
  return db->submit_transaction(t);
}

int DBObjectMapv0::get(const hobject_t &hoid,
		     Index index,
		     bufferlist *_header,
		     map<string, bufferlist> *out)
{
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return -ENOENT;
  _get_header(header, _header);
  ObjectMapIterator iter = _get_iterator(header);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    if (iter->status())
      return iter->status();
    out->insert(make_pair(iter->key(), iter->value()));
  }
  return 0;
}

int DBObjectMapv0::get_keys(const hobject_t &hoid,
			  Index index,
			  set<string> *keys)
{
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return -ENOENT;
  ObjectMapIterator iter = get_iterator(hoid, index);
  for (; iter->valid(); iter->next()) {
    if (iter->status())
      return iter->status();
    keys->insert(iter->key());
  }
  return 0;
}

int DBObjectMapv0::scan(Header header,
		      const set<string> &in_keys,
		      set<string> *out_keys,
		      map<string, bufferlist> *out_values)
{
  ObjectMapIterator db_iter = _get_iterator(header);
  for (set<string>::const_iterator key_iter = in_keys.begin();
       key_iter != in_keys.end();
       ++key_iter) {
    db_iter->lower_bound(*key_iter);
    if (db_iter->status())
      return db_iter->status();
    if (db_iter->valid() && db_iter->key() == *key_iter) {
      if (out_keys)
	out_keys->insert(*key_iter);
      if (out_values)
	out_values->insert(make_pair(db_iter->key(), db_iter->value()));
    }
  }
  return 0;
}

int DBObjectMapv0::get_values(const hobject_t &hoid,
			    Index index,
			    const set<string> &keys,
			    map<string, bufferlist> *out)
{
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return -ENOENT;
  return scan(header, keys, 0, out);;
}

int DBObjectMapv0::check_keys(const hobject_t &hoid,
			    Index index,
			    const set<string> &keys,
			    set<string> *out)
{
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return -ENOENT;
  return scan(header, keys, out, 0);
}

int DBObjectMapv0::get_xattrs(const hobject_t &hoid,
			    Index index,
			    const set<string> &to_get,
			    map<string, bufferlist> *out)
{
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return -ENOENT;
  return db->get(xattr_prefix(header), to_get, out);
}

int DBObjectMapv0::get_all_xattrs(const hobject_t &hoid,
				Index index,
				set<string> *out)
{
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return -ENOENT;
  KeyValueDB::Iterator iter = db->get_iterator(xattr_prefix(header));
  if (!iter)
    return -EINVAL;
  for (iter->seek_to_first(); !iter->status() && iter->valid(); iter->next())
    out->insert(iter->key());
  return iter->status();
}

int DBObjectMapv0::set_xattrs(const hobject_t &hoid,
			    Index index,
			    const map<string, bufferlist> &to_set)
{
  KeyValueDB::Transaction t = db->get_transaction();
  Header header = lookup_create_map_header(index->coll(), hoid, t);
  if (!header)
    return -EINVAL;
  t->set(xattr_prefix(header), to_set);
  return db->submit_transaction(t);
}

int DBObjectMapv0::remove_xattrs(const hobject_t &hoid,
			       Index index,
			       const set<string> &to_remove)
{
  KeyValueDB::Transaction t = db->get_transaction();
  Header header = lookup_map_header(index->coll(), hoid);
  if (!header)
    return -ENOENT;
  t->rmkeys(xattr_prefix(header), to_remove);
  return db->submit_transaction(t);
}

int DBObjectMapv0::clone(const hobject_t &hoid,
		       Index index,
		       const hobject_t &target,
		       Index target_index)
{
  assert(index->coll() != target_index->coll() ||
	 hoid != target);
  KeyValueDB::Transaction t = db->get_transaction();
  {
    Header destination = lookup_map_header(target_index->coll(), target);
    if (destination) {
      remove_map_header(target_index->coll(), target, destination, t);
      destination->num_children--;
      _clear(destination, t);
    }
  }

  Header parent = lookup_map_header(index->coll(), hoid);
  if (!parent)
    return db->submit_transaction(t);

  Header source = generate_new_header(index->coll(), hoid, parent);
  Header destination = generate_new_header(target_index->coll(), target, parent);
  _Header lsource, ldestination;
  source->num_children = parent->num_children;
  lsource.parent = source->seq;
  ldestination.parent = destination->seq;
  parent->num_children = 2;
  set_header(parent, t);
  set_header(source, t);
  set_header(destination, t);
  set_map_header(target_index->coll(), target, ldestination, t);

  map<string, bufferlist> to_set;
  KeyValueDB::Iterator xattr_iter = db->get_iterator(xattr_prefix(parent));
  for (xattr_iter->seek_to_first();
       xattr_iter->valid();
       xattr_iter->next())
    to_set.insert(make_pair(xattr_iter->key(), xattr_iter->value()));
  t->set(xattr_prefix(source), to_set);
  t->set(xattr_prefix(destination), to_set);
  t->rmkeys_by_prefix(xattr_prefix(parent));

  string hkey(header_key(parent->seq));
  KeyValueDB::Iterator iter = db->get_iterator(REVERSE_LEAF_PREFIX);
  iter->lower_bound(hkey);
  assert(iter->valid());
  assert(string(iter->key(), 0, hkey.size()) == hkey);
  for (; iter->valid() && string(iter->key(), 0, hkey.size()) == hkey;
       iter->next()) {
    pair<coll_t, hobject_t> p;
    bufferlist bl = iter->value();
    bufferlist::iterator bliter = bl.begin();
    ::decode(p, bliter);
    remove_map_header(p.first, p.second, parent, t);
    set_map_header(p.first, p.second, lsource , t);
  }
  return db->submit_transaction(t);
}

int DBObjectMapv0::link(const hobject_t &hoid,
		      Index index,
		      const hobject_t &target,
		      Index target_index)
{
  assert(index->coll() != target_index->coll() ||
	 hoid != target);
  KeyValueDB::Transaction t = db->get_transaction();
  {
    Header destination = lookup_map_header(target_index->coll(), target);
    if (destination) {
      remove_map_header(target_index->coll(), target, destination, t);
      destination->num_children--;
      _clear(destination, t);
    }
  }
  Header header = lookup_create_map_header(index->coll(), hoid, t);

  assert(header->num_children > 0);
  header->num_children++;
  set_header(header, t);
  _Header ldestination;
  ldestination.parent = header->seq;
  set_map_header(target_index->coll(), target, ldestination, t);
  return db->submit_transaction(t);
}

int DBObjectMapv0::init() {
  State state;
  map<string, bufferlist> result;
  set<string> to_get;
  to_get.insert(GLOBAL_STATE_KEY);
  int r = db->get(SYS_PREFIX, to_get, &result);
  if (r < 0)
    return r;
  if (result.size()) {
    bufferlist::iterator bliter = result.begin()->second.begin();
    state.decode(bliter);
    next_seq = state.seq;
  }
  dout(20) << "(init)dbobjectmap: seq is " << next_seq << dendl;
  return 0;
}

int DBObjectMapv0::sync() {
  return write_state(true);
}

int DBObjectMapv0::write_state(bool sync) {
  dout(20) << "dbobjectmap: seq is " << next_seq << dendl;
  KeyValueDB::Transaction t = db->get_transaction();
  State state;
  state.seq = next_seq;
  bufferlist bl;
  state.encode(bl);
  map<string, bufferlist> to_write;
  to_write[GLOBAL_STATE_KEY] = bl;
  t->set(SYS_PREFIX, to_write);
  return sync ? db->submit_transaction_sync(t) : db->submit_transaction(t);
}


DBObjectMapv0::Header DBObjectMapv0::lookup_map_header(coll_t c, const hobject_t &hoid)
{
  Mutex::Locker l(header_lock);
  _Header lheader;
  while (true) {
    map<string, bufferlist> out;
    set<string> keys;
    keys.insert(map_header_key(c, hoid));
    int r = db->get(LEAF_PREFIX, keys, &out);
    if (r < 0)
      return Header();
    if (out.size() < 1)
      return Header();
    bufferlist::iterator iter = out.begin()->second.begin();
    lheader.decode(iter);

    if (in_use.count(lheader.parent)) {
      header_cond.Wait(header_lock);
      continue;
    }
    in_use.insert(lheader.parent);
    break;
  }

  dout(20) << "lookup_map_header: parent seq is " << lheader.parent
       << " for hoid " << hoid << dendl;
  map<string, bufferlist> out;
  set<string> keys;
  keys.insert(HEADER_KEY);
  int r = db->get(sys_parent_prefix(lheader), keys, &out);
  if (r < 0)
    return Header();
  assert(out.size());

  Header header = Header(new _Header(), RemoveOnDelete(this));
  header->seq = lheader.parent;

  bufferlist::iterator iter = out.begin()->second.begin();
  header->decode(iter);
  return header;
}

DBObjectMapv0::Header DBObjectMapv0::generate_new_header(coll_t c, const hobject_t &hoid,
						     Header parent)
{
  Mutex::Locker l(header_lock);
  Header header = Header(new _Header(), RemoveOnDelete(this));
  header->seq = next_seq++;
  if (parent)
    header->parent = parent->seq;
  header->num_children = 1;
  header->c = c;
  header->hoid = hoid;
  assert(!in_use.count(header->seq));
  in_use.insert(header->seq);

  write_state();
  return header;
}

DBObjectMapv0::Header DBObjectMapv0::lookup_parent(Header input)
{
  Mutex::Locker l(header_lock);
  while (in_use.count(input->parent))
    header_cond.Wait(header_lock);
  map<string, bufferlist> out;
  set<string> keys;
  keys.insert(HEADER_KEY);

  dout(20) << "lookup_parent: parent " << input->parent
       << " for seq " << input->seq << dendl;
  int r = db->get(sys_parent_prefix(input), keys, &out);
  if (r < 0) {
    assert(0);
    return Header();
  }
  if (out.size() < 1) {
    assert(0);
    return Header();
  }

  Header header = Header(new _Header(), RemoveOnDelete(this));
  header->seq = input->parent;
  bufferlist::iterator iter = out.begin()->second.begin();
  header->decode(iter);
  dout(20) << "lookup_parent: parent seq is " << header->seq << " with parent "
       << header->parent << dendl;
  in_use.insert(header->seq);
  return header;
}

DBObjectMapv0::Header DBObjectMapv0::lookup_create_map_header(coll_t c, const hobject_t &hoid,
							  KeyValueDB::Transaction t)
{
  Header header = lookup_map_header(c, hoid);
  if (!header) {
    header = generate_new_header(c, hoid, Header());
    set_header(header, t);
    _Header lheader;
    lheader.parent = header->seq;
    set_map_header(c, hoid, lheader, t);
  }
  return header;
}

void DBObjectMapv0::clear_header(Header header, KeyValueDB::Transaction t)
{
  dout(20) << "clear_header: clearing seq " << header->seq << dendl;
  t->rmkeys_by_prefix(user_prefix(header));
  t->rmkeys_by_prefix(sys_prefix(header));
  t->rmkeys_by_prefix(complete_prefix(header));
  t->rmkeys_by_prefix(xattr_prefix(header));
  set<string> keys;
  keys.insert(header_key(header->seq));
  t->rmkeys(USER_PREFIX, keys);
}

void DBObjectMapv0::set_header(Header header, KeyValueDB::Transaction t)
{
  dout(20) << "set_header: setting seq " << header->seq << dendl;
  map<string, bufferlist> to_write;
  header->encode(to_write[HEADER_KEY]);
  t->set(sys_prefix(header), to_write);
}

void DBObjectMapv0::remove_map_header(coll_t c, const hobject_t &hoid,
				    Header header,
				    KeyValueDB::Transaction t)
{
  dout(20) << "remove_map_header: removing " << header->seq
       << " hoid " << hoid << dendl;
  set<string> to_remove;
  to_remove.insert(map_header_key(c, hoid));
  t->rmkeys(LEAF_PREFIX, to_remove);
  to_remove.clear();
  to_remove.insert(header_key(header->seq) +
		   map_header_key(c, hoid));
  t->rmkeys(REVERSE_LEAF_PREFIX, to_remove);
}

void DBObjectMapv0::set_map_header(coll_t c, const hobject_t &hoid, _Header header,
				 KeyValueDB::Transaction t)
{
  dout(20) << "set_map_header: setting " << header.seq
       << " hoid " << hoid << " parent seq "
       << header.parent << dendl;
  map<string, bufferlist> to_set;
  header.encode(to_set[map_header_key(c, hoid)]);
  t->set(LEAF_PREFIX, to_set);
  to_set.clear();
  ::encode(make_pair(c, hoid), to_set[
	     header_key(header.parent) +
	     map_header_key(c, hoid)]);
  t->set(REVERSE_LEAF_PREFIX, to_set);
}
