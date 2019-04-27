// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "include/int_types.h"
#include "include/buffer.h"

#include <iostream>
#include <set>
#include <map>
#include <string>
#include <vector>

#include "os/ObjectMap.h"
#include "kv/KeyValueDB.h"
#include "DBObjectMap.h"
#include <errno.h>

#include "common/debug.h"
#include "common/config.h"
#include "include/ceph_assert.h"

#define dout_context cct
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "filestore "

const string DBObjectMap::USER_PREFIX = "_USER_";
const string DBObjectMap::XATTR_PREFIX = "_AXATTR_";
const string DBObjectMap::SYS_PREFIX = "_SYS_";
const string DBObjectMap::COMPLETE_PREFIX = "_COMPLETE_";
const string DBObjectMap::HEADER_KEY = "HEADER";
const string DBObjectMap::USER_HEADER_KEY = "USER_HEADER";
const string DBObjectMap::GLOBAL_STATE_KEY = "HEADER";
const string DBObjectMap::HOBJECT_TO_SEQ = "_HOBJTOSEQ_";

// Legacy
const string DBObjectMap::LEAF_PREFIX = "_LEAF_";
const string DBObjectMap::REVERSE_LEAF_PREFIX = "_REVLEAF_";

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

int DBObjectMap::check(std::ostream &out, bool repair, bool force)
{
  int errors = 0, comp_errors = 0;
  bool repaired = false;
  map<uint64_t, uint64_t> parent_to_num_children;
  map<uint64_t, uint64_t> parent_to_actual_num_children;
  KeyValueDB::Iterator iter = db->get_iterator(HOBJECT_TO_SEQ);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    _Header header;
    bufferlist bl = iter->value();
    while (true) {
      auto bliter = bl.cbegin();
      header.decode(bliter);
      if (header.seq != 0)
	parent_to_actual_num_children[header.seq] = header.num_children;

      if (state.v == 2 || force) {
	// Check complete table
	bool complete_error = false;
	boost::optional<string> prev;
	KeyValueDB::Iterator complete_iter = db->get_iterator(USER_PREFIX + header_key(header.seq) + COMPLETE_PREFIX);
	for (complete_iter->seek_to_first(); complete_iter->valid();
	     complete_iter->next()) {
	  if (prev && prev >= complete_iter->key()) {
	     out << "Bad complete for " << header.oid << std::endl;
	     complete_error = true;
	     break;
	  }
	  prev = string(complete_iter->value().c_str(), complete_iter->value().length() - 1);
	}
	if (complete_error) {
	  out << "Complete mapping for " << header.seq << " :" << std::endl;
	  for (complete_iter->seek_to_first(); complete_iter->valid();
	       complete_iter->next()) {
	    out << complete_iter->key() << " -> " << string(complete_iter->value().c_str(), complete_iter->value().length() - 1) << std::endl;
	  }
	  if (repair) {
	    repaired = true;
	    KeyValueDB::Transaction t = db->get_transaction();
	    t->rmkeys_by_prefix(USER_PREFIX + header_key(header.seq) + COMPLETE_PREFIX);
	    db->submit_transaction(t);
	    out << "Cleared complete mapping to repair" << std::endl;
	  } else {
	    errors++;  // Only count when not repaired
	    comp_errors++;  // Track errors here for version update
	  }
	}
      }

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
      if (got.empty()) {
	out << "Missing: seq " << header.parent << std::endl;
	errors++;
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
      errors++;
    }
    parent_to_actual_num_children.erase(i->first);
  }

  // Only advance the version from 2 to 3 here
  // Mark as legacy because there are still older structures
  // we don't update.  The value of legacy is only used
  // for internal assertions.
  if (comp_errors == 0 && state.v == 2 && repair) {
    state.v = 3;
    state.legacy = true;
    set_state();
  }

  if (errors == 0 && repaired)
    return -1;
  return errors;
}

string DBObjectMap::ghobject_key(const ghobject_t &oid)
{
  string out;
  append_escaped(oid.hobj.oid.name, &out);
  out.push_back('.');
  append_escaped(oid.hobj.get_key(), &out);
  out.push_back('.');
  append_escaped(oid.hobj.nspace, &out);
  out.push_back('.');

  char snap_with_hash[1000];
  char *t = snap_with_hash;
  char *end = t + sizeof(snap_with_hash);
  if (oid.hobj.snap == CEPH_NOSNAP)
    t += snprintf(t, end - t, "head");
  else if (oid.hobj.snap == CEPH_SNAPDIR)
    t += snprintf(t, end - t, "snapdir");
  else
    t += snprintf(t, end - t, "%llx", (long long unsigned)oid.hobj.snap);

  if (oid.hobj.pool == -1)
    t += snprintf(t, end - t, ".none");
  else
    t += snprintf(t, end - t, ".%llx", (long long unsigned)oid.hobj.pool);
  t += snprintf(t, end - t, ".%.*X", (int)(sizeof(uint32_t)*2), oid.hobj.get_hash());

  if (oid.generation != ghobject_t::NO_GEN ||
      oid.shard_id != shard_id_t::NO_SHARD) {
    t += snprintf(t, end - t, ".%llx", (long long unsigned)oid.generation);
    t += snprintf(t, end - t, ".%x", (int)oid.shard_id);
  }
  out += string(snap_with_hash);
  return out;
}

//    ok: pglog%u3%efs1...0.none.0017B237
//   bad: plana8923501-10...4c.3.ffffffffffffffff.2
// fixed: plana8923501-10...4c.3.CB767F2D.ffffffffffffffff.2
// returns 0 for false, 1 for true, negative for error
int DBObjectMap::is_buggy_ghobject_key_v1(CephContext* cct,
					  const string &in)
{
  int dots = 5;  // skip 5 .'s
  const char *s = in.c_str();
  do {
    while (*s && *s != '.')
      ++s;
    if (!*s) {
      derr << "unexpected null at " << (int)(s-in.c_str()) << dendl;
      return -EINVAL;
    }
    ++s;
  } while (*s && --dots);
  if (!*s) {
    derr << "unexpected null at " << (int)(s-in.c_str()) << dendl;
    return -EINVAL;
  }
  // we are now either at a hash value (32 bits, 8 chars) or a generation
  // value (64 bits) '.' and shard id.  count the dots!
  int len = 0;
  while (*s && *s != '.') {
    ++s;
    ++len;
  }
  if (*s == '\0') {
    if (len != 8) {
      derr << "hash value is not 8 chars" << dendl;
      return -EINVAL;  // the hash value is always 8 chars.
    }
    return 0;
  }
  if (*s != '.') { // the shard follows.
    derr << "missing final . and shard id at " << (int)(s-in.c_str()) << dendl;
    return -EINVAL;
  }
  return 1;
}


string DBObjectMap::map_header_key(const ghobject_t &oid)
{
  return ghobject_key(oid);
}

string DBObjectMap::header_key(uint64_t seq)
{
  char buf[100];
  snprintf(buf, sizeof(buf), "%.*" PRId64, (int)(2*sizeof(seq)), seq);
  return string(buf);
}

string DBObjectMap::complete_prefix(Header header)
{
  return USER_PREFIX + header_key(header->seq) + COMPLETE_PREFIX;
}

string DBObjectMap::user_prefix(Header header)
{
  return USER_PREFIX + header_key(header->seq) + USER_PREFIX;
}

string DBObjectMap::sys_prefix(Header header)
{
  return USER_PREFIX + header_key(header->seq) + SYS_PREFIX;
}

string DBObjectMap::xattr_prefix(Header header)
{
  return USER_PREFIX + header_key(header->seq) + XATTR_PREFIX;
}

string DBObjectMap::sys_parent_prefix(_Header header)
{
  return USER_PREFIX + header_key(header.parent) + SYS_PREFIX;
}

int DBObjectMap::DBObjectMapIteratorImpl::init()
{
  invalid = false;
  if (ready) {
    return 0;
  }
  ceph_assert(!parent_iter);
  if (header->parent) {
    Header parent = map->lookup_parent(header);
    if (!parent) {
      ceph_abort();
      return -EINVAL;
    }
    parent_iter = std::make_shared<DBObjectMapIteratorImpl>(map, parent);
  }
  key_iter = map->db->get_iterator(map->user_prefix(header));
  ceph_assert(key_iter);
  complete_iter = map->db->get_iterator(map->complete_prefix(header));
  ceph_assert(complete_iter);
  cur_iter = key_iter;
  ceph_assert(cur_iter);
  ready = true;
  return 0;
}

ObjectMap::ObjectMapIterator DBObjectMap::get_iterator(
  const ghobject_t &oid)
{
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return ObjectMapIterator(new EmptyIteratorImpl());
  DBObjectMapIterator iter = _get_iterator(header);
  iter->hlock.swap(hl);
  return iter;
}

int DBObjectMap::DBObjectMapIteratorImpl::seek_to_first()
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

int DBObjectMap::DBObjectMapIteratorImpl::seek_to_last()
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

int DBObjectMap::DBObjectMapIteratorImpl::lower_bound(const string &to)
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

int DBObjectMap::DBObjectMapIteratorImpl::lower_bound_parent(const string &to)
{
  int r = lower_bound(to);
  if (r < 0)
    return r;
  if (valid() && !on_parent())
    return next_parent();
  else
    return r;
}

int DBObjectMap::DBObjectMapIteratorImpl::upper_bound(const string &after)
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

bool DBObjectMap::DBObjectMapIteratorImpl::valid()
{
  bool valid = !invalid && ready;
  ceph_assert(!valid || cur_iter->valid());
  return valid;
}

bool DBObjectMap::DBObjectMapIteratorImpl::valid_parent()
{
  if (parent_iter && parent_iter->valid() &&
      (!key_iter->valid() || key_iter->key() > parent_iter->key()))
    return true;
  return false;
}

int DBObjectMap::DBObjectMapIteratorImpl::next()
{
  ceph_assert(cur_iter->valid());
  ceph_assert(valid());
  cur_iter->next();
  return adjust();
}

int DBObjectMap::DBObjectMapIteratorImpl::next_parent()
{
  r = next();
  if (r < 0)
    return r;
  while (parent_iter && parent_iter->valid() && !on_parent()) {
    ceph_assert(valid());
    r = lower_bound(parent_iter->key());
    if (r < 0)
      return r;
  }

  if (!parent_iter || !parent_iter->valid()) {
    invalid = true;
  }
  return 0;
}

int DBObjectMap::DBObjectMapIteratorImpl::in_complete_region(const string &to_test,
							     string *begin,
							     string *end)
{
  /* This is clumsy because one cannot call prev() on end(), nor can one
   * test for == begin().
   */
  complete_iter->upper_bound(to_test);
  if (complete_iter->valid()) {
    complete_iter->prev();
    if (!complete_iter->valid()) {
      complete_iter->upper_bound(to_test);
      return false;
    }
  } else {
    complete_iter->seek_to_last();
    if (!complete_iter->valid())
      return false;
  }

  ceph_assert(complete_iter->key() <= to_test);
  ceph_assert(complete_iter->value().length() >= 1);
  string _end(complete_iter->value().c_str(),
	      complete_iter->value().length() - 1);
  if (_end.empty() || _end > to_test) {
    if (begin)
      *begin = complete_iter->key();
    if (end)
      *end = _end;
    return true;
  } else {
    complete_iter->next();
    ceph_assert(!complete_iter->valid() || complete_iter->key() > to_test);
    return false;
  }
}

/**
 * Moves parent_iter to the next position both out of the complete_region and
 * not equal to key_iter.  Then, we set cur_iter to parent_iter if valid and
 * less than key_iter and key_iter otherwise.
 */
int DBObjectMap::DBObjectMapIteratorImpl::adjust()
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
  ceph_assert(invalid || cur_iter->valid());
  return 0;
}


string DBObjectMap::DBObjectMapIteratorImpl::key()
{
  return cur_iter->key();
}

bufferlist DBObjectMap::DBObjectMapIteratorImpl::value()
{
  return cur_iter->value();
}

int DBObjectMap::DBObjectMapIteratorImpl::status()
{
  return r;
}

int DBObjectMap::set_keys(const ghobject_t &oid,
			  const map<string, bufferlist> &set,
			  const SequencerPosition *spos)
{
  KeyValueDB::Transaction t = db->get_transaction();
  MapHeaderLock hl(this, oid);
  Header header = lookup_create_map_header(hl, oid, t);
  if (!header)
    return -EINVAL;
  if (check_spos(oid, header, spos))
    return 0;

  t->set(user_prefix(header), set);

  return db->submit_transaction(t);
}

int DBObjectMap::set_header(const ghobject_t &oid,
			    const bufferlist &bl,
			    const SequencerPosition *spos)
{
  KeyValueDB::Transaction t = db->get_transaction();
  MapHeaderLock hl(this, oid);
  Header header = lookup_create_map_header(hl, oid, t);
  if (!header)
    return -EINVAL;
  if (check_spos(oid, header, spos))
    return 0;
  _set_header(header, bl, t);
  return db->submit_transaction(t);
}

void DBObjectMap::_set_header(Header header, const bufferlist &bl,
			      KeyValueDB::Transaction t)
{
  map<string, bufferlist> to_set;
  to_set[USER_HEADER_KEY] = bl;
  t->set(sys_prefix(header), to_set);
}

int DBObjectMap::get_header(const ghobject_t &oid,
			    bufferlist *bl)
{
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header) {
    return 0;
  }
  return _get_header(header, bl);
}

int DBObjectMap::_get_header(Header header,
			     bufferlist *bl)
{
  map<string, bufferlist> out;
  while (true) {
    out.clear();
    set<string> to_get;
    to_get.insert(USER_HEADER_KEY);
    int r = db->get(sys_prefix(header), to_get, &out);
    if (r == 0 && !out.empty())
      break;
    if (r < 0)
      return r;
    Header current(header);
    if (!current->parent)
      break;
    header = lookup_parent(current);
  }

  if (!out.empty())
    bl->swap(out.begin()->second);
  return 0;
}

int DBObjectMap::clear(const ghobject_t &oid,
		       const SequencerPosition *spos)
{
  KeyValueDB::Transaction t = db->get_transaction();
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return -ENOENT;
  if (check_spos(oid, header, spos))
    return 0;
  remove_map_header(hl, oid, header, t);
  ceph_assert(header->num_children > 0);
  header->num_children--;
  int r = _clear(header, t);
  if (r < 0)
    return r;
  return db->submit_transaction(t);
}

int DBObjectMap::_clear(Header header,
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
    ceph_assert(parent->num_children > 0);
    parent->num_children--;
    header.swap(parent);
  }
  return 0;
}

int DBObjectMap::copy_up_header(Header header,
				KeyValueDB::Transaction t)
{
  bufferlist bl;
  int r = _get_header(header, &bl);
  if (r < 0)
    return r;

  _set_header(header, bl, t);
  return 0;
}

int DBObjectMap::rm_keys(const ghobject_t &oid,
			 const set<string> &to_clear,
			 const SequencerPosition *spos)
{
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return -ENOENT;
  KeyValueDB::Transaction t = db->get_transaction();
  if (check_spos(oid, header, spos))
    return 0;
  t->rmkeys(user_prefix(header), to_clear);
  if (!header->parent) {
    return db->submit_transaction(t);
  }

  ceph_assert(state.legacy);

  {
    // We only get here for legacy (v2) stores
    // Copy up all keys from parent excluding to_clear
    // and remove parent
    // This eliminates a v2 format use of complete for this oid only
    map<string, bufferlist> to_write;
    ObjectMapIterator iter = _get_iterator(header);
    for (iter->seek_to_first() ; iter->valid() ; iter->next()) {
      if (iter->status())
        return iter->status();
      if (!to_clear.count(iter->key()))
        to_write[iter->key()] = iter->value();
    }
    t->set(user_prefix(header), to_write);
  } // destruct iter which has parent in_use

  copy_up_header(header, t);
  Header parent = lookup_parent(header);
  if (!parent)
    return -EINVAL;
  parent->num_children--;
  _clear(parent, t);
  header->parent = 0;
  set_map_header(hl, oid, *header, t);
  t->rmkeys_by_prefix(complete_prefix(header));
  return db->submit_transaction(t);
}

int DBObjectMap::clear_keys_header(const ghobject_t &oid,
				   const SequencerPosition *spos)
{
  KeyValueDB::Transaction t = db->get_transaction();
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return -ENOENT;
  if (check_spos(oid, header, spos))
    return 0;

  // save old attrs
  KeyValueDB::Iterator iter = db->get_iterator(xattr_prefix(header));
  if (!iter)
    return -EINVAL;
  map<string, bufferlist> attrs;
  for (iter->seek_to_first(); !iter->status() && iter->valid(); iter->next())
    attrs.insert(make_pair(iter->key(), iter->value()));
  if (iter->status())
    return iter->status();

  // remove current header
  remove_map_header(hl, oid, header, t);
  ceph_assert(header->num_children > 0);
  header->num_children--;
  int r = _clear(header, t);
  if (r < 0)
    return r;

  // create new header
  Header newheader = generate_new_header(oid, Header());
  set_map_header(hl, oid, *newheader, t);
  if (!attrs.empty())
    t->set(xattr_prefix(newheader), attrs);
  return db->submit_transaction(t);
}

int DBObjectMap::get(const ghobject_t &oid,
		     bufferlist *_header,
		     map<string, bufferlist> *out)
{
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
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

int DBObjectMap::get_keys(const ghobject_t &oid,
			  set<string> *keys)
{
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return -ENOENT;
  ObjectMapIterator iter = _get_iterator(header);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    if (iter->status())
      return iter->status();
    keys->insert(iter->key());
  }
  return 0;
}

int DBObjectMap::scan(Header header,
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

int DBObjectMap::get_values(const ghobject_t &oid,
			    const set<string> &keys,
			    map<string, bufferlist> *out)
{
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return -ENOENT;
  return scan(header, keys, 0, out);
}

int DBObjectMap::check_keys(const ghobject_t &oid,
			    const set<string> &keys,
			    set<string> *out)
{
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return -ENOENT;
  return scan(header, keys, out, 0);
}

int DBObjectMap::get_xattrs(const ghobject_t &oid,
			    const set<string> &to_get,
			    map<string, bufferlist> *out)
{
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return -ENOENT;
  return db->get(xattr_prefix(header), to_get, out);
}

int DBObjectMap::get_all_xattrs(const ghobject_t &oid,
				set<string> *out)
{
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return -ENOENT;
  KeyValueDB::Iterator iter = db->get_iterator(xattr_prefix(header));
  if (!iter)
    return -EINVAL;
  for (iter->seek_to_first(); !iter->status() && iter->valid(); iter->next())
    out->insert(iter->key());
  return iter->status();
}

int DBObjectMap::set_xattrs(const ghobject_t &oid,
			    const map<string, bufferlist> &to_set,
			    const SequencerPosition *spos)
{
  KeyValueDB::Transaction t = db->get_transaction();
  MapHeaderLock hl(this, oid);
  Header header = lookup_create_map_header(hl, oid, t);
  if (!header)
    return -EINVAL;
  if (check_spos(oid, header, spos))
    return 0;
  t->set(xattr_prefix(header), to_set);
  return db->submit_transaction(t);
}

int DBObjectMap::remove_xattrs(const ghobject_t &oid,
			       const set<string> &to_remove,
			       const SequencerPosition *spos)
{
  KeyValueDB::Transaction t = db->get_transaction();
  MapHeaderLock hl(this, oid);
  Header header = lookup_map_header(hl, oid);
  if (!header)
    return -ENOENT;
  if (check_spos(oid, header, spos))
    return 0;
  t->rmkeys(xattr_prefix(header), to_remove);
  return db->submit_transaction(t);
}

// ONLY USED FOR TESTING
// Set version to 2 to avoid asserts
int DBObjectMap::legacy_clone(const ghobject_t &oid,
		       const ghobject_t &target,
		       const SequencerPosition *spos)
{
  state.legacy = true;

  if (oid == target)
    return 0;

  MapHeaderLock _l1(this, std::min(oid, target));
  MapHeaderLock _l2(this, std::max(oid, target));
  MapHeaderLock *lsource, *ltarget;
  if (oid > target) {
    lsource = &_l2;
    ltarget= &_l1;
  } else {
    lsource = &_l1;
    ltarget= &_l2;
  }

  KeyValueDB::Transaction t = db->get_transaction();
  {
    Header destination = lookup_map_header(*ltarget, target);
    if (destination) {
      if (check_spos(target, destination, spos))
	return 0;
      destination->num_children--;
      remove_map_header(*ltarget, target, destination, t);
      _clear(destination, t);
    }
  }

  Header parent = lookup_map_header(*lsource, oid);
  if (!parent)
    return db->submit_transaction(t);

  Header source = generate_new_header(oid, parent);
  Header destination = generate_new_header(target, parent);
  if (spos)
    destination->spos = *spos;

  parent->num_children = 2;
  set_header(parent, t);
  set_map_header(*lsource, oid, *source, t);
  set_map_header(*ltarget, target, *destination, t);

  map<string, bufferlist> to_set;
  KeyValueDB::Iterator xattr_iter = db->get_iterator(xattr_prefix(parent));
  for (xattr_iter->seek_to_first();
       xattr_iter->valid();
       xattr_iter->next())
    to_set.insert(make_pair(xattr_iter->key(), xattr_iter->value()));
  t->set(xattr_prefix(source), to_set);
  t->set(xattr_prefix(destination), to_set);
  t->rmkeys_by_prefix(xattr_prefix(parent));
  return db->submit_transaction(t);
}

int DBObjectMap::clone(const ghobject_t &oid,
		       const ghobject_t &target,
		       const SequencerPosition *spos)
{
  if (oid == target)
    return 0;

  MapHeaderLock _l1(this, std::min(oid, target));
  MapHeaderLock _l2(this, std::max(oid, target));
  MapHeaderLock *lsource, *ltarget;
  if (oid > target) {
    lsource = &_l2;
    ltarget= &_l1;
  } else {
    lsource = &_l1;
    ltarget= &_l2;
  }

  KeyValueDB::Transaction t = db->get_transaction();
  {
    Header destination = lookup_map_header(*ltarget, target);
    if (destination) {
      if (check_spos(target, destination, spos))
	return 0;
      destination->num_children--;
      remove_map_header(*ltarget, target, destination, t);
      _clear(destination, t);
    }
  }

  Header source = lookup_map_header(*lsource, oid);
  if (!source)
    return db->submit_transaction(t);

  Header destination = generate_new_header(target, Header());
  if (spos)
    destination->spos = *spos;

  set_map_header(*ltarget, target, *destination, t);

  bufferlist bl;
  int r = _get_header(source, &bl);
  if (r < 0)
    return r;
  _set_header(destination, bl, t);

  map<string, bufferlist> to_set;
  KeyValueDB::Iterator xattr_iter = db->get_iterator(xattr_prefix(source));
  for (xattr_iter->seek_to_first();
       xattr_iter->valid();
       xattr_iter->next())
    to_set.insert(make_pair(xattr_iter->key(), xattr_iter->value()));
  t->set(xattr_prefix(destination), to_set);

  map<string, bufferlist> to_write;
  ObjectMapIterator iter = _get_iterator(source);
  for (iter->seek_to_first() ; iter->valid() ; iter->next()) {
    if (iter->status())
      return iter->status();
    to_write[iter->key()] = iter->value();
  }
  t->set(user_prefix(destination), to_write);

  return db->submit_transaction(t);
}

int DBObjectMap::upgrade_to_v2()
{
  dout(1) << __func__ << " start" << dendl;
  KeyValueDB::Iterator iter = db->get_iterator(HOBJECT_TO_SEQ);
  iter->seek_to_first();
  while (iter->valid()) {
    unsigned count = 0;
    KeyValueDB::Transaction t = db->get_transaction();
    set<string> remove;
    map<string, bufferlist> add;
    for (;
        iter->valid() && count < 300;
        iter->next()) {
      dout(20) << __func__ << " key is " << iter->key() << dendl;
      int r = is_buggy_ghobject_key_v1(cct, iter->key());
      if (r < 0) {
	derr << __func__ << " bad key '" << iter->key() << "'" << dendl;
	return r;
      }
      if (!r) {
	dout(20) << __func__ << " " << iter->key() << " ok" << dendl;
	continue;
      }

      // decode header to get oid
      _Header hdr;
      bufferlist bl = iter->value();
      auto bliter = bl.cbegin();
      hdr.decode(bliter);

      string newkey(ghobject_key(hdr.oid));
      dout(20) << __func__ << " " << iter->key() << " -> " << newkey << dendl;
      add[newkey] = iter->value();
      remove.insert(iter->key());
      ++count;
    }

    if (!remove.empty()) {
      dout(20) << __func__ << " updating " << remove.size() << " keys" << dendl;
      t->rmkeys(HOBJECT_TO_SEQ, remove);
      t->set(HOBJECT_TO_SEQ, add);
      int r = db->submit_transaction(t);
      if (r < 0)
	return r;
    }
  }

  state.v = 2;

  set_state();
  return 0;
}

void DBObjectMap::set_state()
{
  Mutex::Locker l(header_lock);
  KeyValueDB::Transaction t = db->get_transaction();
  write_state(t);
  int ret = db->submit_transaction_sync(t);
  ceph_assert(ret == 0);
  dout(1) << __func__ << " done" << dendl;
  return;
}

int DBObjectMap::get_state()
{
  map<string, bufferlist> result;
  set<string> to_get;
  to_get.insert(GLOBAL_STATE_KEY);
  int r = db->get(SYS_PREFIX, to_get, &result);
  if (r < 0)
    return r;
  if (!result.empty()) {
    auto bliter = result.begin()->second.cbegin();
    state.decode(bliter);
  } else {
    // New store
    state.v = State::CUR_VERSION;
    state.seq = 1;
    state.legacy = false;
  }
  return 0;
}

int DBObjectMap::init(bool do_upgrade)
{
  int ret = get_state();
  if (ret < 0)
    return ret;
  if (state.v < 1) {
    dout(1) << "DBObjectMap is *very* old; upgrade to an older version first"
	    << dendl;
    return -ENOTSUP;
  }
  if (state.v < 2) { // Needs upgrade
    if (!do_upgrade) {
      dout(1) << "DOBjbectMap requires an upgrade,"
	      << " set filestore_update_to"
	      << dendl;
      return -ENOTSUP;
    } else {
      int r = upgrade_to_v2();
      if (r < 0)
	return r;
    }
  }
  ostringstream ss;
  int errors = check(ss, true);
  if (errors) {
    derr << ss.str() << dendl;
    if (errors > 0)
      return -EINVAL;
  }
  dout(20) << "(init)dbobjectmap: seq is " << state.seq << dendl;
  return 0;
}

int DBObjectMap::sync(const ghobject_t *oid,
		      const SequencerPosition *spos) {
  KeyValueDB::Transaction t = db->get_transaction();
  if (oid) {
    ceph_assert(spos);
    MapHeaderLock hl(this, *oid);
    Header header = lookup_map_header(hl, *oid);
    if (header) {
      dout(10) << "oid: " << *oid << " setting spos to "
	       << *spos << dendl;
      header->spos = *spos;
      set_map_header(hl, *oid, *header, t);
    }
    /* It may appear that this and the identical portion of the else
     * block can combined below, but in this block, the transaction
     * must be submitted under *both* the MapHeaderLock and the full
     * header_lock.
     *
     * See 2b63dd25fc1c73fa42e52e9ea4ab5a45dd9422a0 and bug 9891.
     */
    Mutex::Locker l(header_lock);
    write_state(t);
    return db->submit_transaction_sync(t);
  } else {
    Mutex::Locker l(header_lock);
    write_state(t);
    return db->submit_transaction_sync(t);
  }
}

int DBObjectMap::write_state(KeyValueDB::Transaction _t) {
  ceph_assert(header_lock.is_locked_by_me());
  dout(20) << "dbobjectmap: seq is " << state.seq << dendl;
  KeyValueDB::Transaction t = _t ? _t : db->get_transaction();
  bufferlist bl;
  state.encode(bl);
  map<string, bufferlist> to_write;
  to_write[GLOBAL_STATE_KEY] = bl;
  t->set(SYS_PREFIX, to_write);
  return _t ? 0 : db->submit_transaction(t);
}


DBObjectMap::Header DBObjectMap::_lookup_map_header(
  const MapHeaderLock &l,
  const ghobject_t &oid)
{
  ceph_assert(l.get_locked() == oid);

  _Header *header = new _Header();
  {
    Mutex::Locker l(cache_lock);
    if (caches.lookup(oid, header)) {
      ceph_assert(!in_use.count(header->seq));
      in_use.insert(header->seq);
      return Header(header, RemoveOnDelete(this));
    }
  }

  bufferlist out;
  int r = db->get(HOBJECT_TO_SEQ, map_header_key(oid), &out);
  if (r < 0 || out.length()==0) {
    delete header;
    return Header();
  }

  Header ret(header, RemoveOnDelete(this));
  auto iter = out.cbegin();
  ret->decode(iter);
  {
    Mutex::Locker l(cache_lock);
    caches.add(oid, *ret);
  }

  ceph_assert(!in_use.count(header->seq));
  in_use.insert(header->seq);
  return ret;
}

DBObjectMap::Header DBObjectMap::_generate_new_header(const ghobject_t &oid,
						      Header parent)
{
  Header header = Header(new _Header(), RemoveOnDelete(this));
  header->seq = state.seq++;
  if (parent) {
    header->parent = parent->seq;
    header->spos = parent->spos;
  }
  header->num_children = 1;
  header->oid = oid;
  ceph_assert(!in_use.count(header->seq));
  in_use.insert(header->seq);

  write_state();
  return header;
}

DBObjectMap::Header DBObjectMap::lookup_parent(Header input)
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
    ceph_abort();
    return Header();
  }
  if (out.empty()) {
    ceph_abort();
    return Header();
  }

  Header header = Header(new _Header(), RemoveOnDelete(this));
  auto iter = out.begin()->second.cbegin();
  header->decode(iter);
  ceph_assert(header->seq == input->parent);
  dout(20) << "lookup_parent: parent seq is " << header->seq << " with parent "
       << header->parent << dendl;
  in_use.insert(header->seq);
  return header;
}

DBObjectMap::Header DBObjectMap::lookup_create_map_header(
  const MapHeaderLock &hl,
  const ghobject_t &oid,
  KeyValueDB::Transaction t)
{
  Mutex::Locker l(header_lock);
  Header header = _lookup_map_header(hl, oid);
  if (!header) {
    header = _generate_new_header(oid, Header());
    set_map_header(hl, oid, *header, t);
  }
  return header;
}

void DBObjectMap::clear_header(Header header, KeyValueDB::Transaction t)
{
  dout(20) << "clear_header: clearing seq " << header->seq << dendl;
  t->rmkeys_by_prefix(user_prefix(header));
  t->rmkeys_by_prefix(sys_prefix(header));
  if (state.legacy)
    t->rmkeys_by_prefix(complete_prefix(header)); // Needed when header.parent != 0
  t->rmkeys_by_prefix(xattr_prefix(header));
  set<string> keys;
  keys.insert(header_key(header->seq));
  t->rmkeys(USER_PREFIX, keys);
}

void DBObjectMap::set_header(Header header, KeyValueDB::Transaction t)
{
  dout(20) << "set_header: setting seq " << header->seq << dendl;
  map<string, bufferlist> to_write;
  header->encode(to_write[HEADER_KEY]);
  t->set(sys_prefix(header), to_write);
}

void DBObjectMap::remove_map_header(
  const MapHeaderLock &l,
  const ghobject_t &oid,
  Header header,
  KeyValueDB::Transaction t)
{
  ceph_assert(l.get_locked() == oid);
  dout(20) << "remove_map_header: removing " << header->seq
	   << " oid " << oid << dendl;
  set<string> to_remove;
  to_remove.insert(map_header_key(oid));
  t->rmkeys(HOBJECT_TO_SEQ, to_remove);
  {
    Mutex::Locker l(cache_lock);
    caches.clear(oid);
  }
}

void DBObjectMap::set_map_header(
  const MapHeaderLock &l,
  const ghobject_t &oid, _Header header,
  KeyValueDB::Transaction t)
{
  ceph_assert(l.get_locked() == oid);
  dout(20) << "set_map_header: setting " << header.seq
	   << " oid " << oid << " parent seq "
	   << header.parent << dendl;
  map<string, bufferlist> to_set;
  header.encode(to_set[map_header_key(oid)]);
  t->set(HOBJECT_TO_SEQ, to_set);
  {
    Mutex::Locker l(cache_lock);
    caches.add(oid, header);
  }
}

bool DBObjectMap::check_spos(const ghobject_t &oid,
			     Header header,
			     const SequencerPosition *spos)
{
  if (!spos || *spos > header->spos) {
    stringstream out;
    if (spos)
      dout(10) << "oid: " << oid << " not skipping op, *spos "
	       << *spos << dendl;
    else
      dout(10) << "oid: " << oid << " not skipping op, *spos "
	       << "empty" << dendl;
    dout(10) << " > header.spos " << header->spos << dendl;
    return false;
  } else {
    dout(10) << "oid: " << oid << " skipping op, *spos " << *spos
	     << " <= header.spos " << header->spos << dendl;
    return true;
  }
}

int DBObjectMap::list_objects(vector<ghobject_t> *out)
{
  KeyValueDB::Iterator iter = db->get_iterator(HOBJECT_TO_SEQ);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    bufferlist bl = iter->value();
    auto bliter = bl.cbegin();
    _Header header;
    header.decode(bliter);
    out->push_back(header.oid);
  }
  return 0;
}

int DBObjectMap::list_object_headers(vector<_Header> *out)
{
  int error = 0;
  KeyValueDB::Iterator iter = db->get_iterator(HOBJECT_TO_SEQ);
  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    bufferlist bl = iter->value();
    auto bliter = bl.cbegin();
    _Header header;
    header.decode(bliter);
    out->push_back(header);
    while (header.parent) {
      set<string> to_get;
      map<string, bufferlist> got;
      to_get.insert(HEADER_KEY);
      db->get(sys_parent_prefix(header), to_get, &got);
      if (got.empty()) {
	dout(0) << "Missing: seq " << header.parent << dendl;
	error = -ENOENT;
	break;
      } else {
	bl = got.begin()->second;
        auto bliter = bl.cbegin();
        header.decode(bliter);
        out->push_back(header);
      }
    }
  }
  return error;
}

ostream& operator<<(ostream& out, const DBObjectMap::_Header& h)
{
  out << "seq=" << h.seq << " parent=" << h.parent 
      << " num_children=" << h.num_children
      << " ghobject=" << h.oid;
  return out;
}

int DBObjectMap::rename(const ghobject_t &from,
		       const ghobject_t &to,
		       const SequencerPosition *spos)
{
  if (from == to)
    return 0;

  MapHeaderLock _l1(this, std::min(from, to));
  MapHeaderLock _l2(this, std::max(from, to));
  MapHeaderLock *lsource, *ltarget;
  if (from > to) {
    lsource = &_l2;
    ltarget= &_l1;
  } else {
    lsource = &_l1;
    ltarget= &_l2;
  }

  KeyValueDB::Transaction t = db->get_transaction();
  {
    Header destination = lookup_map_header(*ltarget, to);
    if (destination) {
      if (check_spos(to, destination, spos))
	return 0;
      destination->num_children--;
      remove_map_header(*ltarget, to, destination, t);
      _clear(destination, t);
    }
  }

  Header hdr = lookup_map_header(*lsource, from);
  if (!hdr)
    return db->submit_transaction(t);

  remove_map_header(*lsource, from, hdr, t);
  hdr->oid = to;
  set_map_header(*ltarget, to, *hdr, t);

  return db->submit_transaction(t);
}
