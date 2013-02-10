// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <string>
#include <map>
#include <set>
#include <vector>
#include <errno.h>
#include <string.h>

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include "osd/osd_types.h"
#include "include/object.h"
#include "common/config.h"
#include "common/debug.h"
#include "include/buffer.h"
#include "common/ceph_crypto.h"
#include "include/compat.h"
#include "chain_xattr.h"

#include "LFNIndex.h"
using ceph::crypto::SHA1;

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "LFNIndex(" << get_base_path() << ") "


const string LFNIndex::LFN_ATTR = "user.cephos.lfn";
const string LFNIndex::PHASH_ATTR_PREFIX = "user.cephos.phash.";
const string LFNIndex::SUBDIR_PREFIX = "DIR_";
const string LFNIndex::FILENAME_COOKIE = "long";
const int LFNIndex::FILENAME_PREFIX_LEN =  FILENAME_SHORT_LEN - FILENAME_HASH_LEN - 
								FILENAME_COOKIE.size() - 
								FILENAME_EXTRA;
void LFNIndex::maybe_inject_failure() {
  if (error_injection_enabled) {
    if (current_failure > last_failure &&
	(((double)(rand() % 10000))/((double)(10000))
	 < error_injection_probability)) {
      last_failure = current_failure;
      current_failure = 0;
      throw RetryException();
    }
    ++current_failure;
  }
}

/* Public methods */

void LFNIndex::set_ref(std::tr1::shared_ptr<CollectionIndex> ref) {
  self_ref = ref;
}

int LFNIndex::init() {
  return _init();
}

int LFNIndex::created(const hobject_t &hoid, const char *path) {
  vector<string> path_comp;
  string short_name;
  int r;
  r = decompose_full_path(path, &path_comp, 0, &short_name);
  if (r < 0)
    return r;
  r = lfn_created(path_comp, hoid, short_name);
  if (r < 0)
    return r;
  return _created(path_comp, hoid, short_name);
}

int LFNIndex::unlink(const hobject_t &hoid) {
  WRAP_RETRY(
  vector<string> path;
  string short_name;
  r = _lookup(hoid, &path, &short_name, NULL);
  if (r < 0) {
    goto out;
  }
  r = _remove(path, hoid, short_name);
  if (r < 0) {
    goto out;
  }
  );
}

int LFNIndex::lookup(const hobject_t &hoid,
		     IndexedPath *out_path,
		     int *exist) {
  WRAP_RETRY(
  vector<string> path;
  string short_name;
  r = _lookup(hoid, &path, &short_name, exist);
  if (r < 0)
    goto out;
  string full_path = get_full_path(path, short_name);
  struct stat buf;
  maybe_inject_failure();
  r = ::stat(full_path.c_str(), &buf);
  maybe_inject_failure();
  if (r < 0) {
    if (errno == ENOENT) {
      *exist = 0;
    } else {
      r = -errno;
      goto out;
    }
  } else {
    *exist = 1;
  }
  *out_path = IndexedPath(new Path(full_path, self_ref));
  r = 0;
  );
}

int LFNIndex::collection_list(vector<hobject_t> *ls) {
  return _collection_list(ls);
}


int LFNIndex::collection_list_partial(const hobject_t &start,
				      int min_count,
				      int max_count,
				      snapid_t seq,
				      vector<hobject_t> *ls,
				      hobject_t *next) {
  return _collection_list_partial(start, min_count, max_count, seq, ls, next);
}

/* Derived class utility methods */

int LFNIndex::fsync_dir(const vector<string> &path) {
  maybe_inject_failure();
  int fd = ::open(get_full_path_subdir(path).c_str(), O_RDONLY);
  if (fd < 0)
    return -errno;
  maybe_inject_failure();
  int r = ::fsync(fd);
  TEMP_FAILURE_RETRY(::close(fd));
  maybe_inject_failure();
  if (r < 0)
    return -errno;
  else
    return 0;
}

int LFNIndex::link_object(const vector<string> &from,
			  const vector<string> &to,
			  const hobject_t &hoid,
			  const string &from_short_name) {
  int r;
  string from_path = get_full_path(from, from_short_name);
  string to_path;
  maybe_inject_failure();
  r = lfn_get_name(to, hoid, 0, &to_path, 0);
  if (r < 0)
    return r;
  maybe_inject_failure();
  r = ::link(from_path.c_str(), to_path.c_str());
  maybe_inject_failure();
  if (r < 0)
    return -errno;
  else
    return 0;
}

int LFNIndex::remove_objects(const vector<string> &dir,
			     const map<string, hobject_t> &to_remove,
			     map<string, hobject_t> *remaining) {
  set<string> clean_chains;
  for (map<string, hobject_t>::const_iterator to_clean = to_remove.begin();
       to_clean != to_remove.end();
       ++to_clean) {
    if (!lfn_is_hashed_filename(to_clean->first)) {
      maybe_inject_failure();
      int r = ::unlink(get_full_path(dir, to_clean->first).c_str());
      maybe_inject_failure();
      if (r < 0)
	return -errno;
      continue;
    }
    if (clean_chains.count(lfn_get_short_name(to_clean->second, 0)))
      continue;
    set<int> holes;
    map<int, pair<string, hobject_t> > chain;
    for (int i = 0; ; ++i) {
      string short_name = lfn_get_short_name(to_clean->second, i);
      if (remaining->count(short_name)) {
	chain[i] = *(remaining->find(short_name));
      } else if (to_remove.count(short_name)) {
	holes.insert(i);
      } else {
	break;
      }
    }

    map<int, pair<string, hobject_t > >::reverse_iterator candidate = chain.rbegin();
    for (set<int>::iterator i = holes.begin();
	 i != holes.end();
	 ++i) {
      if (candidate == chain.rend() || *i > candidate->first) {
	string remove_path_name = 
	  get_full_path(dir, lfn_get_short_name(to_clean->second, *i)); 
	maybe_inject_failure();
	int r = ::unlink(remove_path_name.c_str());
	maybe_inject_failure();
	if (r < 0)
	  return -errno;
	continue;
      }
      string from = get_full_path(dir, candidate->second.first);
      string to = get_full_path(dir, lfn_get_short_name(candidate->second.second, *i));
      maybe_inject_failure();
      int r = ::rename(from.c_str(), to.c_str());
      maybe_inject_failure();
      if (r < 0)
	return -errno;
      remaining->erase(candidate->second.first);
      remaining->insert(pair<string, hobject_t>(
			  lfn_get_short_name(candidate->second.second, *i),
					     candidate->second.second));
      candidate++;
    }
    if (holes.size() > 0)
      clean_chains.insert(lfn_get_short_name(to_clean->second, 0));
  }
  return 0;
}

int LFNIndex::move_objects(const vector<string> &from,
			   const vector<string> &to) {
  map<string, hobject_t> to_move;
  int r;
  r = list_objects(from, 0, NULL, &to_move);
  if (r < 0)
    return r;
  for (map<string,hobject_t>::iterator i = to_move.begin();
       i != to_move.end();
       ++i) {
    string from_path = get_full_path(from, i->first);
    string to_path, to_name;
    r = lfn_get_name(to, i->second, &to_name, &to_path, 0);
    if (r < 0)
      return r;
    maybe_inject_failure();
    r = ::link(from_path.c_str(), to_path.c_str());
    if (r < 0 && errno != EEXIST)
      return -errno;
    maybe_inject_failure();
    r = lfn_created(to, i->second, to_name);
    maybe_inject_failure();
    if (r < 0)
      return r;
  }
  r = fsync_dir(to);
  if (r < 0)
    return r;
  for (map<string,hobject_t>::iterator i = to_move.begin();
       i != to_move.end();
       ++i) {
    maybe_inject_failure();
    r = ::unlink(get_full_path(from, i->first).c_str());
    maybe_inject_failure();
    if (r < 0)
      return -errno;
  }
  return fsync_dir(from);
}

int LFNIndex::remove_object(const vector<string> &from,
			    const hobject_t &hoid) {
  string short_name;
  int r, exist;
  maybe_inject_failure();
  r = get_mangled_name(from, hoid, &short_name, &exist);
  maybe_inject_failure();
  if (r < 0)
    return r;
  return lfn_unlink(from, hoid, short_name);
}

int LFNIndex::get_mangled_name(const vector<string> &from,
			       const hobject_t &hoid,
			       string *mangled_name, int *exists) {
  return lfn_get_name(from, hoid, mangled_name, 0, exists);
}

int LFNIndex::move_subdir(
  LFNIndex &from,
  LFNIndex &dest,
  const vector<string> &path,
  string dir
  ) {
  vector<string> sub_path(path.begin(), path.end());
  sub_path.push_back(dir);
  string from_path(from.get_full_path_subdir(sub_path));
  string to_path(dest.get_full_path_subdir(sub_path));
  int r = ::rename(from_path.c_str(), to_path.c_str());
  if (r < 0)
    return -errno;
  return 0;
}

int LFNIndex::move_object(
  LFNIndex &from,
  LFNIndex &dest,
  const vector<string> &path,
  const pair<string, hobject_t> &obj
  ) {
  string from_path(from.get_full_path(path, obj.first));
  string to_path;
  string to_name;
  int exists;
  int r = dest.lfn_get_name(path, obj.second, &to_name, &to_path, &exists);
  if (r < 0)
    return r;
  if (!exists) {
    r = ::link(from_path.c_str(), to_path.c_str());
    if (r < 0)
      return r;
  }
  r = dest.lfn_created(path, obj.second, to_name);
  if (r < 0)
    return r;
  r = dest.fsync_dir(path);
  if (r < 0)
    return r;
  r = from.remove_object(path, obj.second);
  if (r < 0)
    return r;
  return from.fsync_dir(path);
}


static int get_hobject_from_oinfo(const char *dir, const char *file, 
				  hobject_t *o) {
  char path[PATH_MAX];
  bufferptr bp(PATH_MAX);
  snprintf(path, sizeof(path), "%s/%s", dir, file);
  // Hack, user.ceph._ is the attribute used to store the object info
  int r = chain_getxattr(path, "user.ceph._", bp.c_str(), bp.length());
  if (r < 0)
    return r;
  bufferlist bl;
  bl.push_back(bp);
  object_info_t oi(bl);
  *o = oi.soid;
  return 0;
}


int LFNIndex::list_objects(const vector<string> &to_list, int max_objs,
			   long *handle, map<string, hobject_t> *out) {
  string to_list_path = get_full_path_subdir(to_list);
  DIR *dir = ::opendir(to_list_path.c_str());
  char buf[PATH_MAX];
  int r;
  if (!dir) {
    return -errno;
  }

  if (handle && *handle) {
    seekdir(dir, *handle);
  }

  struct dirent *de;
  int listed = 0;
  bool end = false;
  while (!::readdir_r(dir, reinterpret_cast<struct dirent*>(buf), &de)) {
    if (!de) {
      end = true;
      break;
    }
    if (max_objs > 0 && listed >= max_objs) {
      break;
    }
    if (de->d_name[0] == '.')
      continue;
    string short_name(de->d_name);
    hobject_t obj;
    if (lfn_is_object(short_name)) {
      r = lfn_translate(to_list, short_name, &obj);
      if (r < 0) {
	r = -errno;
	goto cleanup;
      } else if (r > 0) {
	string long_name = lfn_generate_object_name(obj);
	if (!lfn_must_hash(long_name)) {
	  assert(long_name == short_name);
	}
	if (index_version == HASH_INDEX_TAG)
	  get_hobject_from_oinfo(to_list_path.c_str(), short_name.c_str(), &obj);
	  
	out->insert(pair<string, hobject_t>(short_name, obj));
	++listed;
      } else {
	continue;
      }
    }
  }

  if (handle && !end) {
    *handle = telldir(dir);
  }

  r = 0;
 cleanup:
  ::closedir(dir);
  return r;
}

int LFNIndex::list_subdirs(const vector<string> &to_list,
				  set<string> *out) {
  string to_list_path = get_full_path_subdir(to_list);
  DIR *dir = ::opendir(to_list_path.c_str());
  char buf[PATH_MAX];
  if (!dir)
    return -errno;

  struct dirent *de;
  while (!::readdir_r(dir, reinterpret_cast<struct dirent*>(buf), &de)) {
    if (!de) {
      break;
    }
    string short_name(de->d_name);
    string demangled_name;
    hobject_t obj;
    if (lfn_is_subdir(short_name, &demangled_name)) {
      out->insert(demangled_name);
    }
  }

  ::closedir(dir);
  return 0;
}

int LFNIndex::create_path(const vector<string> &to_create) {
  maybe_inject_failure();
  int r = ::mkdir(get_full_path_subdir(to_create).c_str(), 0777);
  maybe_inject_failure();
  if (r < 0)
    return -errno;
  else
    return 0;
}

int LFNIndex::remove_path(const vector<string> &to_remove) {
  maybe_inject_failure();
  int r = ::rmdir(get_full_path_subdir(to_remove).c_str());
  maybe_inject_failure();
  if (r < 0)
    return -errno;
  else
    return 0;
}

int LFNIndex::path_exists(const vector<string> &to_check, int *exists) {
  string full_path = get_full_path_subdir(to_check);
  struct stat buf;
  if (::stat(full_path.c_str(), &buf)) {
    int r = -errno;
    if (r == -ENOENT) {
      *exists = 0;
      return 0;
    } else {
      return r;
    }
  } else {
    *exists = 1;
    return 0;
  }
}

int LFNIndex::add_attr_path(const vector<string> &path,
			    const string &attr_name, 
			    bufferlist &attr_value) {
  string full_path = get_full_path_subdir(path);
  maybe_inject_failure();
  return chain_setxattr(full_path.c_str(), mangle_attr_name(attr_name).c_str(),
		     reinterpret_cast<void *>(attr_value.c_str()),
		     attr_value.length());
}

int LFNIndex::get_attr_path(const vector<string> &path,
			    const string &attr_name, 
			    bufferlist &attr_value) {
  string full_path = get_full_path_subdir(path);
  size_t size = 1024; // Initial
  while (1) {
    bufferptr buf(size);
    int r = chain_getxattr(full_path.c_str(), mangle_attr_name(attr_name).c_str(),
			 reinterpret_cast<void *>(buf.c_str()),
			 size);
    if (r > 0) {
      buf.set_length(r);
      attr_value.push_back(buf);
      break;
    } else {
      r = -errno;
      if (r == -ERANGE) {
	size *= 2;
      } else {
	return r;
      }
    }
  }
  return 0;
}

int LFNIndex::remove_attr_path(const vector<string> &path,
			       const string &attr_name) {
  string full_path = get_full_path_subdir(path);
  string mangled_attr_name = mangle_attr_name(attr_name);
  maybe_inject_failure();
  return chain_removexattr(full_path.c_str(), mangled_attr_name.c_str());
}
  
string LFNIndex::lfn_generate_object_name_keyless(const hobject_t &hoid) {
  char s[FILENAME_MAX_LEN];
  char *end = s + sizeof(s);
  char *t = s;

  const char *i = hoid.oid.name.c_str();
  // Escape subdir prefix
  if (hoid.oid.name.substr(0, 4) == "DIR_") {
    *t++ = '\\';
    *t++ = 'd';
    i += 4;
  }
  while (*i && t < end) {
    if (*i == '\\') {
      *t++ = '\\';
      *t++ = '\\';      
    } else if (*i == '.' && i == hoid.oid.name.c_str()) {  // only escape leading .
      *t++ = '\\';
      *t++ = '.';
    } else if (*i == '/') {
      *t++ = '\\';
      *t++ = 's';
    } else
      *t++ = *i;
    i++;
  }

  if (hoid.snap == CEPH_NOSNAP)
    t += snprintf(t, end - t, "_head");
  else if (hoid.snap == CEPH_SNAPDIR)
    t += snprintf(t, end - t, "_snapdir");
  else
    t += snprintf(t, end - t, "_%llx", (long long unsigned)hoid.snap);
  snprintf(t, end - t, "_%.*X", (int)(sizeof(hoid.hash)*2), hoid.hash);

  return string(s);
}

static void append_escaped(string::const_iterator begin,
			   string::const_iterator end, 
			   string *out) {
  for (string::const_iterator i = begin; i != end; ++i) {
    if (*i == '\\') {
      out->append("\\\\");
    } else if (*i == '/') {
      out->append("\\s");
    } else if (*i == '_') {
      out->append("\\u");
    } else if (*i == '\0') {
      out->append("\\n");
    } else {
      out->append(i, i+1);
    }
  }
}

string LFNIndex::lfn_generate_object_name(const hobject_t &hoid) {
  if (index_version == HASH_INDEX_TAG)
    return lfn_generate_object_name_keyless(hoid);
  if (index_version == HASH_INDEX_TAG_2)
    return lfn_generate_object_name_poolless(hoid);

  string full_name;
  string::const_iterator i = hoid.oid.name.begin();
  if (hoid.oid.name.substr(0, 4) == "DIR_") {
    full_name.append("\\d");
    i += 4;
  } else if (hoid.oid.name[0] == '.') {
    full_name.append("\\.");
    ++i;
  }
  append_escaped(i, hoid.oid.name.end(), &full_name);
  full_name.append("_");
  append_escaped(hoid.get_key().begin(), hoid.get_key().end(), &full_name);
  full_name.append("_");

  char buf[PATH_MAX];
  char *t = buf;
  char *end = t + sizeof(buf);
  if (hoid.snap == CEPH_NOSNAP)
    t += snprintf(t, end - t, "head");
  else if (hoid.snap == CEPH_SNAPDIR)
    t += snprintf(t, end - t, "snapdir");
  else
    t += snprintf(t, end - t, "%llx", (long long unsigned)hoid.snap);
  snprintf(t, end - t, "_%.*X", (int)(sizeof(hoid.hash)*2), hoid.hash);
  full_name += string(buf);
  full_name.append("_");

  append_escaped(hoid.nspace.begin(), hoid.nspace.end(), &full_name);
  full_name.append("_");

  t = buf;
  end = t + sizeof(buf);
  if (hoid.pool == -1)
    t += snprintf(t, end - t, "none");
  else
    t += snprintf(t, end - t, "%llx", (long long unsigned)hoid.pool);
  full_name += string(buf);

  return full_name;
}

string LFNIndex::lfn_generate_object_name_poolless(const hobject_t &hoid) {
  if (index_version == HASH_INDEX_TAG)
    return lfn_generate_object_name_keyless(hoid);

  string full_name;
  string::const_iterator i = hoid.oid.name.begin();
  if (hoid.oid.name.substr(0, 4) == "DIR_") {
    full_name.append("\\d");
    i += 4;
  } else if (hoid.oid.name[0] == '.') {
    full_name.append("\\.");
    ++i;
  }
  append_escaped(i, hoid.oid.name.end(), &full_name);
  full_name.append("_");
  append_escaped(hoid.get_key().begin(), hoid.get_key().end(), &full_name);
  full_name.append("_");

  char snap_with_hash[PATH_MAX];
  char *t = snap_with_hash;
  char *end = t + sizeof(snap_with_hash);
  if (hoid.snap == CEPH_NOSNAP)
    t += snprintf(t, end - t, "head");
  else if (hoid.snap == CEPH_SNAPDIR)
    t += snprintf(t, end - t, "snapdir");
  else
    t += snprintf(t, end - t, "%llx", (long long unsigned)hoid.snap);
  snprintf(t, end - t, "_%.*X", (int)(sizeof(hoid.hash)*2), hoid.hash);
  full_name += string(snap_with_hash);
  return full_name;
}

int LFNIndex::lfn_get_name(const vector<string> &path, 
			   const hobject_t &hoid,
			   string *mangled_name, string *out_path,
			   int *exists) {
  string subdir_path = get_full_path_subdir(path);
  string full_name = lfn_generate_object_name(hoid);
  int r;

  if (!lfn_must_hash(full_name)) {
    if (mangled_name)
      *mangled_name = full_name;
    if (out_path)
      *out_path = get_full_path(path, full_name);
    if (exists) {
      struct stat buf;
      string full_path = get_full_path(path, full_name);
      maybe_inject_failure();
      r = ::stat(full_path.c_str(), &buf);
      if (r < 0) {
	if (errno == ENOENT)
	  *exists = 0;
	else
	  return -errno;
      } else {
	*exists = 1;
      }
    }
    return 0;
  }

  int i = 0;
  string candidate;
  string candidate_path;
  char buf[FILENAME_MAX_LEN + 1];
  for ( ; ; ++i) {
    candidate = lfn_get_short_name(hoid, i);
    candidate_path = get_full_path(path, candidate);
    r = chain_getxattr(candidate_path.c_str(), get_lfn_attr().c_str(), buf, sizeof(buf));
    if (r < 0) {
      if (errno != ENODATA && errno != ENOENT)
	return -errno;
      if (errno == ENODATA) {
	// Left over from incomplete transaction, it'll be replayed
	maybe_inject_failure();
	r = ::unlink(candidate_path.c_str());
	maybe_inject_failure();
	if (r < 0)
	  return -errno;
      }
      if (mangled_name)
	*mangled_name = candidate;
      if (out_path)
	*out_path = candidate_path;
      if (exists)
	*exists = 0;
      return 0;
    }
    assert(r > 0);
    buf[MIN((int)sizeof(buf) - 1, r)] = '\0';
    if (!strcmp(buf, full_name.c_str())) {
      if (mangled_name)
	*mangled_name = candidate;
      if (out_path)
	*out_path = candidate_path;
      if (exists)
	*exists = 1;
      return 0;
    }
  }
  assert(0); // Unreachable
  return 0;
}

int LFNIndex::lfn_created(const vector<string> &path,
			  const hobject_t &hoid,
			  const string &mangled_name) {
  if (!lfn_is_hashed_filename(mangled_name))
    return 0;
  string full_path = get_full_path(path, mangled_name);
  string full_name = lfn_generate_object_name(hoid);
  maybe_inject_failure();
  return chain_setxattr(full_path.c_str(), get_lfn_attr().c_str(), 
		     full_name.c_str(), full_name.size());
}

int LFNIndex::lfn_unlink(const vector<string> &path,
			 const hobject_t &hoid,
			 const string &mangled_name) {
  if (!lfn_is_hashed_filename(mangled_name)) {
    string full_path = get_full_path(path, mangled_name);
    maybe_inject_failure();
    int r = ::unlink(full_path.c_str());
    maybe_inject_failure();
    if (r < 0)
      return -errno;
    return 0;
  }
  string subdir_path = get_full_path_subdir(path);
  
  
  int i = 0;
  for ( ; ; ++i) {
    string candidate = lfn_get_short_name(hoid, i);
    if (candidate == mangled_name)
      break;
  }
  int removed_index = i;
  ++i;
  for ( ; ; ++i) {
    struct stat buf;
    string to_check = lfn_get_short_name(hoid, i);
    string to_check_path = get_full_path(path, to_check);
    int r = ::stat(to_check_path.c_str(), &buf);
    if (r < 0) {
      if (errno == ENOENT) {
	break;
      } else {
	return -errno;
      }
    }
  }
  if (i == removed_index + 1) {
    string full_path = get_full_path(path, mangled_name);
    maybe_inject_failure();
    int r = ::unlink(full_path.c_str());
    maybe_inject_failure();
    if (r < 0)
      return -errno;
    else
      return 0;
  } else {
    string rename_to = get_full_path(path, mangled_name);
    string rename_from = get_full_path(path, lfn_get_short_name(hoid, i - 1));
    maybe_inject_failure();
    int r = ::rename(rename_from.c_str(), rename_to.c_str());
    maybe_inject_failure();
    if (r < 0)
      return -errno;
    else
      return 0;
  }
}

int LFNIndex::lfn_translate(const vector<string> &path,
			    const string &short_name,
			    hobject_t *out) {
  if (!lfn_is_hashed_filename(short_name)) {
    return lfn_parse_object_name(short_name, out);
  }
  // Get lfn_attr
  string full_path = get_full_path(path, short_name);
  char attr[PATH_MAX];
  int r = chain_getxattr(full_path.c_str(), get_lfn_attr().c_str(), attr, sizeof(attr) - 1);
  if (r < 0)
    return -errno;
  if (r < (int)sizeof(attr))
    attr[r] = '\0';

  string long_name(attr);
  return lfn_parse_object_name(long_name, out);
}

bool LFNIndex::lfn_is_object(const string &short_name) {
  return lfn_is_hashed_filename(short_name) || !lfn_is_subdir(short_name, 0);
}

bool LFNIndex::lfn_is_subdir(const string &name, string *demangled) {
  if (name.substr(0, SUBDIR_PREFIX.size()) == SUBDIR_PREFIX) {
    if (demangled)
      *demangled = demangle_path_component(name);
    return 1;
  }
  return 0;
}

static int parse_object(const char *s, hobject_t& o)
{
  const char *hash = s + strlen(s) - 1;
  while (*hash != '_' &&
	 hash > s)
    hash--;
  const char *bar = hash - 1;
  while (*bar != '_' &&
	 bar > s)
    bar--;
  if (*bar == '_') {
    char buf[bar-s + 1];
    char *t = buf;
    const char *i = s;
    while (i < bar) {
      if (*i == '\\') {
	i++;
	switch (*i) {
	case '\\': *t++ = '\\'; break;
	case '.': *t++ = '.'; break;
	case 's': *t++ = '/'; break;
	case 'd': {
	  *t++ = 'D';
	  *t++ = 'I';
	  *t++ = 'R';
	  *t++ = '_';
	  break;
	}
	default: assert(0);
	}
      } else {
	*t++ = *i;
      }
      i++;
    }
    *t = 0;
    o.oid.name = string(buf, t-buf);
    if (strncmp(bar+1, "head", 4) == 0)
      o.snap = CEPH_NOSNAP;
    else if (strncmp(bar+1, "snapdir", 7) == 0)
      o.snap = CEPH_SNAPDIR;
    else 
      o.snap = strtoull(bar+1, NULL, 16);
    sscanf(hash, "_%X", &o.hash);

    return 1;
  }
  return 0;
}

bool LFNIndex::lfn_parse_object_name_keyless(const string &long_name, hobject_t *out) {
  bool r = parse_object(long_name.c_str(), *out);
  int64_t pool = -1;
  pg_t pg;
  if (coll().is_pg_prefix(pg))
    pool = (int64_t)pg.pool();
  out->pool = pool;
  if (!r) return r;
  string temp = lfn_generate_object_name(*out);
  return r;
}

static bool append_unescaped(string::const_iterator begin,
			     string::const_iterator end, 
			     string *out) {
  for (string::const_iterator i = begin; i != end; ++i) {
    if (*i == '\\') {
      ++i;
      if (*i == '\\')
	out->append("\\");
      else if (*i == 's')
	out->append("/");
      else if (*i == 'n')
	out->append('\0');
      else if (*i == 'u')
	out->append("_");
      else
	return false;
    } else {
      out->append(i, i+1);
    }
  }
  return true;
}

bool LFNIndex::lfn_parse_object_name_poolless(const string &long_name,
					      hobject_t *out) {
  string name;
  string key;
  uint32_t hash;
  snapid_t snap;

  string::const_iterator current = long_name.begin();
  if (*current == '\\') {
    ++current;
    if (current == long_name.end()) {
      return false;
    } else if (*current == 'd') {
      name.append("DIR_");
      ++current;
    } else if (*current == '.') {
      name.append(".");
      ++current;
    } else {
      --current;
    }
  }

  string::const_iterator end = current;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end == long_name.end())
    return false;
  if (!append_unescaped(current, end, &name))
    return false;

  current = ++end;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end == long_name.end())
    return false;
  if (!append_unescaped(current, end, &key))
    return false;

  current = ++end;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end == long_name.end())
    return false;
  string snap_str(current, end);

  current = ++end;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end != long_name.end())
    return false;
  string hash_str(current, end);

  if (snap_str == "head")
    snap = CEPH_NOSNAP;
  else if (snap_str == "snapdir")
    snap = CEPH_SNAPDIR;
  else
    snap = strtoull(snap_str.c_str(), NULL, 16);
  sscanf(hash_str.c_str(), "%X", &hash);


  int64_t pool = -1;
  pg_t pg;
  if (coll().is_pg_prefix(pg))
    pool = (int64_t)pg.pool();
  (*out) = hobject_t(name, key, snap, hash, pool);
  return true;
}


bool LFNIndex::lfn_parse_object_name(const string &long_name, hobject_t *out) {
  string name;
  string key;
  string ns;
  uint32_t hash;
  snapid_t snap;
  uint64_t pool;

  if (index_version == HASH_INDEX_TAG)
    return lfn_parse_object_name_keyless(long_name, out);
  if (index_version == HASH_INDEX_TAG_2)
    return lfn_parse_object_name_poolless(long_name, out);

  string::const_iterator current = long_name.begin();
  if (*current == '\\') {
    ++current;
    if (current == long_name.end()) {
      return false;
    } else if (*current == 'd') {
      name.append("DIR_");
      ++current;
    } else if (*current == '.') {
      name.append(".");
      ++current;
    } else {
      --current;
    }
  }

  string::const_iterator end = current;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end == long_name.end())
    return false;
  if (!append_unescaped(current, end, &name))
    return false;

  current = ++end;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end == long_name.end())
    return false;
  if (!append_unescaped(current, end, &key))
    return false;

  current = ++end;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end == long_name.end())
    return false;
  string snap_str(current, end);

  current = ++end;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end == long_name.end())
    return false;
  string hash_str(current, end);

  current = ++end;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end == long_name.end())
    return false;
  if (!append_unescaped(current, end, &ns))
    return false;

  current = ++end;
  for ( ; end != long_name.end() && *end != '_'; ++end) ;
  if (end != long_name.end())
    return false;
  string pstring(current, end);

  if (snap_str == "head")
    snap = CEPH_NOSNAP;
  else if (snap_str == "snapdir")
    snap = CEPH_SNAPDIR;
  else
    snap = strtoull(snap_str.c_str(), NULL, 16);
  sscanf(hash_str.c_str(), "%X", &hash);

  if (pstring == "none")
    pool = (uint64_t)-1;
  else
    pool = strtoull(pstring.c_str(), NULL, 16);

  (*out) = hobject_t(name, key, snap, hash, (int64_t)pool);
  return true;
}

bool LFNIndex::lfn_is_hashed_filename(const string &name) {
  if (name.size() < (unsigned)FILENAME_SHORT_LEN) {
    return 0;
  }
  if (name.substr(name.size() - FILENAME_COOKIE.size(), FILENAME_COOKIE.size())
      == FILENAME_COOKIE) {
    return 1;
  } else {
    return 0;
  }
}

bool LFNIndex::lfn_must_hash(const string &long_name) {
  return (int)long_name.size() >= FILENAME_SHORT_LEN;
}

static inline void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

int LFNIndex::hash_filename(const char *filename, char *hash, int buf_len)
{
  if (buf_len < FILENAME_HASH_LEN + 1)
    return -EINVAL;

  char buf[FILENAME_LFN_DIGEST_SIZE];
  char hex[FILENAME_LFN_DIGEST_SIZE * 2];

  SHA1 h;
  h.Update((const byte *)filename, strlen(filename));
  h.Final((byte *)buf);

  buf_to_hex((byte *)buf, (FILENAME_HASH_LEN + 1) / 2, hex);
  strncpy(hash, hex, FILENAME_HASH_LEN);
  hash[FILENAME_HASH_LEN] = '\0';
  return 0;
}

void LFNIndex::build_filename(const char *old_filename, int i, char *filename, int len)
{
  char hash[FILENAME_HASH_LEN + 1];

  assert(len >= FILENAME_SHORT_LEN + 4);

  strncpy(filename, old_filename, FILENAME_PREFIX_LEN);
  filename[FILENAME_PREFIX_LEN] = '\0';
  if ((int)strlen(filename) < FILENAME_PREFIX_LEN)
    return;
  if (old_filename[FILENAME_PREFIX_LEN] == '\0')
    return;

  hash_filename(old_filename, hash, sizeof(hash));
  int ofs = FILENAME_PREFIX_LEN;
  int suffix_len;
  while (1) {
    suffix_len = sprintf(filename + ofs, "_%s_%d_%s", hash, i, FILENAME_COOKIE.c_str());
    if (ofs + suffix_len <= FILENAME_SHORT_LEN || !ofs)
      break;
    ofs--;
  }
}

string LFNIndex::lfn_get_short_name(const hobject_t &hoid, int i) {
  string long_name = lfn_generate_object_name(hoid);
  assert(lfn_must_hash(long_name));
  char buf[FILENAME_SHORT_LEN + 4];
  build_filename(long_name.c_str(), i, buf, sizeof(buf));
  return string(buf);
}

const string &LFNIndex::get_base_path() {
  return base_path;
}

string LFNIndex::get_full_path_subdir(const vector<string> &rel) {
  string retval = get_base_path();
  for (vector<string>::const_iterator i = rel.begin();
       i != rel.end();
       ++i) {
    retval += "/";
    retval += mangle_path_component(*i);
  }
  return retval;
}

string LFNIndex::get_full_path(const vector<string> &rel, const string &name) {
  return get_full_path_subdir(rel) + "/" + name;
}

string LFNIndex::mangle_path_component(const string &component) {
  return SUBDIR_PREFIX + component;
}

string LFNIndex::demangle_path_component(const string &component) {
  return component.substr(SUBDIR_PREFIX.size(), component.size() - SUBDIR_PREFIX.size());
}

int LFNIndex::decompose_full_path(const char *in, vector<string> *out,
				  hobject_t *hoid, string *shortname) {
  const char *beginning = in + get_base_path().size();
  const char *end = beginning;
  while (1) {
    end++;
    beginning = end++;
    for ( ; *end != '\0' && *end != '/'; ++end) ;
    if (*end != '\0') {
      out->push_back(demangle_path_component(string(beginning, end - beginning)));
      continue;
    } else {
      break;
    }
  }
  *shortname = string(beginning, end - beginning);
  if (hoid) {
    int r = lfn_translate(*out, *shortname, hoid);
    if (r < 0)
      return r;
  }
  return 0;
}

string LFNIndex::mangle_attr_name(const string &attr) {
  return PHASH_ATTR_PREFIX + attr;
}
