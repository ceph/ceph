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

#if defined(__FreeBSD__)
#include <sys/cdefs.h>
#include <sys/param.h>
#endif

#include "FlatIndex.h"
#include "CollectionIndex.h"
#include "common/ceph_crypto.h"
#include "osd/osd_types.h"
#include <errno.h>

#include "chain_xattr.h"

using ceph::crypto::SHA1;

/*
 * long file names will have the following format:
 *
 * prefix_hash_index_cookie
 *
 * The prefix will just be the first X bytes of the original file name.
 * The cookie is a constant string that shows whether this file name
 * is hashed
 */

#define FILENAME_LFN_DIGEST_SIZE CEPH_CRYPTO_SHA1_DIGESTSIZE

#define FILENAME_MAX_LEN        4096    // the long file name size
#define FILENAME_SHORT_LEN      255     // the short file name size
#define FILENAME_COOKIE         "long"  // ceph long file name
#define FILENAME_HASH_LEN       FILENAME_LFN_DIGEST_SIZE
#define FILENAME_EXTRA	        4       // underscores and digit

#define LFN_ATTR "user.cephos.lfn"

#define FILENAME_PREFIX_LEN (FILENAME_SHORT_LEN - FILENAME_HASH_LEN - (sizeof(FILENAME_COOKIE) - 1) - FILENAME_EXTRA)

void FlatIndex::set_ref(ceph::shared_ptr<CollectionIndex> ref) {
  self_ref = ref;
}

int FlatIndex::cleanup() {
  return 0;
}

static inline void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

static int hash_filename(const char *filename, char *hash, int buf_len)
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

static void build_filename(char *filename, int len, const char *old_filename, int i)
{
  char hash[FILENAME_HASH_LEN + 1];

  assert(len >= FILENAME_SHORT_LEN + 4);

  strncpy(filename, old_filename, FILENAME_PREFIX_LEN);
  filename[FILENAME_PREFIX_LEN] = '\0';
  if (strlen(filename) < FILENAME_PREFIX_LEN)
    return;
  if (old_filename[FILENAME_PREFIX_LEN] == '\0')
    return;

  hash_filename(old_filename, hash, sizeof(hash));
  int ofs = FILENAME_PREFIX_LEN;
  while (1) {
    int suffix_len = sprintf(filename + ofs, "_%s_%d_" FILENAME_COOKIE, hash, i);
    if (ofs + suffix_len <= FILENAME_SHORT_LEN || !ofs)
      break;
    ofs--;
  }
}

/* is this a candidate? */
static int lfn_is_hashed_filename(const char *filename)
{
  int len = strlen(filename);
  if (len < FILENAME_SHORT_LEN)
    return 0;
  return (strcmp(filename + len - (sizeof(FILENAME_COOKIE) - 1), FILENAME_COOKIE) == 0);
}

static void lfn_translate(const char *path, const char *name, char *new_name, int len)
{
  if (!lfn_is_hashed_filename(name)) {
    strncpy(new_name, name, len);
    return;
  }

  char buf[PATH_MAX];

  snprintf(buf, sizeof(buf), "%s/%s", path, name);
  int r = chain_getxattr(buf, LFN_ATTR, new_name, len - 1);
  if (r < 0)
    strncpy(new_name, name, len);
  else
    new_name[r] = '\0';
  return;
}

static int append_oname(const ghobject_t &oid, char *s, int len)
{
  //assert(sizeof(oid) == 28);
  char *end = s + len;
  char *t = s + strlen(s);

  const char *i = oid.hobj.oid.name.c_str();
  while (*i && t < end) {
    if (*i == '\\') {
      *t++ = '\\';
      *t++ = '\\';      
    } else if (*i == '.' && i == oid.hobj.oid.name.c_str()) {  // only escape leading .
      *t++ = '\\';
      *t++ = '.';
    } else if (*i == '/') {
      *t++ = '\\';
      *t++ = 's';
    } else
      *t++ = *i;
    i++;
  }

  int size = t - s;

  if (oid.hobj.snap == CEPH_NOSNAP)
    size += snprintf(t, end - t, "_head");
  else if (oid.hobj.snap == CEPH_SNAPDIR)
    size += snprintf(t, end - t, "_snapdir");
  else
    size += snprintf(t, end - t, "_%llx", (long long unsigned)oid.hobj.snap);

  return size;
}

static bool parse_object(char *s, ghobject_t& oid)
{
  sobject_t o;
  char *bar = s + strlen(s) - 1;
  while (*bar != '_' &&
	 bar > s)
    bar--;
  if (*bar == '_') {
    char buf[bar-s + 1];
    char *t = buf;
    char *i = s;
    while (i < bar) {
      if (*i == '\\') {
	i++;
	switch (*i) {
	case '\\': *t++ = '\\'; break;
	case '.': *t++ = '.'; break;
	case 's': *t++ = '/'; break;
	default: assert(0);
	}
      } else {
	*t++ = *i;
      }
      i++;
    }
    *t = 0;
    o.oid.name = string(buf, t-buf);
    if (strcmp(bar+1, "head") == 0)
      o.snap = CEPH_NOSNAP;
    else if (strcmp(bar+1, "snapdir") == 0)
      o.snap = CEPH_SNAPDIR;
    else
      o.snap = strtoull(bar+1, &s, 16);
    oid = ghobject_t(hobject_t(o));
    return true;
  }
  return false;
}

static int lfn_get(const char *coll_path, const ghobject_t& oid, char *pathname, int len, char *lfn, int lfn_len, int *exist, int *is_lfn)
{
  int i = 0;
  strncpy(pathname, coll_path, len);
  size_t path_len = strlen(coll_path);
  pathname[path_len] = '/';
  path_len++;
  pathname[path_len] = '\0';
  char *filename = pathname + path_len;

  *lfn = '\0';
  int actual_len = append_oname(oid, lfn, lfn_len);

  if (actual_len < (int)FILENAME_PREFIX_LEN) {
    /* not a long file name, just build it as it is */
    strncpy(filename, lfn, len - path_len);
    *is_lfn = 0;
    struct stat buf;
    int r = ::stat(pathname, &buf);
    if (r < 0) {
      if (errno == ENOENT) {
	*exist = 0;
      } else {
	return -errno;
      }
    } else {
      *exist = 1;
    }

    return 0;
  }

  *is_lfn = 1;
  *exist = 0;

  while (1) {
    char buf[PATH_MAX];
    int r;

    build_filename(filename, len - path_len, lfn, i);
    r = chain_getxattr(pathname, LFN_ATTR, buf, sizeof(buf));
    if (r < 0)
      r = -errno;
    if (r > 0) {
      buf[MIN((int)sizeof(buf)-1, r)] = '\0';
      if (strcmp(buf, lfn) == 0) { // a match?
        *exist = 1;
        return i;
      }
    }
    switch (r) {
    case -ENOENT:
      return i;
    case -ERANGE:
      assert(0); // shouldn't happen
    default:
      break;
    }
    if (r < 0)
      break;
    i++;
  }

  return 0; // unreachable anyway
}

int FlatIndex::init() {
  return 0;
}

int FlatIndex::created(const ghobject_t &hoid, const char *path) {
  char long_name[PATH_MAX];
  long_name[0] = '\0';
  int actual_len = append_oname(hoid, long_name, sizeof(long_name));
  if (actual_len < (int)FILENAME_PREFIX_LEN) {
    return 0;
  }
  assert(long_name[actual_len] == '\0');
  assert(long_name[actual_len - 1] != '\0');
  int r = chain_setxattr(path, LFN_ATTR, long_name, actual_len);
  if (r < 0)
    return r;
  return 0;
}

int FlatIndex::unlink(const ghobject_t &o) {
  char long_fn[PATH_MAX];
  char short_fn[PATH_MAX];
  char short_fn2[PATH_MAX];
  int r, i, exist, err;
  int path_len;
  int is_lfn;

  r = lfn_get(base_path.c_str(), o, short_fn, sizeof(short_fn), 
	      long_fn, sizeof(long_fn), &exist, &is_lfn);
  if (r < 0)
    return r;
  if (!is_lfn) {
    r = ::unlink(short_fn);
    if (r < 0) {
      return -errno;
    }
    return 0;
  }
  if (!exist)
    return -ENOENT;

  const char *next = strncpy(short_fn2, base_path.c_str(), sizeof(short_fn2));
  path_len = next - short_fn2;
  short_fn2[path_len] = '/';
  path_len++;
  short_fn2[path_len] = '\0';

  for (i = r + 1; ; i++) {
    struct stat buf;
    int ret;

    build_filename(&short_fn2[path_len], sizeof(short_fn2) - path_len, long_fn, i);
    ret = ::stat(short_fn2, &buf);
    if (ret < 0) {
      if (i == r + 1) {
        err = ::unlink(short_fn);
        if (err < 0)
          return err;
        return 0;
      }
      break;
    }
  }

  build_filename(&short_fn2[path_len], sizeof(short_fn2) - path_len, long_fn, i - 1);

  if (rename(short_fn2, short_fn) < 0) {
    assert(0);
  }

  return 0;
}

int FlatIndex::lookup(const ghobject_t &hoid, IndexedPath *path, int *exist) {
  char long_fn[PATH_MAX];
  char short_fn[PATH_MAX];
  int r;
  int is_lfn;
  r = lfn_get(base_path.c_str(), hoid, 
	      short_fn, sizeof(short_fn), long_fn, 
	      sizeof(long_fn), exist, &is_lfn);
  if (r < 0)
    return r;
  *path = IndexedPath(new Path(string(short_fn), self_ref));
  return 0;
}

static int get_hobject_from_oinfo(const char *dir, const char *file, 
				  ghobject_t *o) {
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

int FlatIndex::collection_list_partial(const ghobject_t &start,
				       int min_count,
				       int max_count,
				       snapid_t seq,
				       vector<ghobject_t> *ls,
				       ghobject_t *next) {
  assert(0); // Should not be called
  return 0;
}

int FlatIndex::collection_list(vector<ghobject_t> *ls) {
  char buf[offsetof(struct dirent, d_name) + PATH_MAX + 1];
  char dir_name[PATH_MAX], new_name[PATH_MAX];
  strncpy(dir_name, base_path.c_str(), sizeof(dir_name));
  dir_name[sizeof(dir_name)-1]='\0';

  DIR *dir = ::opendir(dir_name);
  if (!dir)
    return -errno;
  
  // first, build (ino, object) list
  vector< pair<ino_t,ghobject_t> > inolist;

  struct dirent *de;
  while (::readdir_r(dir, (struct dirent *)buf, &de) == 0) {
    if (!de)
      break;
    // parse
    if (de->d_name[0] == '.')
      continue;
    //cout << "  got object " << de->d_name << std::endl;
    ghobject_t o;
    lfn_translate(dir_name, de->d_name, new_name, sizeof(new_name));
    if (parse_object(new_name, o)) {
      get_hobject_from_oinfo(dir_name, de->d_name, &o);
      inolist.push_back(pair<ino_t,ghobject_t>(de->d_ino, o));
      ls->push_back(o);
    }
  }

  // sort
  sort(inolist.begin(), inolist.end());

  // build final list
  ls->resize(inolist.size());
  int i = 0;
  for (vector< pair<ino_t,ghobject_t> >::iterator p = inolist.begin(); p != inolist.end(); ++p)
    (*ls)[i++].swap(p->second);
  
  ::closedir(dir);
  return 0;
}
