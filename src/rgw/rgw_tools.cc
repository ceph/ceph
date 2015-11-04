// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/errno.h"
#include "common/safe_io.h"

#include "include/types.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

#define READ_CHUNK_LEN (512 * 1024)

static map<string, string> ext_mime_map;

int rgw_put_system_obj(RGWRados *rgwstore, rgw_bucket& bucket, string& oid, const char *data, size_t size, bool exclusive,
                       RGWObjVersionTracker *objv_tracker, time_t set_mtime, map<string, bufferlist> *pattrs)
{
  map<string,bufferlist> no_attrs;
  if (!pattrs)
    pattrs = &no_attrs;

  rgw_obj obj(bucket, oid);

  int ret = rgwstore->put_system_obj(NULL, obj, data, size, exclusive, NULL, *pattrs, objv_tracker, set_mtime);

  if (ret == -ENOENT) {
    ret = rgwstore->create_pool(bucket);
    if (ret >= 0)
      ret = rgwstore->put_system_obj(NULL, obj, data, size, exclusive, NULL, *pattrs, objv_tracker, set_mtime);
  }

  return ret;
}

int rgw_get_system_obj(RGWRados *rgwstore, RGWObjectCtx& obj_ctx, rgw_bucket& bucket, const string& key, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker, time_t *pmtime, map<string, bufferlist> *pattrs,
                       rgw_cache_entry_info *cache_info)
{
  struct rgw_err err;
  bufferlist::iterator iter;
  int request_len = READ_CHUNK_LEN;
  rgw_obj obj(bucket, key);

  do {
    RGWRados::SystemObject source(rgwstore, obj_ctx, obj);
    RGWRados::SystemObject::Read rop(&source);

    rop.stat_params.attrs = pattrs;
    rop.stat_params.lastmod = pmtime;

    int ret = rop.stat(objv_tracker);
    if (ret < 0)
      return ret;

    rop.read_params.cache_info = cache_info;

    ret = rop.read(0, request_len - 1, bl, objv_tracker);
    if (ret == -ECANCELED) {
      /* raced, restart */
      continue;
    }
    if (ret < 0)
      return ret;

    if (ret < request_len)
      break;
    bl.clear();
    request_len *= 2;
  } while (true);

  return 0;
}

void parse_mime_map_line(const char *start, const char *end)
{
  char line[end - start + 1];
  strncpy(line, start, end - start);
  line[end - start] = '\0';
  char *l = line;
#define DELIMS " \t\n\r"

  while (isspace(*l))
    l++;

  char *mime = strsep(&l, DELIMS);
  if (!mime)
    return;

  char *ext;
  do {
    ext = strsep(&l, DELIMS);
    if (ext && *ext) {
      ext_mime_map[ext] = mime;
    }
  } while (ext);
}


void parse_mime_map(const char *buf)
{
  const char *start = buf, *end = buf;
  while (*end) {
    while (*end && *end != '\n') {
      end++;
    }
    parse_mime_map_line(start, end);
    end++;
    start = end;
  }
}

static int ext_mime_map_init(CephContext *cct, const char *ext_map)
{
  int fd = open(ext_map, O_RDONLY);
  char *buf = NULL;
  int ret;
  if (fd < 0) {
    ret = -errno;
    ldout(cct, 0) << "ext_mime_map_init(): failed to open file=" << ext_map << " ret=" << ret << dendl;
    return ret;
  }

  struct stat st;
  ret = fstat(fd, &st);
  if (ret < 0) {
    ret = -errno;
    ldout(cct, 0) << "ext_mime_map_init(): failed to stat file=" << ext_map << " ret=" << ret << dendl;
    goto done;
  }

  buf = (char *)malloc(st.st_size + 1);
  if (!buf) {
    ret = -ENOMEM;
    ldout(cct, 0) << "ext_mime_map_init(): failed to allocate buf" << dendl;
    goto done;
  }

  ret = safe_read(fd, buf, st.st_size + 1);
  if (ret != st.st_size) {
    // huh? file size has changed?
    ldout(cct, 0) << "ext_mime_map_init(): raced! will retry.." << dendl;
    free(buf);
    close(fd);
    return ext_mime_map_init(cct, ext_map);
  }
  buf[st.st_size] = '\0';

  parse_mime_map(buf);
  ret = 0;
done:
  free(buf);
  close(fd);
  return ret;
}

const char *rgw_find_mime_by_ext(string& ext)
{
  map<string, string>::iterator iter = ext_mime_map.find(ext);
  if (iter == ext_mime_map.end())
    return NULL;

  return iter->second.c_str();
}

int rgw_tools_init(CephContext *cct)
{
  int ret = ext_mime_map_init(cct, cct->_conf->rgw_mime_types_file.c_str());
  if (ret < 0)
    return ret;

  return 0;
}

void rgw_tools_cleanup()
{
  ext_mime_map.clear();
}
