// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "rgw_file.h"

extern RGWLib librgw;


bool is_root(const string& uri)
{
  return uri.equals("");
}

bool is_bucket(const string& uri)
{
  int pos = req.find('/');
  return (pos < 0);
}

/*
  get generate nfs_handle
*/
int rgw_get_handle(const char* uri, struct nfs_handle* handle)
{
  handle->handle = librgw.get_handle(uri);
  return 0;
}

/*
  check nfs_handle
*/
int rgw_check_handle(const struct nfs_handle* handle)
{
  return librgw.check_handle(handle);
}

int rgw_mount(const char* uid, const char* key, const char* secret,
	      const struct nfs_handle* handle)
{
  int rc;
  string uri = uid + "\";
  string auth_hdr;
  map<string, string> meta_map;
  map<string, string> sub_resources;

  rgw_create_s3_canonical_header('GET',
                                 NULL, /* const char *content_md5 */
                                 'text/html',
                                 "",
                                 meta_map,
                                 urti.c_str(),
                                 sub_resources,
                                 auth_hdr);

  /* check key */
  rc = rgw_get_s3_header_digest(auth_hdr, key, secret);
  if (rc < 0 ) {
    return rc;
  }

  return rgw_get_handle(uri);
}

/*
  create a new dirctory
*/
int rgw_create_directory(const struct nfs_handle* parent_handle,
const char* name)
{
  return 0;
}

/*
  create a new file
*/
int rgw_create_file(const struct nfs_handle* parent_handle, const char* name)
{
  return 0;
}

int rgw_rename(const struct nfs_handle* parent_handle, const char* old_name,
const char* new_name)
{
  return 0;
}
/*
  remove file or directory
*/
int rgw_unlink(const struct nfs_handle* parent_handle, const char* path)
{
  return 0;
}

/*
  lookup a directory or file
*/
int rgw_lookup(const struct nfs_handle* parent_handle, const struct nfs_handle*
               parent_handle, const char* path, uint64_t* handle)
{
  string uri;
  int rc;

  rc = get_uri(parent_handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  if (is_root(uri)) {
    librgw.get_bucket(uri);
  } else if (is_bucket(uri)) {
    /* get the object */
  } else { /* parent cannot be an object */
    return -1;
  }

  uri += "/" + path;
  /* find or create an handle for the object or bucket */
  *handle =  librgw.get_new_handle(uri);
  return 0;
}

/*
  read directory content
*/
int rgw_readdir(const struct nfs_handle* parent_handle, const char* path)
{
  string uri;
  int rc;

  rc = get_uri(parent_handle, uri);
  if (rc < 0 ) { /* invalid parent */
    return rc;
  }

  if (librgw.is_root(uri)) {
    /* get the bucket list */
    return librgw.get_user_buckets_list();
  } else if (librgw.is_bucket(uri)) {
    /* get the object list */
    return librgw.get_buckets_object_list();
  } else { /* parent cannot be an object */
    return -1;
  }

  return 0;
}
