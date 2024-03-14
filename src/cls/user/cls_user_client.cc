// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "cls/user/cls_user_client.h"
#include "include/rados/librados.hpp"

using std::list;
using std::string;

using ceph::bufferlist;
using ceph::real_clock;

using librados::IoCtx;
using librados::ObjectOperationCompletion;
using librados::ObjectReadOperation;

void cls_user_set_buckets(librados::ObjectWriteOperation& op, list<cls_user_bucket_entry>& entries, bool add)
{
  bufferlist in;
  cls_user_set_buckets_op call;
  call.entries = entries;
  call.add = add;
  call.time = real_clock::now();
  encode(call, in);
  op.exec("user", "set_buckets_info", in);
}

void cls_user_complete_stats_sync(librados::ObjectWriteOperation& op)
{
  bufferlist in;
  cls_user_complete_stats_sync_op call;
  call.time = real_clock::now();
  encode(call, in);
  op.exec("user", "complete_stats_sync", in);
}

void cls_user_remove_bucket(librados::ObjectWriteOperation& op, const cls_user_bucket& bucket)
{
  bufferlist in;
  cls_user_remove_bucket_op call;
  call.bucket = bucket;
  encode(call, in);
  op.exec("user", "remove_bucket", in);
}

class ClsUserListCtx : public ObjectOperationCompletion {
  list<cls_user_bucket_entry> *entries;
  string *marker;
  bool *truncated;
  int *pret;
public:
  ClsUserListCtx(list<cls_user_bucket_entry> *_entries, string *_marker, bool *_truncated, int *_pret) :
                                      entries(_entries), marker(_marker), truncated(_truncated), pret(_pret) {}
  void handle_completion(int r, bufferlist& outbl) override {
    if (r >= 0) {
      cls_user_list_buckets_ret ret;
      try {
        auto iter = outbl.cbegin();
        decode(ret, iter);
        if (entries)
	  *entries = ret.entries;
        if (truncated)
          *truncated = ret.truncated;
        if (marker)
          *marker = ret.marker;
      } catch (ceph::buffer::error& err) {
        r = -EIO;
      }
    }
    if (pret) {
      *pret = r;
    }
  }
};

void cls_user_bucket_list(librados::ObjectReadOperation& op,
                          const string& in_marker,
                          const string& end_marker,
                          int max_entries,
                          list<cls_user_bucket_entry>& entries,
                          string *out_marker,
                          bool *truncated,
                          int *pret)
{
  bufferlist inbl;
  cls_user_list_buckets_op call;
  call.marker = in_marker;
  call.end_marker = end_marker;
  call.max_entries = max_entries;

  encode(call, inbl);

  op.exec("user", "list_buckets", inbl, new ClsUserListCtx(&entries, out_marker, truncated, pret));
}

class ClsUserGetHeaderCtx : public ObjectOperationCompletion {
  cls_user_header *header;
  RGWGetUserHeader_CB *ret_ctx;
  int *pret;
public:
  ClsUserGetHeaderCtx(cls_user_header *_h, RGWGetUserHeader_CB *_ctx, int *_pret) : header(_h), ret_ctx(_ctx), pret(_pret) {}
  ~ClsUserGetHeaderCtx() override {
    if (ret_ctx) {
      ret_ctx->put();
    }
  }
  void handle_completion(int r, bufferlist& outbl) override {
    if (r >= 0) {
      cls_user_get_header_ret ret;
      try {
        auto iter = outbl.cbegin();
        decode(ret, iter);
        if (header)
	  *header = ret.header;
      } catch (ceph::buffer::error& err) {
        r = -EIO;
      }
      if (ret_ctx) {
        ret_ctx->handle_response(r, ret.header);
      }
    }
    if (pret) {
      *pret = r;
    }
  }
};

void cls_user_get_header(librados::ObjectReadOperation& op,
                       cls_user_header *header, int *pret)
{
  bufferlist inbl;
  cls_user_get_header_op call;

  encode(call, inbl);

  op.exec("user", "get_header", inbl, new ClsUserGetHeaderCtx(header, NULL, pret));
}

void cls_user_reset_stats(librados::ObjectWriteOperation &op)
{
  bufferlist inbl;
  cls_user_reset_stats_op call;
  call.time = real_clock::now();
  encode(call, inbl);
  op.exec("user", "reset_user_stats", inbl);
}

int cls_user_get_header_async(IoCtx& io_ctx, string& oid, RGWGetUserHeader_CB *ctx)
{
  bufferlist in, out;
  cls_user_get_header_op call;
  encode(call, in);
  ObjectReadOperation op;
  op.exec("user", "get_header", in, new ClsUserGetHeaderCtx(NULL, ctx, NULL)); /* no need to pass pret, as we'll call ctx->handle_response() with correct error */
  auto c = librados::Rados::aio_create_completion(nullptr, nullptr);
  int r = io_ctx.aio_operate(oid, c, &op, NULL);
  c->release();
  if (r < 0)
    return r;

  return 0;
}


void cls_user_account_resource_add(librados::ObjectWriteOperation& op,
                                   const cls_user_account_resource& entry,
                                   bool exclusive, uint32_t limit)
{
  cls_user_account_resource_add_op call;
  call.entry = entry;
  call.exclusive = exclusive;
  call.limit = limit;

  bufferlist inbl;
  encode(call, inbl);
  op.exec("user", "account_resource_add", inbl);
}

class ResourceGetCB : public librados::ObjectOperationCompletion {
  cls_user_account_resource* entry;
  int* pret;
public:
  ResourceGetCB(cls_user_account_resource* entry, int* pret)
    : entry(entry), pret(pret)
  {}
  void handle_completion(int r, bufferlist& outbl) override {
    if (r >= 0) {
      cls_user_account_resource_get_ret ret;
      try {
        auto iter = outbl.cbegin();
        decode(ret, iter);
        if (entry) {
          *entry = std::move(ret.entry);
        }
      } catch (const ceph::buffer::error& err) {
        r = -EIO;
      }
    }
    if (pret) {
      *pret = r;
    }
  }
};

void cls_user_account_resource_get(librados::ObjectReadOperation& op,
                                   std::string_view name,
                                   cls_user_account_resource& entry,
                                   int* pret)
{
  cls_user_account_resource_get_op call;
  call.name = name;

  bufferlist inbl;
  encode(call, inbl);
  op.exec("user", "account_resource_get", inbl,
          new ResourceGetCB(&entry, pret));
}

void cls_user_account_resource_rm(librados::ObjectWriteOperation& op,
                                  std::string_view name)
{
  cls_user_account_resource_rm_op call;
  call.name = name;

  bufferlist inbl;
  encode(call, inbl);
  op.exec("user", "account_resource_rm", inbl);
}

class ResourceListCB : public librados::ObjectOperationCompletion {
  std::vector<cls_user_account_resource>* entries;
  bool* truncated;
  std::string* next_marker;
  int* pret;
public:
  ResourceListCB(std::vector<cls_user_account_resource>* entries,
                 bool* truncated, std::string* next_marker, int* pret)
    : entries(entries), truncated(truncated),
      next_marker(next_marker), pret(pret)
  {}
  void handle_completion(int r, bufferlist& outbl) override {
    if (r >= 0) {
      cls_user_account_resource_list_ret ret;
      try {
        auto iter = outbl.cbegin();
        decode(ret, iter);
        if (entries) {
          *entries = std::move(ret.entries);
        }
        if (next_marker) {
          *next_marker = std::move(ret.marker);
        }
        if (truncated) {
          *truncated = ret.truncated;
        }
      } catch (const ceph::buffer::error& err) {
        r = -EIO;
      }
    }
    if (pret) {
      *pret = r;
    }
  }
};

void cls_user_account_resource_list(librados::ObjectReadOperation& op,
                                    std::string_view marker,
                                    std::string_view path_prefix,
                                    uint32_t max_entries,
                                    std::vector<cls_user_account_resource>& entries,
                                    bool* truncated, std::string* next_marker,
                                    int* pret)
{
  cls_user_account_resource_list_op call;
  call.marker = marker;
  call.path_prefix = path_prefix;
  call.max_entries = max_entries;

  bufferlist inbl;
  encode(call, inbl);
  op.exec("user", "account_resource_list", inbl,
          new ResourceListCB(&entries, truncated, next_marker, pret));
}
