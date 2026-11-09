// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <errno.h>
#include <utility>

#include "cls/user/cls_user_client.h"
#include "rgw/datum/adapters/cls/user.h"
#include "include/rados/librados.hpp"

using std::list;
using std::string;

using ceph::bufferlist;
using ceph::real_clock;

using librados::IoCtx;
using librados::ObjectOperationCompletion;
using librados::ObjectReadOperation;
using namespace cls::user;

void cls_user_set_buckets(librados::ObjectWriteOperation& op, list<cls_user_bucket_entry>& entries, bool add)
{
  auto in = cls::rgw::user_datum::encode_bulk(entries, add, real_clock::now());
  op.exec(method::set_buckets_info, in);
}

void cls_user_complete_stats_sync(librados::ObjectWriteOperation& op)
{
  auto in = cls::rgw::user_datum::encode_time(real_clock::now());
  op.exec(method::complete_stats_sync, in);
}

void cls_user_remove_bucket(librados::ObjectWriteOperation& op, const cls_user_bucket& bucket)
{
  auto in = cls::rgw::user_datum::encode_bucket(bucket);
  op.exec(method::remove_bucket, in);
}

namespace {

template <typename PublishT>
class reply_completion final : public ObjectOperationCompletion
{
  int *status;
  [[no_unique_address]] PublishT publish;

public:
  reply_completion(int *status, PublishT publish)
    : status(status), publish(std::move(publish))
  {}

public:
  void handle_completion(int result, bufferlist& bytes) override
  {
    if (result >= 0) {
      try
       {
        publish(bytes);
       }
      catch (const ceph::buffer::error&)
       {
        result = -EIO;
       }
    }
    if (status) {
      *status = result;
    }
  }

};

} // anonymous namespace

void cls_user_bucket_list(librados::ObjectReadOperation& op,
                          const string& in_marker,
                          const string& end_marker,
                          int max_entries,
                          list<cls_user_bucket_entry>& entries,
                          string *out_marker,
                          bool *truncated,
                          int *pret)
{
  auto inbl = cls::rgw::user_datum::encode_bucket_list(in_marker, end_marker, max_entries);

  op.exec(method::list_buckets, inbl, new reply_completion {pret,
    [&entries, out_marker, truncated](bufferlist& bytes) {
      auto reply = cls::rgw::user_datum::decode_bucket_list_reply(bytes);
      entries = std::move(reply.entries);
      if (truncated) {
        *truncated = reply.truncated;
      }
      if (out_marker) {
        *out_marker = std::move(reply.marker);
      }
    }});
}

class ClsUserGetHeaderCtx final : public ObjectOperationCompletion
{
  cls_user_header *header;
  RGWGetUserHeader_CB *ret_ctx;
  int *pret;

public:
  ClsUserGetHeaderCtx(cls_user_header *header, RGWGetUserHeader_CB *context, int *status)
    : header(header), ret_ctx(context), pret(status)
  {}

  ~ClsUserGetHeaderCtx() override
  {
    if (ret_ctx) {
      ret_ctx->put();
    }
  }

public:
  void handle_completion(int r, bufferlist& outbl) override
  {
    if (r >= 0) {
      cls_user_header reply;
      try
       {
        cls::rgw::user_datum::decode_header_reply(outbl, reply);
        if (header) {
          *header = reply;
        }
       }
      catch (const ceph::buffer::error&)
       {
        r = -EIO;
       }
      if (ret_ctx) {
        ret_ctx->handle_response(r, reply);
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
  auto inbl = cls::rgw::user_datum::encode_get_header();

  op.exec(method::get_header, inbl, new ClsUserGetHeaderCtx(header, NULL, pret));
}

void cls_user_reset_stats(librados::ObjectWriteOperation &op)
{
  auto inbl = cls::rgw::user_datum::encode_time(real_clock::now());
  op.exec(method::reset_user_stats, inbl);
}

int cls_user_get_header_async(IoCtx& io_ctx, string& oid, RGWGetUserHeader_CB *ctx)
{
  auto in = cls::rgw::user_datum::encode_get_header();
  ObjectReadOperation op;
  op.exec(method::get_header, in, new ClsUserGetHeaderCtx(NULL, ctx, NULL)); /* no need to pass pret, as we'll call ctx->handle_response() with correct error */
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
  auto inbl = cls::rgw::user_datum::encode_resource_add(entry, exclusive, limit);
  op.exec(method::account_resource_add, inbl);
}

void cls_user_account_resource_get(librados::ObjectReadOperation& op,
                                   std::string_view name,
                                   cls_user_account_resource& entry,
                                   int* pret)
{
  auto inbl = cls::rgw::user_datum::encode_name(name);
  op.exec(method::account_resource_get, inbl, new reply_completion {pret,
    [&entry](bufferlist& bytes) {
      entry = cls::rgw::user_datum::decode_resource_reply(bytes);
    }});
}

void cls_user_account_resource_rm(librados::ObjectWriteOperation& op,
                                  std::string_view name)
{
  auto inbl = cls::rgw::user_datum::encode_name(name);
  op.exec(method::account_resource_rm, inbl);
}

void cls_user_account_resource_list(librados::ObjectReadOperation& op,
                                    std::string_view marker,
                                    std::string_view path_prefix,
                                    uint32_t max_entries,
                                    std::vector<cls_user_account_resource>& entries,
                                    bool* truncated, std::string* next_marker,
                                    int* pret)
{
  auto inbl = cls::rgw::user_datum::encode_resource_list(marker, path_prefix, max_entries);
  op.exec(method::account_resource_list, inbl, new reply_completion {pret,
    [&entries, truncated, next_marker](bufferlist& bytes) {
      auto reply = cls::rgw::user_datum::decode_resource_list_reply(bytes);
      entries = std::move(reply.entries);
      if (next_marker) {
        *next_marker = std::move(reply.marker);
      }
      if (truncated) {
        *truncated = reply.truncated;
      }
    }});
}
