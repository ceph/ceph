// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "rgw_sync_error_repo.h"
#include "rgw_coroutine.h"
#include "rgw_sal.h"
#include "cls/cmpomap/client.h"

namespace rgw::error_repo {

// prefix for the binary encoding of keys. this particular value is not
// valid as the first byte of a utf8 code point, so we use this to
// differentiate the binary encoding from existing string keys for
// backward-compatibility
constexpr uint8_t binary_key_prefix = 0x80;

struct key_type {
  rgw_bucket_shard bs;
  std::optional<uint64_t> gen;
};

void encode(const key_type& k, bufferlist& bl, uint64_t f=0)
{
  ENCODE_START(1, 1, bl);
  encode(k.bs, bl);
  encode(k.gen, bl);
  ENCODE_FINISH(bl);
}

void decode(key_type& k, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(k.bs, bl);
  decode(k.gen, bl);
  DECODE_FINISH(bl);
}

std::string encode_key(const rgw_bucket_shard& bs,
                       std::optional<uint64_t> gen)
{
  using ceph::encode;
  const auto key = key_type{bs, gen};
  bufferlist bl;
  encode(binary_key_prefix, bl);
  encode(key, bl);
  return bl.to_str();
}

int decode_key(std::string encoded,
               rgw_bucket_shard& bs,
               std::optional<uint64_t>& gen)
{
  using ceph::decode;
  key_type key;
  const auto bl = bufferlist::static_from_string(encoded);
  auto p = bl.cbegin();
  try {
    uint8_t prefix;
    decode(prefix, p);
    if (prefix != binary_key_prefix) {
      return -EINVAL;
    }
    decode(key, p);
  } catch (const buffer::error&) {
    return -EIO;
  }
  if (!p.end()) {
    return -EIO; // buffer contained unexpected bytes
  }
  bs = std::move(key.bs);
  gen = key.gen;
  return 0;
}

ceph::real_time decode_value(const bufferlist& bl)
{
  uint64_t value;
  try {
    using ceph::decode;
    decode(value, bl);
  } catch (const buffer::error&) {
    value = 0; // empty buffer = 0
  }
  return ceph::real_clock::zero() + ceph::timespan(value);
}

int write(librados::ObjectWriteOperation& op,
          const std::string& key,
          ceph::real_time timestamp)
{
  // overwrite the existing timestamp if value is greater
  const uint64_t value = timestamp.time_since_epoch().count();
  using namespace ::cls::cmpomap;
  const bufferlist zero = u64_buffer(0); // compare against 0 for missing keys
  return cmp_set_vals(op, Mode::U64, Op::GT, {{key, u64_buffer(value)}}, zero);
}

int remove(librados::ObjectWriteOperation& op,
           const std::string& key,
           ceph::real_time timestamp)
{
  // remove the omap key if value >= existing
  const uint64_t value = timestamp.time_since_epoch().count();
  using namespace ::cls::cmpomap;
  return cmp_rm_keys(op, Mode::U64, Op::GTE, {{key, u64_buffer(value)}});
}

class RGWErrorRepoWriteCR : public RGWSimpleCoroutine {
  librados::Rados* rados;
  rgw_raw_obj raw_obj;
  std::string key;
  ceph::real_time timestamp;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
 public:
  RGWErrorRepoWriteCR(librados::Rados* rados, const rgw_raw_obj& raw_obj,
                      const std::string& key, ceph::real_time timestamp)
    : RGWSimpleCoroutine(static_cast<CephContext*>(rados->cct())),
      rados(rados), raw_obj(raw_obj),
      key(key), timestamp(timestamp)
  {}

  int send_request(const DoutPrefixProvider *dpp) override {
    librados::ObjectWriteOperation op;
    int r = write(op, key, timestamp);
    if (r < 0) {
      return r;
    }
    rgw_rados_ref ref;
    r = rgw_get_rados_ref(dpp, rados, raw_obj, &ref);
    if (r < 0) {
      return r;
    }

    cn = stack->create_completion_notifier();
    return ref.aio_operate(cn->completion(), &op);
  }

  int request_complete() override {
    return cn->completion()->get_return_value();
  }
};

RGWCoroutine* write_cr(librados::Rados* rados,
                       const rgw_raw_obj& obj,
                       const std::string& key,
                       ceph::real_time timestamp)
{
  return new RGWErrorRepoWriteCR(rados, obj, key, timestamp);
}


class RGWErrorRepoRemoveCR : public RGWSimpleCoroutine {
  librados::Rados* rados;
  rgw_raw_obj raw_obj;
  std::string key;
  ceph::real_time timestamp;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
 public:
  RGWErrorRepoRemoveCR(librados::Rados* rados, const rgw_raw_obj& raw_obj,
                       const std::string& key, ceph::real_time timestamp)
    : RGWSimpleCoroutine(static_cast<CephContext*>(rados->cct())),
      rados(rados), raw_obj(raw_obj),
      key(key), timestamp(timestamp)
  {}

  int send_request(const DoutPrefixProvider *dpp) override {
    librados::ObjectWriteOperation op;
    int r = remove(op, key, timestamp);
    if (r < 0) {
      return r;
    }
    rgw_rados_ref ref;
    r = rgw_get_rados_ref(dpp, rados, raw_obj, &ref);
    if (r < 0) {
      return r;
    }

    cn = stack->create_completion_notifier();
    return ref.aio_operate(cn->completion(), &op);
  }

  int request_complete() override {
    return cn->completion()->get_return_value();
  }
};

RGWCoroutine* remove_cr(librados::Rados* rados,
                        const rgw_raw_obj& obj,
                        const std::string& key,
                        ceph::real_time timestamp)
{
  return new RGWErrorRepoRemoveCR(rados, obj, key, timestamp);
}

} // namespace rgw::error_repo
