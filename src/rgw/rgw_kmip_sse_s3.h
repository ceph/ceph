// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_common.h"
#include "rgw_kmip_client.h"
#include "include/buffer.h"

/**
 * KMIP Backend for SSE-S3 Encryption
 *
 * Contract (RGW-facing, independent of transport threading):
 * - Bucket-level KEK stored in KMIP; id returned to RGW for attrs / config.
 * - Per-object DEK generated in RGW; wrap/unwrap via KMIP Encrypt/Decrypt.
 *
 * Implements KEK/DEK pattern:
 * - Bucket-level KEK stored in KMIP server
 * - Per-object DEK generated in RGW
 * - DEK wrapped/unwrapped using KMIP Encrypt/Decrypt operations
 */
class RGWKmipSSES3 {
private:
  CephContext* cct;

public:
  RGWKmipSSES3(CephContext* cct);
  ~RGWKmipSSES3() = default;

  int initialize();

  int create_bucket_key(const DoutPrefixProvider* dpp,
                        const std::string& bucket_name,
                        std::string& kek_id_out,
                        optional_yield y);

  int destroy_bucket_key(const DoutPrefixProvider* dpp,
                         const std::string& kek_id,
                         optional_yield y,
                         int* worker_id_out = nullptr);

  int generate_and_wrap_dek(const DoutPrefixProvider* dpp,
                            const std::string& kek_id,
                            const std::string& encryption_context,
                            bufferlist& plaintext_dek_out,
                            bufferlist& wrapped_dek_out,
                            optional_yield y);

  int unwrap_dek(const DoutPrefixProvider* dpp,
                 const std::string& kek_id,
                 const bufferlist& wrapped_dek,
                 const std::string& encryption_context,
                 bufferlist& plaintext_dek_out,
                 optional_yield y);
};

RGWKmipSSES3* get_kmip_sse_s3_backend(CephContext* cct);

void cleanup_kmip_sse_s3_backend();