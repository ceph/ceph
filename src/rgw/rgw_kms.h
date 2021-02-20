// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/**
 * Server-side encryption integrations with Key Management Systems (SSE-KMS)
 */

#ifndef CEPH_RGW_KMS_H
#define CEPH_RGW_KMS_H

static const std::string RGW_SSE_KMS_BACKEND_TESTING = "testing";
static const std::string RGW_SSE_KMS_BACKEND_BARBICAN = "barbican";
static const std::string RGW_SSE_KMS_BACKEND_VAULT = "vault";

static const std::string RGW_SSE_KMS_VAULT_AUTH_TOKEN = "token";
static const std::string RGW_SSE_KMS_VAULT_AUTH_AGENT = "agent";

static const std::string RGW_SSE_KMS_VAULT_SE_TRANSIT = "transit";
static const std::string RGW_SSE_KMS_VAULT_SE_KV = "kv";

/**
 * Retrieves the actual server-side encryption key from a KMS system given a
 * key ID. Currently supported KMS systems are OpenStack Barbican and HashiCorp
 * Vault, but keys can also be retrieved from Ceph configuration file (if
 * kms is set to 'local').
 *
 * \params
 * TODO
 * \return
 */
int get_actual_key_from_kms(CephContext *cct,
                            std::string_view key_id,
                            std::string_view key_selector,
                            std::string& actual_key);

/**
 * SecretEngine Interface
 * Defining interface here such that we can use both a real implementation
 * of this interface, and a mock implementation in tests.
**/
class SecretEngine {

public:
  virtual int get_key(std::string_view key_id, std::string& actual_key) = 0;
  virtual ~SecretEngine(){};
protected:
  virtual int send_request(std::string_view key_id, JSONParser* parser) = 0;
  virtual int decode_secret(JSONObj* json_obj, std::string& actual_key) = 0;
};
#endif
