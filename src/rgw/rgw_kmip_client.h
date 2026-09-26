// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <atomic>
#include <functional>

class DoutPrefixProvider;
class RGWKMIPManager;
extern "C" {
#include "kmip.h"
#include "kmip_bio.h"
}

class RGWKMIPTransceiver {
public:
  enum kmip_operation {
    CREATE,
    LOCATE,
    GET,
    GET_ATTRIBUTES,
    GET_ATTRIBUTE_LIST,
    DESTROY,
    ENCRYPT,
    DECRYPT
  };
  CephContext *cct;
  kmip_operation operation;
  char *name = 0;
  char *unique_id = 0;
  // output - must free
  char *out = 0;    // unique_id, several
  struct {    // unique_ids, locate
    char **strings;
    int string_count;
  } outlist[1] = {{0, 0}};
  struct {    // key, get
    unsigned char *data;
    int keylen;
  } outkey[1] = {0, 0};
  // end must free
  int ret;
  int worker_id = -1;  // set by worker that processed this request; -1 if unprocessed
  std::atomic<bool> done{false};
  ceph::mutex lock = ceph::make_mutex("rgw_kmip_req::lock");
  ceph::condition_variable cond;

  int wait(const DoutPrefixProvider* dpp, optional_yield y);
  RGWKMIPTransceiver(CephContext * const cct,
    kmip_operation operation)
  : cct(cct),
    operation(operation),
    ret(-EDOM)
  {}
  virtual ~RGWKMIPTransceiver();

  int send();
  int process(const DoutPrefixProvider* dpp, optional_yield y);
  /**
   * Called by the worker thread with an active KMIP context and BIO.
   * Return value tristate (checked by do_one_entry):
   *   0        — not handled; fall through to the legacy batch-request path
   *   positive — custom handler succeeded (mapped to errno 0 for the caller)
   *   negative — custom handler failed; errno propagated directly
   * The default implementation returns 0 (fall through).
   */
  virtual int execute(KMIP* ctx, BIO* bio) {
    return 0;
  }
};

class RGWKMIPManager {
protected:
  CephContext *cct;
  bool is_started = false;
  RGWKMIPManager(CephContext *cct) : cct(cct) {};
public:
  virtual ~RGWKMIPManager() { };
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual int add_request(RGWKMIPTransceiver*) = 0;

  /**
   * Execute op_fn on a worker thread with a pooled KMIP connection.
   * op_fn(ctx, bio) must return 0 on success or a negative errno on error.
   * Blocks the caller until the worker completes (optional_yield is a
   * hint for a future async implementation).
   */
  int execute_fn(const DoutPrefixProvider* dpp,
                 optional_yield y,
                 std::function<int(KMIP*, BIO*)> op_fn,
                 int* worker_id_out = nullptr);
};

extern RGWKMIPManager *rgw_kmip_manager;

/** Install global KMIP manager and start worker threads (see RGWKMIPManager::start). */
void rgw_kmip_client_init(RGWKMIPManager &);

void rgw_kmip_client_cleanup();
