// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_CLIENT_IO_H
#define CEPH_RGW_CLIENT_IO_H

#include <memory>
#include <stdlib.h>

#include "include/types.h"

#include "rgw_common.h"

class RGWClientIOEngine {
public:
  virtual ~RGWClientIOEngine() {};

  virtual int write_data(const char *buf, int len) = 0;
  virtual int read_data(char *buf, int max) = 0;

  virtual void init_env(CephContext *cct) = 0;
  virtual void flush(RGWClientIO& controller) = 0;
  virtual int send_status(RGWClientIO& controller,
                          const char * const status,
                          const char * const status_name) = 0;
  virtual int send_100_continue(RGWClientIO& controller) = 0;
  virtual int complete_header(RGWClientIO& controller) = 0;
  virtual int complete_request(RGWClientIO& controller) = 0;
  virtual int send_content_length(RGWClientIO& controller, uint64_t len) = 0;
  virtual RGWEnv& get_env() = 0;
};


class RGWClientIOEngineDecorator : public RGWClientIOEngine {
  std::shared_ptr<RGWClientIOEngine> decorated;

public:
  RGWClientIOEngineDecorator(std::shared_ptr<RGWClientIOEngine> impl)
    : decorated(impl) {
  }

  /* A lot of wrappers */
  virtual void init_env(CephContext *cct) override {
    return decorated->init_env(cct);
  }

  virtual int write_data(const char * const buf,
                         const int len) override {
    return decorated->write_data(buf, len);
  }

  virtual int read_data(char * const buf,
                        const int max) override {
    return decorated->read_data(buf, max);
  }

  virtual void flush(RGWClientIO& controller) {
    return decorated->flush(controller);
  }

  virtual int send_status(RGWClientIO& controller,
                          const char * const status,
                          const char * const status_name) override {
    return decorated->send_status(controller, status, status_name);
  }

  virtual int send_100_continue(RGWClientIO& controller) override {
    return decorated->send_100_continue(controller);
  }

  virtual int complete_header(RGWClientIO& controller) override {
    return decorated->complete_header(controller);
  }

  virtual int complete_request(RGWClientIO& controller) override {
    return decorated->complete_request(controller);
  }

  virtual int send_content_length(RGWClientIO& controller,
                                  const uint64_t len) override {
    return decorated->send_content_length(controller, len);
  }

  virtual RGWEnv& get_env() override {
    return decorated->get_env();
  }
};


class RGWClientIOEngineReorderer : public RGWClientIOEngineDecorator {
protected:
  enum class ReorderState {
    RGW_EARLY_HEADERS,  /* Headers sent before calling send_status. */
    RGW_STATUS_SEEN,    /* Status has been seen. */
    RGW_DATA            /* Header has been completed. */
  } phase;

  bufferlist early_header_data;
  bufferlist header_data;

public:
  RGWClientIOEngineReorderer(std::shared_ptr<RGWClientIOEngine> engine)
    : RGWClientIOEngineDecorator(engine),
      phase(ReorderState::RGW_EARLY_HEADERS) {
  }

  virtual int write_data(const char *buf, const int len) override;
  virtual int send_status(RGWClientIO& controller,
                         const char * const status,
                         const char * const status_name) override;
  virtual int send_100_continue(RGWClientIO& controller) override;
  virtual int complete_header(RGWClientIO& controller) override;
};


class RGWClientIOEngineBufferAware : public RGWClientIOEngineDecorator {
protected:
  bufferlist data;

  bool has_content_length;
  bool buffer_data;

  virtual int write_data(const char *buf, const int len) override;

public:
  RGWClientIOEngineBufferAware(std::shared_ptr<RGWClientIOEngine> engine)
    : RGWClientIOEngineDecorator(engine),
      has_content_length(false),
      buffer_data(false) {
  }

  int send_content_length(RGWClientIO& controller,
                          const uint64_t len) override;
  int complete_request(RGWClientIO& controller) override;
  int complete_header(RGWClientIO& controller) override;
};


class RGWClientIOEnginePrefixer : public RGWClientIOEngineDecorator {
protected:
  const std::string prefix;
  RGWEnv env;

public:
  RGWClientIOEnginePrefixer(std::shared_ptr<RGWClientIOEngine> engine,
                            std::string const pfx)
    : RGWClientIOEngineDecorator(engine),
      prefix(pfx) {
  }

  virtual RGWEnv& get_env() override;
};


class RGWClientIO {
  bool account;

  size_t bytes_sent;
  size_t bytes_received;

protected:
  const std::shared_ptr<RGWClientIOEngine> engine;

  RGWClientIO(const std::shared_ptr<RGWClientIOEngine> engine)
    : account(false),
      bytes_sent(0),
      bytes_received(0),
      engine(engine) {
  }

public:
  class Builder;

  virtual ~RGWClientIO() {
  }

  void init(CephContext *cct);
  int print(const char *format, ...);
  int write(const char *buf, int len);
  int read(char *buf, int max, int *actual);

  RGWEnv& get_env() {
    return engine->get_env();
  }

  void set_account(bool _account) {
    account = _account;
  }

  uint64_t get_bytes_sent() const {
    return bytes_sent;
  }

  uint64_t get_bytes_received() const {
    return bytes_received;
  }

  /* Public interface parts which must be implemented for concrete
   * frontend provider. */
  virtual void flush() {
    engine->flush(*this);
  }

  virtual int send_status(const char * const status,
                          const char * const status_name) {
    return engine->send_status(*this, status, status_name);
  }

  virtual int send_100_continue() {
    return engine->send_100_continue(*this);
  }

  virtual int complete_header() {
    return engine->complete_header(*this);
  }

  virtual int complete_request() {
    return engine->complete_request(*this);
  }

  virtual int send_content_length(const uint64_t len) {
    return engine->send_content_length(*this, len);
  }
};


class RGWClientIO::Builder {
protected:
  /* Whether engine is resistant to sending some headers first and then
   * setting HTTP status or not and we need to reorder operations. */
  bool needs_reordering = false;

  /* Prepend each requested URL with this prefix. */
  std::string prefix;

  /* Last stage in pipeline. */
  std::shared_ptr<RGWClientIOEngine> final_engine;

public:
  Builder(std::shared_ptr<RGWClientIOEngine> engine)
    : final_engine(engine) {
  }

  bool set_reordering(const bool enabled) {
    const auto prev = needs_reordering;
    needs_reordering = enabled;
    return prev;
  }

  string use_prefix(const string s) {
    const auto prev = prefix;
    prefix = s;
    return prev;
  }

  RGWClientIO getResult() {
    std::shared_ptr<RGWClientIOEngine> stage = final_engine;

    if (prefix.length()) {
      stage = std::make_shared<RGWClientIOEnginePrefixer>(stage, prefix);
    }

    if (needs_reordering) {
      stage = std::make_shared<RGWClientIOEngineReorderer>(stage);
    }

    stage = std::make_shared<RGWClientIOEngineBufferAware>(stage);

    return RGWClientIO(stage);
  }
};
#endif
