/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef WNBD_HANDLER_H
#define WNBD_HANDLER_H

#include <wnbd.h>

#include "common/admin_socket.h"
#include "common/ceph_context.h"
#include "common/Thread.h"

#include "include/rbd/librbd.hpp"
#include "include/xlist.h"

#include "global/global_context.h"

// TODO: make this configurable.
#define RBD_WNBD_MAX_TRANSFER 2 * 1024 * 1024
#define SOFT_REMOVE_RETRY_INTERVAL 2

// Not defined by mingw.
#ifndef SCSI_ADSENSE_UNRECOVERED_ERROR
#define SCSI_ADSENSE_UNRECOVERED_ERROR 0x11
#endif

// The following will be assigned to the "Owner" field of the WNBD
// parameters, which can be used to determine the application managing
// a disk. We'll ignore other disks.
#define RBD_WNBD_OWNER_NAME "ceph-rbd-wnbd"

class WnbdHandler;

class WnbdAdminHook : public AdminSocketHook {
  WnbdHandler *m_handler;

public:
  explicit WnbdAdminHook(WnbdHandler *handler) :
        m_handler(handler) {
    g_ceph_context->get_admin_socket()->register_command(
      "wnbd stats", this, "get WNBD stats");
  }
  ~WnbdAdminHook() override {
    g_ceph_context->get_admin_socket()->unregister_commands(this);
  }

  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f, std::ostream& errss, bufferlist& out) override;
};


class WnbdHandler
{
private:
  librbd::Image &image;
  std::string instance_name;
  uint64_t block_count;
  uint32_t block_size;
  bool readonly;
  bool rbd_cache_enabled;
  uint32_t io_req_workers;
  uint32_t io_reply_workers;
  WnbdAdminHook* admin_hook;
  boost::asio::thread_pool* reply_tpool;

public:
  WnbdHandler(librbd::Image& _image, std::string _instance_name,
              uint64_t _block_count, uint32_t _block_size,
              bool _readonly, bool _rbd_cache_enabled,
              uint32_t _io_req_workers,
              uint32_t _io_reply_workers)
    : image(_image)
    , instance_name(_instance_name)
    , block_count(_block_count)
    , block_size(_block_size)
    , readonly(_readonly)
    , rbd_cache_enabled(_rbd_cache_enabled)
    , io_req_workers(_io_req_workers)
    , io_reply_workers(_io_reply_workers)
  {
    admin_hook = new WnbdAdminHook(this);
    // Instead of relying on librbd's own thread pool, we're going to use a
    // separate one. This allows us to make assumptions on the threads that
    // are going to send the IO replies and thus be able to cache Windows
    // OVERLAPPED structures.
    reply_tpool = new boost::asio::thread_pool(_io_reply_workers);
  }

  int resize(uint64_t new_size);
  int start();
  // Wait for the handler to stop, which normally happens when the driver
  // passes the "Disconnect" request.
  int wait();
  void shutdown();

  int dump_stats(Formatter *f);

  ~WnbdHandler();

  static VOID LogMessage(
    WnbdLogLevel LogLevel,
    const char* Message,
    const char* FileName,
    UINT32 Line,
    const char* FunctionName);

private:
  ceph::mutex shutdown_lock = ceph::make_mutex("WnbdHandler::DisconnectLocker");
  bool started = false;
  bool terminated = false;
  WNBD_DISK* wnbd_disk = nullptr;

  struct IOContext
  {
    xlist<IOContext*>::item item;
    WnbdHandler *handler = nullptr;
    WNBD_STATUS wnbd_status = {0};
    WnbdRequestType req_type = WnbdReqTypeUnknown;
    uint64_t req_handle = 0;
    uint32_t err_code = 0;
    size_t req_size;
    uint64_t req_from;
    bufferlist data;

    IOContext()
      : item(this)
    {}

    void set_sense(uint8_t sense_key, uint8_t asc, uint64_t info);
    void set_sense(uint8_t sense_key, uint8_t asc);
  };

  friend std::ostream &operator<<(std::ostream &os, const IOContext &ctx);

  void send_io_response(IOContext *ctx);

  static void aio_callback(librbd::completion_t cb, void *arg);

  // WNBD IO entry points
  static void Read(
    PWNBD_DISK Disk,
    UINT64 RequestHandle,
    PVOID Buffer,
    UINT64 BlockAddress,
    UINT32 BlockCount,
    BOOLEAN ForceUnitAccess);
  static void Write(
    PWNBD_DISK Disk,
    UINT64 RequestHandle,
    PVOID Buffer,
    UINT64 BlockAddress,
    UINT32 BlockCount,
    BOOLEAN ForceUnitAccess);
  static void Flush(
    PWNBD_DISK Disk,
    UINT64 RequestHandle,
    UINT64 BlockAddress,
    UINT32 BlockCount);
  static void Unmap(
    PWNBD_DISK Disk,
    UINT64 RequestHandle,
    PWNBD_UNMAP_DESCRIPTOR Descriptors,
    UINT32 Count);

  static constexpr WNBD_INTERFACE RbdWnbdInterface =
  {
    Read,
    Write,
    Flush,
    Unmap,
  };
};

std::ostream &operator<<(std::ostream &os, const WnbdHandler::IOContext &ctx);

#endif // WNBD_HANDLER_H
