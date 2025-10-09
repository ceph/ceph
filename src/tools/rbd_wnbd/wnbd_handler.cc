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

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd

#include "wnbd_handler.h"

#define _NTSCSI_USER_MODE_
#include <rpc.h>
#include <ddk/scsi.h>

#include <boost/thread/tss.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/SubProcess.h"
#include "common/Formatter.h"

#include "global/global_context.h"

WnbdHandler::~WnbdHandler()
{
  if (started && wnbd_disk) {
    dout(10) << __func__ << ": terminating" << dendl;

    shutdown();
    reply_tpool->join();

    WnbdClose(wnbd_disk);

    started = false;

    delete reply_tpool;
    delete admin_hook;
  }
}

int WnbdHandler::wait()
{
  int err = 0;
  if (started && wnbd_disk) {
    dout(10) << "waiting for WNBD mapping: " << instance_name << dendl;

    err = WnbdWaitDispatcher(wnbd_disk);
    if (err) {
      derr << __func__ << ": failed waiting for dispatcher to stop: "
           << instance_name
           << ". Error: " << err << dendl;
    } else {
      dout(10) << "WNBD mapping disconnected: " << instance_name << dendl;
    }
  }

  return err;
}

WnbdAdminHook::WnbdAdminHook(WnbdHandler *handler, AdminSocket* admin_socket)
  : m_handler(handler)
  , m_admin_socket(admin_socket)
{
  if (m_admin_socket) {
    m_admin_socket->register_command(
      std::string("wnbd stats ") + m_handler->instance_name,
      this, "get WNBD stats");
  } else {
    dout(0) << "no admin socket provided, skipped registering wnbd hooks"
            << dendl;
  }
}

int WnbdAdminHook::call (
  std::string_view command, const cmdmap_t& cmdmap,
  const bufferlist&,
  Formatter *f,
  std::ostream& errss,
  bufferlist& out)
{
  if (command == "wnbd stats " + m_handler->instance_name) {
    return m_handler->dump_stats(f);
  }
  return -ENOSYS;
}

int WnbdHandler::dump_stats(Formatter *f)
{
  if (!f) {
    return -EINVAL;
  }

  WNBD_USR_STATS stats = { 0 };
  DWORD err = WnbdGetUserspaceStats(wnbd_disk, &stats);
  if (err) {
    derr << "Failed to retrieve WNBD userspace stats. Error: " << err << dendl;
    return -EINVAL;
  }

  f->open_object_section("stats");
  f->dump_int("TotalReceivedRequests", stats.TotalReceivedRequests);
  f->dump_int("TotalSubmittedRequests", stats.TotalSubmittedRequests);
  f->dump_int("TotalReceivedReplies", stats.TotalReceivedReplies);
  f->dump_int("UnsubmittedRequests", stats.UnsubmittedRequests);
  f->dump_int("PendingSubmittedRequests", stats.PendingSubmittedRequests);
  f->dump_int("PendingReplies", stats.PendingReplies);
  f->dump_int("ReadErrors", stats.ReadErrors);
  f->dump_int("WriteErrors", stats.WriteErrors);
  f->dump_int("FlushErrors", stats.FlushErrors);
  f->dump_int("UnmapErrors", stats.UnmapErrors);
  f->dump_int("InvalidRequests", stats.InvalidRequests);
  f->dump_int("TotalRWRequests", stats.TotalRWRequests);
  f->dump_int("TotalReadBlocks", stats.TotalReadBlocks);
  f->dump_int("TotalWrittenBlocks", stats.TotalWrittenBlocks);

  f->close_section();
  return 0;
}

void WnbdHandler::shutdown()
{
  std::unique_lock l{shutdown_lock};
  if (!terminated && wnbd_disk) {
    // We're requesting the disk to be removed but continue serving IO
    // requests until the driver sends us the "Disconnect" event.
    // TODO: expose PWNBD_REMOVE_OPTIONS, we're using the defaults ATM.
    WnbdRemove(wnbd_disk, NULL);
    wait();
    terminated = true;
  }
}

void WnbdHandler::aio_callback(librbd::completion_t cb, void *arg)
{
  librbd::RBD::AioCompletion *aio_completion =
    reinterpret_cast<librbd::RBD::AioCompletion*>(cb);

  WnbdHandler::IOContext* ctx = static_cast<WnbdHandler::IOContext*>(arg);
  int ret = aio_completion->get_return_value();

  dout(20) << __func__ << ": " << *ctx << dendl;

  if (ret == -EINVAL) {
    // if shrinking an image, a pagecache writeback might reference
    // extents outside of the range of the new image extents
    dout(0) << __func__ << ": masking IO out-of-bounds error" << *ctx << dendl;
    ctx->data.clear();
    ret = 0;
  }

  if (ret < 0) {
    ctx->err_code = -ret;
    // TODO: check the actual error.
    ctx->set_sense(SCSI_SENSE_MEDIUM_ERROR,
                   SCSI_ADSENSE_UNRECOVERED_ERROR);
  } else if ((ctx->req_type == WnbdReqTypeRead) &&
              ret < static_cast<int>(ctx->req_size)) {
    int pad_byte_count = static_cast<int> (ctx->req_size) - ret;
    ctx->data.append_zero(pad_byte_count);
    dout(20) << __func__ << ": " << *ctx << ": Pad byte count: "
             << pad_byte_count << dendl;
    ctx->err_code = 0;
  } else {
    ctx->err_code = 0;
  }

  boost::asio::post(
    *ctx->handler->reply_tpool,
    [&, ctx]()
    {
      ctx->handler->send_io_response(ctx);
    });

  aio_completion->release();
}

void WnbdHandler::send_io_response(WnbdHandler::IOContext *ctx) {
  std::unique_ptr<WnbdHandler::IOContext> pctx{ctx};
  ceph_assert(WNBD_DEFAULT_MAX_TRANSFER_LENGTH >= pctx->data.length());

  WNBD_IO_RESPONSE wnbd_rsp = {0};
  wnbd_rsp.RequestHandle = pctx->req_handle;
  wnbd_rsp.RequestType = pctx->req_type;
  wnbd_rsp.Status = pctx->wnbd_status;
  int err = 0;

  // Use TLS to store an overlapped structure so that we avoid
  // recreating one each time we send a reply.
  static boost::thread_specific_ptr<OVERLAPPED> overlapped_tls(
    // Cleanup routine
    [](LPOVERLAPPED p_overlapped)
    {
      if (p_overlapped->hEvent) {
        CloseHandle(p_overlapped->hEvent);
      }
      delete p_overlapped;
    });

  LPOVERLAPPED overlapped = overlapped_tls.get();
  if (!overlapped)
  {
    overlapped = new OVERLAPPED{0};
    HANDLE overlapped_evt = CreateEventA(0, TRUE, TRUE, NULL);
    if (!overlapped_evt) {
      err = GetLastError();
      derr << "Could not create event. Error: " << err << dendl;
      return;
    }

    overlapped->hEvent = overlapped_evt;
    overlapped_tls.reset(overlapped);
  }

  if (!ResetEvent(overlapped->hEvent)) {
    err = GetLastError();
    derr << "Could not reset event. Error: " << err << dendl;
    return;
  }

  err = WnbdSendResponseEx(
    pctx->handler->wnbd_disk,
    &wnbd_rsp,
    pctx->data.c_str(),
    pctx->data.length(),
    overlapped);
  if (err == ERROR_IO_PENDING) {
    DWORD returned_bytes = 0;
    err = 0;
    // We've got ERROR_IO_PENDING, which means that the operation is in
    // progress. We'll use GetOverlappedResult to wait for it to complete
    // and then retrieve the result.
    if (!GetOverlappedResult(pctx->handler->wnbd_disk, overlapped,
                             &returned_bytes, TRUE)) {
      err = GetLastError();
      derr << "Could not send response. Request id: " << wnbd_rsp.RequestHandle
           << ". Error: " << err << dendl;
    }
  }
}

void WnbdHandler::IOContext::set_sense(uint8_t sense_key, uint8_t asc, uint64_t info)
{
  WnbdSetSenseEx(&wnbd_status, sense_key, asc, info);
}

void WnbdHandler::IOContext::set_sense(uint8_t sense_key, uint8_t asc)
{
  WnbdSetSense(&wnbd_status, sense_key, asc);
}

void WnbdHandler::Read(
  PWNBD_DISK Disk,
  UINT64 RequestHandle,
  PVOID Buffer,
  UINT64 BlockAddress,
  UINT32 BlockCount,
  BOOLEAN ForceUnitAccess)
{
  WnbdHandler* handler = nullptr;
  ceph_assert(!WnbdGetUserContext(Disk, (PVOID*)&handler));

  WnbdHandler::IOContext* ctx = new WnbdHandler::IOContext();
  ctx->handler = handler;
  ctx->req_handle = RequestHandle;
  ctx->req_type = WnbdReqTypeRead;
  ctx->req_size = BlockCount * handler->block_size;
  ctx->req_from = BlockAddress * handler->block_size;
  ceph_assert(ctx->req_size <= WNBD_DEFAULT_MAX_TRANSFER_LENGTH);

  int op_flags = 0;
  if (ForceUnitAccess) {
    op_flags |= LIBRADOS_OP_FLAG_FADVISE_FUA;
  }

  dout(20) << *ctx << ": start" << dendl;

  librbd::RBD::AioCompletion *c = new librbd::RBD::AioCompletion(ctx, aio_callback);
  handler->image.aio_read2(ctx->req_from, ctx->req_size, ctx->data, c, op_flags);

  dout(20) << *ctx << ": submitted" << dendl;
}

void WnbdHandler::Write(
  PWNBD_DISK Disk,
  UINT64 RequestHandle,
  PVOID Buffer,
  UINT64 BlockAddress,
  UINT32 BlockCount,
  BOOLEAN ForceUnitAccess)
{
  WnbdHandler* handler = nullptr;
  ceph_assert(!WnbdGetUserContext(Disk, (PVOID*)&handler));

  WnbdHandler::IOContext* ctx = new WnbdHandler::IOContext();
  ctx->handler = handler;
  ctx->req_handle = RequestHandle;
  ctx->req_type = WnbdReqTypeWrite;
  ctx->req_size = BlockCount * handler->block_size;
  ctx->req_from = BlockAddress * handler->block_size;

  bufferptr ptr((char*)Buffer, ctx->req_size);
  ctx->data.push_back(ptr);

  int op_flags = 0;
  if (ForceUnitAccess) {
    op_flags |= LIBRADOS_OP_FLAG_FADVISE_FUA;
  }

  dout(20) << *ctx << ": start" << dendl;

  librbd::RBD::AioCompletion *c = new librbd::RBD::AioCompletion(ctx, aio_callback);
  handler->image.aio_write2(ctx->req_from, ctx->req_size, ctx->data, c, op_flags);

  dout(20) << *ctx << ": submitted" << dendl;
}

void WnbdHandler::Flush(
  PWNBD_DISK Disk,
  UINT64 RequestHandle,
  UINT64 BlockAddress,
  UINT32 BlockCount)
{
  WnbdHandler* handler = nullptr;
  ceph_assert(!WnbdGetUserContext(Disk, (PVOID*)&handler));

  WnbdHandler::IOContext* ctx = new WnbdHandler::IOContext();
  ctx->handler = handler;
  ctx->req_handle = RequestHandle;
  ctx->req_type = WnbdReqTypeFlush;
  ctx->req_size = BlockCount * handler->block_size;
  ctx->req_from = BlockAddress * handler->block_size;

  dout(20) << *ctx << ": start" << dendl;

  librbd::RBD::AioCompletion *c = new librbd::RBD::AioCompletion(ctx, aio_callback);
  handler->image.aio_flush(c);

  dout(20) << *ctx << ": submitted" << dendl;
}

void WnbdHandler::Unmap(
  PWNBD_DISK Disk,
  UINT64 RequestHandle,
  PWNBD_UNMAP_DESCRIPTOR Descriptors,
  UINT32 Count)
{
  WnbdHandler* handler = nullptr;
  ceph_assert(!WnbdGetUserContext(Disk, (PVOID*)&handler));
  ceph_assert(1 == Count);

  WnbdHandler::IOContext* ctx = new WnbdHandler::IOContext();
  ctx->handler = handler;
  ctx->req_handle = RequestHandle;
  ctx->req_type = WnbdReqTypeUnmap;
  ctx->req_size = Descriptors[0].BlockCount * handler->block_size;
  ctx->req_from = Descriptors[0].BlockAddress * handler->block_size;

  dout(20) << *ctx << ": start" << dendl;

  librbd::RBD::AioCompletion *c = new librbd::RBD::AioCompletion(ctx, aio_callback);
  handler->image.aio_discard(ctx->req_from, ctx->req_size, c);

  dout(20) << *ctx << ": submitted" << dendl;
}

void WnbdHandler::LogMessage(
  WnbdLogLevel LogLevel,
  const char* Message,
  const char* FileName,
  UINT32 Line,
  const char* FunctionName)
{
  // We're already passing the log level to WNBD, so we'll use the highest
  // log level here.
  dout(0) << "libwnbd.dll!" << FunctionName << " "
          << WnbdLogLevelToStr(LogLevel) << " " << Message << dendl;
}

int WnbdHandler::resize(uint64_t new_size)
{
  int err = 0;
  
  uint64_t new_block_count = new_size / block_size;

  dout(5) << "Resizing disk. Block size: " << block_size
          << ". New block count: " << new_block_count
          << ". Old block count: "
          << wnbd_disk->Properties.BlockCount << "." << dendl;
  err = WnbdSetDiskSize(wnbd_disk, new_block_count);
  if (err) {
    derr << "WNBD: Setting disk size failed with error: "
         << win32_strerror(err) << dendl;
    return -EINVAL;
  }

  dout(5) << "Successfully resized disk to: " << new_block_count << " blocks"
          << dendl;
  return 0;
}

int WnbdHandler::start()
{
  int err = 0;
  WNBD_PROPERTIES wnbd_props = {0};

  instance_name.copy(wnbd_props.InstanceName, sizeof(wnbd_props.InstanceName));
  ceph_assert(strlen(RBD_WNBD_OWNER_NAME) < WNBD_MAX_OWNER_LENGTH);
  strncpy(wnbd_props.Owner, RBD_WNBD_OWNER_NAME, WNBD_MAX_OWNER_LENGTH);

  wnbd_props.BlockCount = block_count;
  wnbd_props.BlockSize = block_size;
  wnbd_props.MaxUnmapDescCount = 1;

  wnbd_props.Flags.ReadOnly = readonly;
  wnbd_props.Flags.UnmapSupported = 1;
  if (rbd_cache_enabled) {
    wnbd_props.Flags.FUASupported = 1;
    wnbd_props.Flags.FlushSupported = 1;
  }

  err = WnbdCreate(&wnbd_props, (const PWNBD_INTERFACE) &RbdWnbdInterface,
                   this, &wnbd_disk);
  if (err)
    goto exit;

  started = true;

  err = WnbdStartDispatcher(wnbd_disk, io_req_workers);
  if (err) {
      derr << "Could not start WNBD dispatcher. Error: " << err << dendl;
  }

exit:
  return err;
}

std::ostream &operator<<(std::ostream &os, const WnbdHandler::IOContext &ctx) {

  os << "[" << std::hex << ctx.req_handle;

  switch (ctx.req_type)
  {
  case WnbdReqTypeRead:
    os << " READ ";
    break;
  case WnbdReqTypeWrite:
    os << " WRITE ";
    break;
  case WnbdReqTypeFlush:
    os << " FLUSH ";
    break;
  case WnbdReqTypeUnmap:
    os << " TRIM ";
    break;
  default:
    os << " UNKNOWN(" << ctx.req_type << ") ";
    break;
  }

  os << ctx.req_from << "~" << ctx.req_size << " "
     << std::dec << ntohl(ctx.err_code) << "]";

  return os;
}
