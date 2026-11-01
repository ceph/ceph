// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_cuobj.h"

#include <cuobjserver.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <optional>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout_fmt.h"
#include "common/oob_arena.h"
#include "common/oob_config.h"
#include "common/rdma_token.h"

std::atomic<RGWCuObjServer*> RGWCuObjServer::s_instance{nullptr};
thread_local uint16_t RGWCuObjServer::tls_channel_id = 0;
thread_local bool RGWCuObjServer::tls_channel_valid = false;
thread_local bool RGWCuObjServer::tls_channel_refused = false;

static constexpr size_t MAX_RDMA_OP_SIZE = 1ULL << 30; // 1 GiB per cuObj API
// DC initiators, at the least
static constexpr int MIN_DCIS = 128;

// Path flags select message severity. The second setTelemFlags() argument
// selects which ops emit telemetry spans; leave that at 0.
static unsigned cuobj_telem_flags(const std::string& level)
{
  if (level == "debug") {
    return CUOBJ_LOG_PATH_ERROR | CUOBJ_LOG_PATH_INFO | CUOBJ_LOG_PATH_DEBUG;
  }
  if (level == "info") {
    return CUOBJ_LOG_PATH_ERROR | CUOBJ_LOG_PATH_INFO;
  }
  if (level == "off") {
    return 0;
  }
  return CUOBJ_LOG_PATH_ERROR;
}

RGWCuObjServer::~RGWCuObjServer()
{
  do_shutdown();
}

int RGWCuObjServer::init(CephContext* cct, ceph::oob::Arena* arena)
{
  if (s_instance.load()) {
    return 0;
  }
  // the frontends take requests already: request threads see the server
  // only once it is complete, and never one whose setup failed
  std::unique_ptr<RGWCuObjServer> srv{new RGWCuObjServer()};
  srv->m_arena = arena;
  if (int r = srv->do_init(cct); r < 0) {
    return r;
  }
  s_instance.store(srv.release(), std::memory_order_release);
  return 0;
}

void RGWCuObjServer::shutdown()
{
  delete s_instance.exchange(nullptr);
}

RGWCuObjServer* RGWCuObjServer::get_instance()
{
  return s_instance.load(std::memory_order_acquire);
}

int RGWCuObjServer::do_init(CephContext* cct)
{
  m_cct = cct;

  ceph::oob::location_t loc;
  std::string err;
  if (int r = ceph::oob::resolve(cct, ceph::oob::public_ip(cct), &loc, &err); r < 0) {
    ldpp_dout_fmt(this, -1, "ERROR: {}", err);
    return r;
  }
  if (loc.addr.empty()) {
    ldpp_dout_fmt(this, -1, "ERROR: no out-of-band address: set oob_network");
    return -EINVAL;
  }
  const std::string rdma_ip = loc.addr;

  // the library chooses its rdma_cm port; no client connects to it
  const unsigned short rdma_port = 0;
  // a DC initiator for each frontend thread: each thread keeps the
  // channel that it takes on its first transfer, and beast moves a
  // request from thread to thread as it waits, so a request that staged
  // on one thread can write to the client from any other
  m_num_dcis = std::max<int>(MIN_DCIS, cct->_conf->rgw_thread_pool_size);

  uint64_t dc_key = 0;
  if (int r = ceph::oob::dc_key(cct, &dc_key, &err); r < 0) {
    ldpp_dout_fmt(this, -1, "ERROR: {}", err);
    return r;
  }

  cuObjRDMATunable params;
  params.setNumDcis(m_num_dcis);
  params.setDcKey(dc_key);

  ldpp_dout_fmt(this, 1,
                "initializing cuObjServer on {}:{} dcis={}, staging in {} bytes",
                rdma_ip, rdma_port, m_num_dcis, m_arena->size());

  // the library logs to stderr, at rgw_cuobj_log_level. Set up before
  // the server, so that its construction logs under these flags as well.
  // A gateway that daemonized has already closed stderr, so only one run
  // with -f keeps these lines
  auto log_level = cct->_conf.get_val<std::string>("rgw_cuobj_log_level");
  cuObjServer::setupTelemetry(false, &std::cerr);
  // setTelemFlags() second arg selects which ops emit telemetry: 0 (none),
  // avaliable options: CUOBJ_LOG_OP_GET, CUOBJ_LOG_OP_PUT, or both OR'ed together
  cuObjServer::setTelemFlags(cuobj_telem_flags(log_level), 0);
  ldpp_dout_fmt(this, 1, "telemetry log level={}", log_level);

  try {
    m_server = std::make_unique<cuObjServer>(rdma_ip.c_str(), rdma_port, CUOBJ_PROTO_RDMA_DC_V1, params);
  } catch (const std::exception& e) {
    ldpp_dout_fmt(this, -1, "ERROR: cuObjServer construction failed: {}", e.what());
    return -EIO;
  }

  if (!m_server->isConnected()) {
    ldpp_dout_fmt(this, -1, "ERROR: cuObjServer failed to connect");
    m_server.reset();
    return -ECONNREFUSED;
  }

  // the whole arena, once: each transfer stages in a buffer of it, which
  // the library addresses by its offset
  m_arena_handle = m_server->registerBuffer(m_arena->base(), m_arena->size());
  if (!m_arena_handle) {
    ldpp_dout_fmt(this, -1,
                  "ERROR: registerBuffer failed for {} bytes of out-of-band memory",
                  m_arena->size());
    do_shutdown();
    return -EIO;
  }
  ldpp_dout_fmt(this, 1, "staging in {} bytes of out-of-band memory",
                m_arena->size());
  return 0;
}

void RGWCuObjServer::do_shutdown()
{
  ldpp_dout_fmt(this, 1, "shutting down cuObjServer");
  // the arena, which holds the memory, goes after the server
  if (m_arena_handle && m_server) {
    m_server->deRegisterBuffer(m_arena_handle);
  }
  m_arena_handle = nullptr;
  m_server.reset();
}

bool RGWCuObjServer::is_available()
{
  // a frontend thread that the library has no channel for serves its
  // requests over HTTP: asked before the first transfer on the thread
  // too, so that a PUT without a body is declined rather than failed
  return m_server && m_server->isConnected() &&
    get_channel_id() != invalid_channel;
}

// descriptor token: leading "addr:size:" hex fields, remainder opaque;
// see common/rdma_token.h
size_t RGWCuObjServer::parse_rdma_descriptor_size(const std::string& rdma_descr)
{
  const DoutPrefix dpp(g_ceph_context, ceph_subsys_rgw, "rgw_cuobj: ");
  ldpp_dout_fmt(&dpp, 21, "parsing RDMA descriptor: {}", rdma_descr);
  auto window = ceph::rdma::parse_rdma_token(rdma_descr);
  if (!window) {
    ldpp_dout_fmt(&dpp, -1, "ERROR: failed to parse RDMA descriptor");
    return 0;
  }
  ldpp_dout_fmt(&dpp, 21, "parsed size from RDMA descriptor: {}", window->size);
  return window->size;
}

static std::optional<uint64_t> parse_rdma_descriptor_addr(const std::string& rdma_descr)
{
  const DoutPrefix dpp(g_ceph_context, ceph_subsys_rgw, "rgw_cuobj: ");
  ldpp_dout_fmt(&dpp, 21, "parsing RDMA descriptor for address: {}", rdma_descr);
  auto window = ceph::rdma::parse_rdma_token(rdma_descr);
  if (!window) {
    ldpp_dout_fmt(&dpp, -1,
                  "ERROR: failed to parse RDMA descriptor for address");
    return std::nullopt;
  }
  ldpp_dout_fmt(&dpp, 21, "parsed address from RDMA descriptor: {}",
                window->addr);
  return window->addr;
}

uint16_t RGWCuObjServer::get_channel_id()
{
  if (!tls_channel_valid) {
    const uint16_t id = m_server->allocateChannelId();
    if (id == invalid_channel) {
      if (!tls_channel_refused) {
        tls_channel_refused = true;
        ldpp_dout_fmt(this, -1,
                      "ERROR: cuObject channel allocation failed ({} DC "
                      "initiators, one for each frontend thread); this thread "
                      "serves its requests over HTTP", m_num_dcis);
      }
      return invalid_channel;
    }
    tls_channel_id = id;
    tls_channel_valid = true;
    ldpp_dout_fmt(this, 21, "allocated channel ID {}", id);
  }
  return tls_channel_id;
}

void RGWCuObjServer::release_channel_id()
{
  if (tls_channel_valid) {
    ldpp_dout_fmt(this, 21, "releasing channel ID {}", tls_channel_id);
    m_server->freeChannelId(tls_channel_id);
    tls_channel_valid = false;
  }
}

RGWCuObjServer::RDMABufEntry* RGWCuObjServer::acquire_buffer(size_t needed_size)
{
  ldpp_dout_fmt(this, 21, "acquiring RDMA buffer for size {}", needed_size);
  // a buffer only for a thread with a channel to move it: a GET takes
  // its buffer before its headers go out, and without one it is served
  // over HTTP
  if (get_channel_id() == invalid_channel) {
    return nullptr;
  }
  // a buffer of the arena at the transfer's size; a larger transfer than
  // Arena::MAX, or one that finds the arena full, gets none
  const auto slice = m_arena->alloc(needed_size);
  if (!slice) {
    ldpp_dout_fmt(this, 4, "no out-of-band buffer of {} bytes free",
                  needed_size);
    return nullptr;
  }
  auto entry = new RDMABufEntry;
  entry->ptr = m_arena->ptr(slice);
  entry->size = slice.len;
  entry->offset = slice.offset;
  entry->handle = m_arena_handle;
  return entry;
}

void RGWCuObjServer::release_buffer(RDMABufEntry* buf)
{
  ldpp_dout_fmt(this, 21, "releasing RDMA buffer");
  if (buf) {
    m_arena->free(buf->offset, buf->size);
    delete buf;
  }
}

ssize_t RGWCuObjServer::rdma_read_from_client(
    const std::string& key,
    RDMABufEntry* buf,
    uint64_t remote_offset,
    size_t size,
    const std::string& rdma_descr)
{
  auto addr = parse_rdma_descriptor_addr(rdma_descr);
  if (!addr) {
    return -EINVAL;
  }
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    // acquire_buffer() found one, on another thread perhaps
    return -EIO;
  }
  uint64_t client_addr = *addr + remote_offset;
  size_t total_read = 0;

  ldpp_dout_fmt(this, 21,
                "handlePutObject key={} size={} client_addr={} channel={} "
                "buf_ptr={} buf_size={} descr={}",
                key, size, client_addr, channel, buf->ptr, buf->size, rdma_descr);

  while (total_read < size) {
    size_t chunk = std::min(size - total_read, MAX_RDMA_OP_SIZE);
    ibv_wc_status wc_status = IBV_WC_SUCCESS;
    ssize_t ret = m_server->handlePutObject(
        key, buf->handle, client_addr + total_read,
        chunk, rdma_descr, channel, buf->offset + total_read, &wc_status);
    if (ret < 0) {
      ldpp_dout_fmt(this, -1,
                    "ERROR: handlePutObject failed: ret={} wc_status={} chunk={} "
                    "local_offset={} remote_addr={}",
                    ret, static_cast<int>(wc_status), chunk, total_read,
                    client_addr + total_read);
      return ret;
    }
    total_read += ret;
    if (static_cast<size_t>(ret) < chunk) {
      break;
    }
  }

  ldpp_dout_fmt(this, 20, "RDMA read {} bytes for key={}", total_read, key);
  return static_cast<ssize_t>(total_read);
}

ssize_t RGWCuObjServer::rdma_write_to_client(
    const std::string& key,
    RDMABufEntry* buf,
    uint64_t remote_offset,
    size_t size,
    const std::string& rdma_descr)
{
  auto addr = parse_rdma_descriptor_addr(rdma_descr);
  if (!addr) {
    return -EINVAL;
  }
  uint16_t channel = get_channel_id();
  if (channel == invalid_channel) {
    // acquire_buffer() found one, on another thread perhaps
    return -EIO;
  }
  uint64_t client_addr = *addr + remote_offset;
  size_t total_written = 0;

  ldpp_dout_fmt(this, 21,
                "handleGetObject key={} size={} client_addr={} channel={} "
                "buf_ptr={} buf_size={} descr={}",
                key, size, client_addr, channel, buf->ptr, buf->size, rdma_descr);
  while (total_written < size) {
    size_t chunk = std::min(size - total_written, MAX_RDMA_OP_SIZE);
    ssize_t ret = m_server->handleGetObject(
        key, buf->handle, client_addr + total_written,
        chunk, rdma_descr, channel, buf->offset + total_written);
    if (ret < 0) {
      ldpp_dout_fmt(this, -1,
                    "ERROR: handleGetObject failed: ret={} chunk={} "
                    "local_offset={} remote_addr={}",
                    ret, chunk, total_written, client_addr + total_written);
      return ret;
    }
    total_written += ret;
    if (static_cast<size_t>(ret) < chunk) {
      break;
    }
  }

  ldpp_dout_fmt(this, 20, "RDMA wrote {} bytes for key={}", total_written, key);
  return static_cast<ssize_t>(total_written);
}
