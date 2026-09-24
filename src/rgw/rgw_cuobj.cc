// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_cuobj.h"

#include <cuobjserver.h>
#ifdef WITH_RADOSGW_CUOBJ_TARGET
#include <cuobjclient.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstdlib>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/cufile_config.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/pick_address.h"
#include "common/rdma_token.h"
#include "include/scope_guard.h"

#define dout_subsys ceph_subsys_rgw

std::unique_ptr<RGWCuObjServer> RGWCuObjServer::s_instance;

static constexpr size_t MAX_RDMA_OP_SIZE = 1ULL << 30; // 1 GiB per cuObj API

RGWCuObjServer::~RGWCuObjServer()
{
  do_shutdown();
}

int RGWCuObjServer::init(CephContext* cct)
{
  if (s_instance) {
    return 0;
  }
  s_instance.reset(new RGWCuObjServer());
  int r = s_instance->do_init(cct);
  if (r < 0) {
    s_instance.reset();
  }
  return r;
}

void RGWCuObjServer::shutdown()
{
  s_instance.reset();
}

RGWCuObjServer* RGWCuObjServer::get_instance()
{
  return s_instance.get();
}

int RGWCuObjServer::do_init(CephContext* cct)
{
  m_cct = cct;

  auto rdma_ip = cct->_conf.get_val<std::string>("rgw_cuobj_rdma_ip");
  if (rdma_ip.empty()) {
    rdma_ip = pick_rdma_addr(cct);
  }
  if (rdma_ip.empty()) {
    lderr(cct) << "rgw_cuobj: ERROR: neither rgw_cuobj_rdma_ip nor a local "
               << "address in rdma_network is configured" << dendl;
    return -EINVAL;
  }

  auto rdma_port = static_cast<unsigned short>(cct->_conf.get_val<uint64_t>("rgw_cuobj_rdma_port"));
  auto num_dcis = static_cast<int>(cct->_conf.get_val<uint64_t>("rgw_cuobj_num_dcis"));
  auto buf_size = static_cast<size_t>(cct->_conf.get_val<Option::size_t>("rgw_cuobj_buffer_size"));
  auto buf_count = static_cast<size_t>(cct->_conf.get_val<uint64_t>("rgw_cuobj_buffer_count"));

  cuObjRDMATunable params;
  params.setNumDcis(num_dcis);

  ldout(cct, 1) << "rgw_cuobj: initializing cuObjServer on "
                << rdma_ip << ":" << rdma_port
                << " dcis=" << num_dcis
                << " bufs=" << buf_count << "x" << buf_size << dendl;

  try {
    m_server = std::make_unique<cuObjServer>(rdma_ip.c_str(), rdma_port, CUOBJ_PROTO_RDMA_DC_V1, params);
  } catch (const std::exception& e) {
    lderr(cct) << "rgw_cuobj: ERROR: cuObjServer construction failed: " << e.what() << dendl;
    return -EIO;
  }

  cuObjServer::setupTelemetry(false, &std::cerr);
#ifdef CUOBJ_SERVER_MAJOR_VERSION
  // cuObject 2.x split op logging into a second flags argument
  cuObjServer::setTelemFlags(CUOBJ_LOG_PATH_INFO | CUOBJ_LOG_PATH_DEBUG | CUOBJ_LOG_PATH_ERROR,
                             CUOBJ_LOG_OP_GET | CUOBJ_LOG_OP_PUT);
#else
  cuObjServer::setTelemFlags(CUOBJ_LOG_PATH_INFO | CUOBJ_LOG_PATH_DEBUG | CUOBJ_LOG_PATH_ERROR);
#endif

  if (!m_server->isConnected()) {
    lderr(cct) << "rgw_cuobj: ERROR: cuObjServer failed to connect" << dendl;
    m_server.reset();
    return -ECONNREFUSED;
  }

  for (int i = 0; i < num_dcis; i++) {
    uint16_t id = m_server->allocateChannelId();
    if (id == invalid_channel) {
      break;
    }
    m_free_channels.push_back(id);
  }
  if (m_free_channels.empty()) {
    lderr(cct) << "rgw_cuobj: ERROR: cuObject channel allocation failed" << dendl;
    do_shutdown();
    return -EIO;
  }
  if (m_free_channels.size() < static_cast<size_t>(num_dcis)) {
    lderr(cct) << "rgw_cuobj: WARNING: only " << m_free_channels.size()
               << " of " << num_dcis << " channels allocated" << dendl;
  }

  m_buf_count = buf_count;
  m_buffer_pool = std::make_unique<RDMABufEntry[]>(buf_count);
  for (size_t i = 0; i < buf_count; i++) {
    auto& entry = m_buffer_pool[i];
    entry.ptr = m_server->allocHostBuffer(buf_size);
    if (!entry.ptr) {
      lderr(cct) << "rgw_cuobj: ERROR: allocHostBuffer failed for buffer " << i << dendl;
      do_shutdown();
      return -ENOMEM;
    }
    entry.size = buf_size;
    entry.handle = m_server->registerBuffer(entry.ptr, buf_size);
    if (!entry.handle) {
      lderr(cct) << "rgw_cuobj: ERROR: registerBuffer failed for buffer " << i << dendl;
      free(entry.ptr);
      entry.ptr = nullptr;
      do_shutdown();
      return -EIO;
    }
    entry.in_use.store(false, std::memory_order_relaxed);
  }

  ldout(cct, 1) << "rgw_cuobj: initialized with " << buf_count
                << " RDMA buffers of " << buf_size << " bytes" << dendl;

  if (cct->_conf.get_val<bool>("rgw_cuobj_osd_push")) {
    // a failure here only loses the push-target role, not RDMA service
    int r = init_push_target(cct, rdma_ip);
    if (r < 0) {
      lderr(cct) << "rgw_cuobj: WARNING: OSDs cannot push into this gateway "
                 << "(" << cpp_strerror(r) << "); staged GETs stay on the "
                 << "messenger" << dendl;
    }
  }
  return 0;
}

namespace {
#ifdef WITH_RADOSGW_CUOBJ_TARGET
// the token flow never uses the library's own get/put callbacks
CUObjIOOps empty_ops{};
#endif

int64_t now_ns()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}
} // namespace

int RGWCuObjServer::init_push_target(CephContext* cct,
                                     const std::string& rdma_ip)
{
#ifndef WITH_RADOSGW_CUOBJ_TARGET
  lderr(cct) << "rgw_cuobj: rgw_cuobj_osd_push set but this radosgw was "
             << "built without the cuObject client library" << dendl;
  return -EOPNOTSUPP;
#else
  // the client library reads its NIC selection from the cufile json
  ceph::rdma::setup_cufile_json(
    cct, cct->_conf.get_val<std::string>("rgw_cuobj_client_config"), rdma_ip);
  try {
    m_client = std::make_unique<cuObjClient>(empty_ops, CUOBJ_PROTO_RDMA_DC_V1);
  } catch (const std::exception& e) {
    lderr(cct) << "rgw_cuobj: cuObjClient init failed: " << e.what() << dendl;
    return -EIO;
  }
  if (!m_client->isConnected()) {
    lderr(cct) << "rgw_cuobj: cuObjClient did not connect to the RDMA fabric"
               << dendl;
    m_client.reset();
    return -EIO;
  }
  // the same memory the local cuObjServer pushes out of is now also a
  // window peers can push into: a second registration and one token
  // per buffer, both held for the life of the pool
  for (size_t i = 0; i < m_buf_count; i++) {
    auto& entry = m_buffer_pool[i];
    if (m_client->cuMemObjGetDescriptor(entry.ptr, entry.size) != CU_OBJ_SUCCESS) {
      lderr(cct) << "rgw_cuobj: client registration of buffer " << i
                 << " failed" << dendl;
      return -EIO;
    }
    if (m_client->cuMemObjGetRDMAToken(entry.ptr, entry.size, 0, CUOBJ_GET,
                                       &entry.token_raw) != CU_OBJ_SUCCESS ||
        !entry.token_raw) {
      entry.token_raw = nullptr;
      m_client->cuMemObjPutDescriptor(entry.ptr);
      lderr(cct) << "rgw_cuobj: token mint for buffer " << i << " failed"
                 << dendl;
      return -EIO;
    }
    entry.token = entry.token_raw;
  }
  m_push_target = true;
  ldout(cct, 1) << "rgw_cuobj: " << m_buf_count << " buffers registered as "
                << "OSD push targets" << dendl;
  return 0;
#endif
}

void RGWCuObjServer::do_shutdown()
{
  ldout(m_cct, 1) << "rgw_cuobj: shutting down cuObjServer" << dendl;
  m_push_target = false;
#ifdef WITH_RADOSGW_CUOBJ_TARGET
  // the library's lifetime rules: tokens before registrations before
  // the client, all before the memory goes
  if (m_client) {
    for (size_t i = 0; i < m_buf_count; i++) {
      auto& entry = m_buffer_pool[i];
      if (entry.token_raw) {
        m_client->cuMemObjPutRDMAToken(entry.token_raw);
        m_client->cuMemObjPutDescriptor(entry.ptr);
        entry.token_raw = nullptr;
        entry.token.clear();
      }
    }
    m_client.reset();
  }
#endif
  for (size_t i = 0; i < m_buf_count; i++) {
    auto& entry = m_buffer_pool[i];
    if (entry.handle && m_server) {
      m_server->deRegisterBuffer(entry.handle);
      entry.handle = nullptr;
    }
    if (entry.ptr) {
      free(entry.ptr);
      entry.ptr = nullptr;
    }
  }
  m_buffer_pool.reset();
  m_buf_count = 0;
  if (m_server) {
    for (auto id : m_free_channels) {
      m_server->freeChannelId(id);
    }
  }
  m_free_channels.clear();
  m_server.reset();
}

bool RGWCuObjServer::is_available() const
{
  return m_server && m_server->isConnected();
}

const char* RGWCuObjServer::zero_buffer()
{
  static const char* zeros = static_cast<const char*>(calloc(1, ZERO_BUFFER_LEN));
  return zeros;
}

// descriptor token: leading "addr:size:" hex fields, remainder opaque;
// see common/rdma_token.h
size_t RGWCuObjServer::parse_rdma_descriptor_size(const std::string& rdma_descr)
{
  auto window = ceph::rdma::parse_rdma_token(rdma_descr);
  if (!window) {
    lderr(g_ceph_context) << "rgw_cuobj: ERROR: failed to parse RDMA descriptor" << dendl;
    return 0;
  }
  ldout(g_ceph_context, 21) << "rgw_cuobj: parsed size from RDMA descriptor: "
                            << window->size << dendl;
  return window->size;
}

static uint64_t parse_rdma_descriptor_addr(const std::string& rdma_descr)
{
  auto window = ceph::rdma::parse_rdma_token(rdma_descr);
  if (!window) {
    lderr(g_ceph_context) << "rgw_cuobj: ERROR: failed to parse RDMA descriptor for address" << dendl;
    return 0;
  }
  ldout(g_ceph_context, 21) << "rgw_cuobj: parsed address from RDMA descriptor: "
                            << window->addr << dendl;
  return window->addr;
}

uint16_t RGWCuObjServer::acquire_channel()
{
  std::unique_lock l{m_channel_lock};
  m_channel_cond.wait(l, [this] { return !m_free_channels.empty(); });
  uint16_t channel = m_free_channels.back();
  m_free_channels.pop_back();
  return channel;
}

void RGWCuObjServer::release_channel(uint16_t channel)
{
  {
    std::lock_guard l{m_channel_lock};
    m_free_channels.push_back(channel);
  }
  m_channel_cond.notify_one();
}

RGWCuObjServer::RDMABufEntry* RGWCuObjServer::acquire_buffer(size_t needed_size)
{
  ldout(m_cct, 21) << "rgw_cuobj: acquiring RDMA buffer for size " << needed_size << dendl;
  const int64_t now = now_ns();
  for (size_t i = 0; i < m_buf_count; i++) {
    auto& entry = m_buffer_pool[i];
    if (entry.size >= needed_size) {
      bool expected = false;
      if (entry.in_use.compare_exchange_strong(expected, true,
                                               std::memory_order_acquire)) {
        if (entry.quarantine_until_ns.load(std::memory_order_relaxed) > now) {
          entry.in_use.store(false, std::memory_order_release);
          continue;
        }
        return &entry;
      }
    }
  }
  lderr(m_cct) << "rgw_cuobj: ERROR: no available RDMA buffer for size " << needed_size << dendl;
  return nullptr;
}

void RGWCuObjServer::release_buffer(RDMABufEntry* buf, uint64_t quarantine_ms)
{
  ldout(m_cct, 21) << "rgw_cuobj: releasing RDMA buffer"
                   << (quarantine_ms ? " (quarantined)" : "") << dendl;
  if (buf) {
    if (quarantine_ms) {
      buf->quarantine_until_ns.store(now_ns() + int64_t(quarantine_ms) * 1000000,
                                     std::memory_order_relaxed);
    }
    buf->in_use.store(false, std::memory_order_release);
  }
}

ssize_t RGWCuObjServer::rdma_read_from_client(
    const std::string& key,
    RDMABufEntry* buf,
    uint64_t remote_offset,
    size_t size,
    const std::string& rdma_descr)
{
  uint16_t channel = acquire_channel();
  auto put_channel = make_scope_guard([this, channel] {
    release_channel(channel);
  });
  uint64_t client_addr = parse_rdma_descriptor_addr(rdma_descr) + remote_offset;
  size_t total_read = 0;

  ldout(m_cct, 21) << "rgw_cuobj: handlePutObject"
                   << " key=" << key
                   << " size=" << size
                   << " client_addr=" << client_addr
                   << " channel=" << channel
                   << " buf_ptr=" << buf->ptr
                   << " buf_size=" << buf->size
                   << " descr=" << rdma_descr
                   << dendl;

  while (total_read < size) {
    size_t chunk = std::min(size - total_read, MAX_RDMA_OP_SIZE);
    ibv_wc_status wc_status = IBV_WC_SUCCESS;
    ssize_t ret = m_server->handlePutObject(
        key, buf->handle, client_addr + total_read,
        chunk, rdma_descr, channel, total_read, &wc_status);
    if (ret < 0) {
      lderr(m_cct) << "rgw_cuobj: ERROR: handlePutObject failed:"
                   << " ret=" << ret
                   << " wc_status=" << static_cast<int>(wc_status)
                   << " chunk=" << chunk
                   << " local_offset=" << total_read
                   << " remote_addr=" << client_addr + total_read
                   << dendl;
      return ret;
    }
    total_read += ret;
    if (static_cast<size_t>(ret) < chunk) {
      break;
    }
  }

  ldout(m_cct, 20) << "rgw_cuobj: RDMA read " << total_read << " bytes" << " for key=" << key << dendl;
  return static_cast<ssize_t>(total_read);
}

ssize_t RGWCuObjServer::rdma_write_to_client(
    const std::string& key,
    RDMABufEntry* buf,
    uint64_t remote_offset,
    size_t size,
    const std::string& rdma_descr)
{
  uint16_t channel = acquire_channel();
  auto put_channel = make_scope_guard([this, channel] {
    release_channel(channel);
  });
  uint64_t client_addr = parse_rdma_descriptor_addr(rdma_descr) + remote_offset;
  size_t total_written = 0;

  ldout(m_cct, 21) << "rgw_cuobj: handleGetObject"
                   << " key=" << key
                   << " size=" << size
                   << " client_addr=" << client_addr
                   << " channel=" << channel
                   << " buf_ptr=" << buf->ptr
                   << " buf_size=" << buf->size
                   << " descr=" << rdma_descr
                   << dendl;
  while (total_written < size) {
    size_t chunk = std::min(size - total_written, MAX_RDMA_OP_SIZE);
    ssize_t ret = m_server->handleGetObject(
        key, buf->handle, client_addr + total_written,
        chunk, rdma_descr, channel, total_written);
    if (ret < 0) {
      lderr(m_cct) << "rgw_cuobj: ERROR: handleGetObject failed:"
                   << " ret=" << ret
                   << " chunk=" << chunk
                   << " local_offset=" << total_written
                   << " remote_addr=" << client_addr + total_written
                   << dendl;
      return ret;
    }
    total_written += ret;
    if (static_cast<size_t>(ret) < chunk) {
      break;
    }
  }

  ldout(m_cct, 20) << "rgw_cuobj: RDMA wrote " << total_written << " bytes" << " for key=" << key << dendl;
  return static_cast<ssize_t>(total_written);
}
