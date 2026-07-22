// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_cuobj.h"

#include <cuobjserver.h>

#include <algorithm>
#include <cstdlib>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rgw

std::unique_ptr<RGWCuObjServer> RGWCuObjServer::s_instance;
thread_local uint16_t RGWCuObjServer::tls_channel_id = 0;
thread_local bool RGWCuObjServer::tls_channel_valid = false;

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
    lderr(cct) << "rgw_cuobj: ERROR: rgw_cuobj_rdma_ip not configured" << dendl;
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
  cuObjServer::setTelemFlags(CUOBJ_LOG_PATH_INFO | CUOBJ_LOG_PATH_DEBUG | CUOBJ_LOG_PATH_ERROR);

  if (!m_server->isConnected()) {
    lderr(cct) << "rgw_cuobj: ERROR: cuObjServer failed to connect" << dendl;
    m_server.reset();
    return -ECONNREFUSED;
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
  return 0;
}

void RGWCuObjServer::do_shutdown()
{
  ldout(m_cct, 1) << "rgw_cuobj: shutting down cuObjServer" << dendl;
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
  m_server.reset();
}

bool RGWCuObjServer::is_available() const
{
  return m_server && m_server->isConnected();
}

// descriptor format: "addr:size:rkey:reserved:qp_num:lid:gid"
// all fields hex-encoded, colon-separated
size_t RGWCuObjServer::parse_rdma_descriptor_size(const std::string& rdma_descr)
{
  ldout(g_ceph_context, 21) << "rgw_cuobj: parsing RDMA descriptor: " << rdma_descr << dendl;
  auto first_colon = rdma_descr.find(':');
  if (first_colon == std::string::npos) {
    lderr(g_ceph_context) << "rgw_cuobj: ERROR: failed to parse RDMA descriptor: no colon found" << dendl;
    return 0;
  }
  auto second_colon = rdma_descr.find(':', first_colon + 1);
  if (second_colon == std::string::npos) {
    lderr(g_ceph_context) << "rgw_cuobj: ERROR: failed to parse RDMA descriptor: second colon not found" << dendl;
    return 0;
  }
  auto size_str = rdma_descr.substr(first_colon + 1, second_colon - first_colon - 1);
  ldout(g_ceph_context, 21) << "rgw_cuobj: parsed size from RDMA descriptor: " << size_str << dendl;
  return std::stoull(size_str, nullptr, 16);
}

static uint64_t parse_rdma_descriptor_addr(const std::string& rdma_descr)
{
  ldout(g_ceph_context, 21) << "rgw_cuobj: parsing RDMA descriptor for address: " << rdma_descr << dendl;
  auto first_colon = rdma_descr.find(':');
  if (first_colon == std::string::npos) {
    lderr(g_ceph_context) << "rgw_cuobj: ERROR: failed to parse RDMA descriptor for address: no colon found" << dendl;
    return 0;
  }
  ldout(g_ceph_context, 21) << "rgw_cuobj: parsed address from RDMA descriptor: " << rdma_descr.substr(0, first_colon) << dendl;
  return std::stoull(rdma_descr.substr(0, first_colon), nullptr, 16);
}

uint16_t RGWCuObjServer::get_channel_id()
{
  if (!tls_channel_valid) {
    tls_channel_id = m_server->allocateChannelId();
    tls_channel_valid = true;
  }
  ldout(m_cct, 21) << "rgw_cuobj: allocated channel ID " << tls_channel_id << dendl;
  return tls_channel_id;
}

void RGWCuObjServer::release_channel_id()
{
  if (tls_channel_valid) {
    ldout(m_cct, 21) << "rgw_cuobj: releasing channel ID " << tls_channel_id << dendl;
    m_server->freeChannelId(tls_channel_id);
    tls_channel_valid = false;
  }
}

RGWCuObjServer::RDMABufEntry* RGWCuObjServer::acquire_buffer(size_t needed_size)
{
  ldout(m_cct, 21) << "rgw_cuobj: acquiring RDMA buffer for size " << needed_size << dendl;
  for (size_t i = 0; i < m_buf_count; i++) {
    auto& entry = m_buffer_pool[i];
    if (entry.size >= needed_size) {
      bool expected = false;
      if (entry.in_use.compare_exchange_strong(expected, true,
                                               std::memory_order_acquire)) {
        return &entry;
      }
    }
  }
  lderr(m_cct) << "rgw_cuobj: ERROR: no available RDMA buffer for size " << needed_size << dendl;
  return nullptr;
}

void RGWCuObjServer::release_buffer(RDMABufEntry* buf)
{
  ldout(m_cct, 21) << "rgw_cuobj: releasing RDMA buffer" << dendl;
  if (buf) {
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
  uint16_t channel = get_channel_id();
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
  uint16_t channel = get_channel_id();
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
