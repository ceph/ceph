// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_cuobj.h"

#include <cuobjserver.h>

#include <algorithm>
#include <cstdlib>
#include <optional>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout_fmt.h"
#include "common/strtol.h"
#include "include/random.h"

std::unique_ptr<RGWCuObjServer> RGWCuObjServer::s_instance;
thread_local uint16_t RGWCuObjServer::tls_channel_id = 0;
thread_local bool RGWCuObjServer::tls_channel_valid = false;

static constexpr size_t MAX_RDMA_OP_SIZE = 1ULL << 30; // 1 GiB per cuObj API

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
    ldpp_dout_fmt(this, -1, "ERROR: rgw_cuobj_rdma_ip not configured");
    return -EINVAL;
  }

  auto rdma_port = static_cast<unsigned short>(cct->_conf.get_val<uint64_t>("rgw_cuobj_rdma_port"));
  auto num_dcis = static_cast<int>(cct->_conf.get_val<uint64_t>("rgw_cuobj_num_dcis"));
  auto buf_size = static_cast<size_t>(cct->_conf.get_val<Option::size_t>("rgw_cuobj_buffer_size"));
  auto buf_count = static_cast<size_t>(cct->_conf.get_val<uint64_t>("rgw_cuobj_buffer_count"));

  cuObjRDMATunable params;
  params.setNumDcis(num_dcis);

  ldpp_dout_fmt(this, 1,
                "initializing cuObjServer on {}:{} dcis={} bufs={}x{}",
                rdma_ip, rdma_port, num_dcis, buf_count, buf_size);

  try {
    m_server = std::make_unique<cuObjServer>(rdma_ip.c_str(), rdma_port, CUOBJ_PROTO_RDMA_DC_V1, params);
  } catch (const std::exception& e) {
    ldpp_dout_fmt(this, -1, "ERROR: cuObjServer construction failed: {}", e.what());
    return -EIO;
  }

  auto log_level = cct->_conf.get_val<std::string>("rgw_cuobj_log_level");
  cuObjServer::setupTelemetry(false, &std::cerr);
  // setTelemFlags() second arg selects which ops emit telemetry: 0 (none),
  // avaliable options: CUOBJ_LOG_OP_GET, CUOBJ_LOG_OP_PUT, or both OR'ed together
  cuObjServer::setTelemFlags(cuobj_telem_flags(log_level), 0);
  ldpp_dout_fmt(this, 1, "telemetry log level={}", log_level);

  if (!m_server->isConnected()) {
    ldpp_dout_fmt(this, -1, "ERROR: cuObjServer failed to connect");
    m_server.reset();
    return -ECONNREFUSED;
  }

  m_buf_count = buf_count;
  m_buffer_pool = std::make_unique<RDMABufEntry[]>(buf_count);
  for (size_t i = 0; i < buf_count; i++) {
    auto& entry = m_buffer_pool[i];
    entry.ptr = m_server->allocHostBuffer(buf_size);
    if (!entry.ptr) {
      ldpp_dout_fmt(this, -1, "ERROR: allocHostBuffer failed for buffer {}", i);
      do_shutdown();
      return -ENOMEM;
    }
    entry.size = buf_size;
    entry.handle = m_server->registerBuffer(entry.ptr, buf_size);
    if (!entry.handle) {
      ldpp_dout_fmt(this, -1, "ERROR: registerBuffer failed for buffer {}", i);
      free(entry.ptr);
      entry.ptr = nullptr;
      do_shutdown();
      return -EIO;
    }
    entry.in_use.store(false, std::memory_order_relaxed);
  }

  ldpp_dout_fmt(this, 1, "initialized with {} RDMA buffers of {} bytes",
                buf_count, buf_size);
  return 0;
}

void RGWCuObjServer::do_shutdown()
{
  ldpp_dout_fmt(this, 1, "shutting down cuObjServer");
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
  const DoutPrefix dpp(g_ceph_context, ceph_subsys_rgw, "rgw_cuobj: ");
  ldpp_dout_fmt(&dpp, 21, "parsing RDMA descriptor: {}", rdma_descr);
  auto first_colon = rdma_descr.find(':');
  if (first_colon == std::string::npos) {
    ldpp_dout_fmt(&dpp, -1, "ERROR: failed to parse RDMA descriptor: no colon found");
    return 0;
  }
  auto second_colon = rdma_descr.find(':', first_colon + 1);
  if (second_colon == std::string::npos) {
    ldpp_dout_fmt(&dpp, -1,
                  "ERROR: failed to parse RDMA descriptor: second colon not found");
    return 0;
  }
  auto size_str = rdma_descr.substr(first_colon + 1, second_colon - first_colon - 1);
  ldpp_dout_fmt(&dpp, 21, "parsed size from RDMA descriptor: {}", size_str);
  return ceph::parse<size_t>(size_str, 16).value_or(0);
}

static std::optional<uint64_t> parse_rdma_descriptor_addr(const std::string& rdma_descr)
{
  const DoutPrefix dpp(g_ceph_context, ceph_subsys_rgw, "rgw_cuobj: ");
  ldpp_dout_fmt(&dpp, 21, "parsing RDMA descriptor for address: {}", rdma_descr);
  auto first_colon = rdma_descr.find(':');
  if (first_colon == std::string::npos) {
    ldpp_dout_fmt(&dpp, -1,
                  "ERROR: failed to parse RDMA descriptor for address: no colon found");
    return std::nullopt;
  }
  ldpp_dout_fmt(&dpp, 21, "parsed address from RDMA descriptor: {}",
                rdma_descr.substr(0, first_colon));
  return ceph::parse<uint64_t>(rdma_descr.substr(0, first_colon), 16);
}

uint16_t RGWCuObjServer::get_channel_id()
{
  if (!tls_channel_valid) {
    tls_channel_id = m_server->allocateChannelId();
    tls_channel_valid = true;
  }
  ldpp_dout_fmt(this, 21, "allocated channel ID {}", tls_channel_id);
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
  // Spread concurrent scans across the pool using a per-thread random engine.
  size_t index = m_buf_count
      ? ceph::util::generate_random_number(m_buf_count - 1) : 0;
  for (size_t i = 0; i < m_buf_count; i++) {
    auto& entry = m_buffer_pool[index];
    // Avoid an atomic read-modify-write on entries that are already busy.
    if (entry.size >= needed_size &&
        !entry.in_use.load(std::memory_order_relaxed)) {
      bool expected = false;
      if (entry.in_use.compare_exchange_strong(expected, true,
                                               std::memory_order_acquire)) {
        return &entry;
      }
    }
    if (++index == m_buf_count) {
      index = 0;
    }
  }
  ldpp_dout_fmt(this, -1, "ERROR: no available RDMA buffer for size {}", needed_size);
  return nullptr;
}

void RGWCuObjServer::release_buffer(RDMABufEntry* buf)
{
  ldpp_dout_fmt(this, 21, "releasing RDMA buffer");
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
  auto addr = parse_rdma_descriptor_addr(rdma_descr);
  if (!addr) {
    return -EINVAL;
  }
  uint16_t channel = get_channel_id();
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
        chunk, rdma_descr, channel, total_read, &wc_status);
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
        chunk, rdma_descr, channel, total_written);
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
