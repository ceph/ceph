// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PmemManager.h"

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::rwl::PmemManager: " \
                           << this << " " <<  __func__ << ": "

namespace librbd::cache::pwl::rwl {

/* p2align and p2roundup require uint64_t, keep the same with offset */
static const uint64_t min_write_alloc_size = PMEM_MIN_WRITE_ALLOC_SIZE;

PmemDev::PmemDev(const char * path, const uint64_t size, CephContext *cct)
  : m_lock(ceph::make_mutex(pwl::unique_lock_name(
      "librbd::cache::pwl::PmemManager::m_lock", this))),
    m_cct(cct) {
  bool allow_simulation = cct->_conf.get_val<bool>(
      "rbd_persistent_cache_allow_simulation");
  m_head_addr = create_dev(path, size);
  if (!m_head_addr) {
    lderr(m_cct) << "failed to create pmem device." << dendl;
    return;
  }
  if (allow_simulation == false && m_is_pmem == false) {
    close_dev();
    lderr(m_cct) << "err: the hardware does not support pmem,"
                 << " and debug simulation is not enabled."
                 << dendl;
    return;
  }
  m_max_offset = p2align(m_mapped_len, min_write_alloc_size);
  ldout(m_cct, 5) << "successfully created pmem Device."
                  << " m_is_pmem: " << m_is_pmem
                  << " m_head_addr: " << reinterpret_cast<void *>(m_head_addr)
                  << " m_mapped_len: " << m_mapped_len
                  << " m_max_offset: " << m_max_offset
                  << dendl;
}

PmemDev::PmemDev(const char * path, CephContext *cct)
  : m_lock(ceph::make_mutex(pwl::unique_lock_name(
      "librbd::cache::pwl::PmemManager::m_lock", this))),
    m_cct(cct) {
  bool allow_simulation = cct->_conf.get_val<bool>(
      "rbd_persistent_cache_allow_simulation");
  m_head_addr = open_dev(path);
  if (!m_head_addr) {
    lderr(m_cct) << "failed to create pmem device." << dendl;
    return;
  }
  if (allow_simulation == false && m_is_pmem == false) {
    close_dev();
    lderr(m_cct) << "err: the hardware does not support pmem"
                 << " and debug simulation is not enabled."
                 << dendl;
    return;
  }
  m_max_offset = p2align(m_mapped_len, min_write_alloc_size);
  ldout(m_cct, 5) << "successfully map existing pmem Device."
                  << " m_is_pmem: " << m_is_pmem
                  << " m_head_addr: " << reinterpret_cast<void *>(m_head_addr)
                  << " m_mapped_len: " << m_mapped_len
                  << " m_max_offset: " << m_max_offset
                  << dendl;
}

PmemDev::~PmemDev() {
}

std::unique_ptr<PmemDev> PmemDev::pmem_create_dev(const char *path,
    const uint64_t size, CephContext *cct) {
  std::unique_ptr<PmemDev> pmem_dev;
  pmem_dev = std::make_unique<PmemDev>(path, size, cct);
  if (!pmem_dev->m_head_addr) {
    return nullptr;
  }
  return pmem_dev;
}

std::unique_ptr<PmemDev> PmemDev::pmem_open_dev(const char *path,
                                                CephContext *cct) {
  std::unique_ptr<PmemDev> pmem_dev;
  pmem_dev = std::make_unique<PmemDev>(path, cct);
  if (!pmem_dev->m_head_addr) {
    return nullptr;
  }
  return pmem_dev;
}

char *PmemDev::create_dev(const char *path, const uint64_t size) {
  int is_pmem = 0;
  char *head_addr = nullptr;

  /* create a pmem file and memory map it */
  head_addr = static_cast<char *>(pmem_map_file(path, size,
              PMEM_FILE_CREATE | PMEM_FILE_SPARSE,
              0666, &m_mapped_len, &is_pmem));
  if (head_addr) {
    m_is_pmem = is_pmem;
  } else {
    lderr(m_cct) << "failed to map new pmem file." << dendl;
  }

  return head_addr;
}

char *PmemDev::open_dev(const char *path) {
  int is_pmem = 0;
  char *head_addr = nullptr;

  /* Memory map an existing file */
  head_addr = static_cast<char *>(pmem_map_file(path, 0, 0, 0,
                                                &m_mapped_len, &is_pmem));
  if (head_addr) {
    m_is_pmem = is_pmem;
  } else {
    lderr(m_cct) << "failed to map existing pmem file." << dendl;
  }

  return head_addr;
}

void PmemDev::close_dev() {
  pmem_unmap(m_head_addr, m_mapped_len);
  m_head_addr = nullptr;
}

void PmemDev::init_data_offset(uint64_t metadata_size) {
  m_first_data_offset = p2roundup(metadata_size, min_write_alloc_size);
  m_first_valid_offset = m_first_data_offset;
  m_first_free_offset = m_first_data_offset;
  ldout(m_cct, 5) << "init pmem space."
                  << " m_first_data_offset: " << m_first_data_offset
                  << " m_first_free_offset: " << m_first_free_offset
                  << " m_first_valid_offset: " << m_first_valid_offset
                  << dendl;
}

void PmemDev::set_data_offset(uint64_t metadata_size,
                              uint64_t first_valid_offset,
                              uint64_t the_last_valid_entry_end_offset) {
  m_first_data_offset = p2roundup(metadata_size, min_write_alloc_size);
  m_first_valid_offset = first_valid_offset;
  m_first_free_offset = p2roundup(the_last_valid_entry_end_offset,
                                  min_write_alloc_size);
  ldout(m_cct, 5) << "successfully recovery pmem data space."
                  << " m_first_data_offset: " << m_first_data_offset
                  << " m_first_free_offset: " << m_first_free_offset
                  << " m_first_valid_offset: " << m_first_valid_offset
                  << dendl;
}

uint64_t PmemDev::alloc(uint64_t size) {
  /* keep m_first_free_offset to m_first_valid_offset at least 1 offset free
   * to distinguish full allocated space from full free space */
  uint64_t ret_offset = 0;
  uint64_t aligned_size = p2roundup(size, min_write_alloc_size);

  std::lock_guard locker(m_lock);
  if ((m_first_valid_offset <= m_first_free_offset &&
       m_first_free_offset + aligned_size < m_max_offset) ||
      (m_first_valid_offset > m_first_free_offset &&
       m_first_free_offset + aligned_size < m_first_valid_offset)) {
    m_first_free_offset += aligned_size;
    ret_offset = m_first_free_offset - aligned_size;
  } else if (m_first_valid_offset <= m_first_free_offset &&
             m_first_free_offset + aligned_size >= m_max_offset &&
             m_first_data_offset + aligned_size < m_first_valid_offset) {
    m_first_free_offset = m_first_data_offset + aligned_size;
    ret_offset = m_first_data_offset;
  } else {
    ldout(m_cct, 20) << "allocation failed, no space. size: " << size << dendl;
    return 0;
  }

  ldout(m_cct, 20) << "allocation succeeded, start offset: " << ret_offset
                   << " size: " << size
                   << " new m_first_free_offset: " << m_first_free_offset
                   << dendl;
  return ret_offset;
}

void PmemDev::release(uint64_t the_last_entry_end_offset) {
  std::lock_guard locker(m_lock);
  m_first_valid_offset = p2roundup(the_last_entry_end_offset,
                                   min_write_alloc_size);
  ldout(m_cct, 20) << "m_first_valid_offset: " << m_first_valid_offset
                   << " release to entry, end offset: "
                   << the_last_entry_end_offset
                   << dendl;
}

char *PmemDev::get_head_addr() const {
  return m_head_addr;
}

uint64_t PmemDev::get_mapped_len() {
  return m_mapped_len;
}

bool PmemDev::is_pmem() {
  return m_is_pmem;
}

} // namespace librbd::cache::pwl::rwl
