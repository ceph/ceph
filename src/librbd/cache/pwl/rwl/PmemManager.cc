// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PmemManager.h"

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::rwl::PmemManager: " \
                           << " " <<  __func__ << ": "

namespace librbd::cache::pwl::rwl {

static inline uint64_t size_to_bit(uint64_t size) {
  return size % MIN_WRITE_ALLOC_SIZE ? size / MIN_WRITE_ALLOC_SIZE + 1 :
         size / MIN_WRITE_ALLOC_SIZE;
}

PmemDev::PmemDev(const char * path, const uint64_t size, CephContext *cct)
  : m_lock(ceph::make_mutex(pwl::unique_lock_name(
      "librbd::cache::pwl::PmemManager::m_lock", this))),
    m_cct(cct)
{
  m_head_addr = create_dev(path, size);
  if (!m_head_addr) {
    lderr(m_cct) << "failed to create pmem device." << dendl;
    return;
  }
  m_max_bit = m_mapped_len / MIN_WRITE_ALLOC_SIZE - 1;  // start from 0
  ldout(m_cct, 5) << "successfully created pmem Device. "
                  << " m_is_pmem: " << m_is_pmem
                  << " m_head_addr: " << (void *)m_head_addr
                  << " m_mapped_len: " << m_mapped_len
                  << " m_max_bit: " << m_max_bit
                  << dendl;
}

PmemDev::PmemDev(const char * path, CephContext *cct)
  : m_lock(ceph::make_mutex(pwl::unique_lock_name(
      "librbd::cache::pwl::PmemManager::m_lock", this))),
    m_cct(cct)
{
  m_head_addr = open_dev(path);
  if (!m_head_addr) {
    lderr(m_cct) << "failed to create pmem device." << dendl;
    return;
  }
  m_max_bit = m_mapped_len / MIN_WRITE_ALLOC_SIZE - 1;
  ldout(m_cct, 5) << "successfully map existing pmem Device. "
                  << " m_is_pmem: " << m_is_pmem
                  << " m_head_addr: " << (void *)m_head_addr
                  << " m_mapped_len: " << m_mapped_len
                  << " m_max_bit: " << m_max_bit
                  << dendl;
}

PmemDev::~PmemDev() {
  //close_dev();  // Generally, the device will be shut down manually
}

std::unique_ptr<PmemDev> PmemDev::pmem_create_dev(const char *path,
    const uint64_t size, CephContext *cct) {
  std::unique_ptr<PmemDev> pmem_dev;
  pmem_dev =std::make_unique<PmemDev>(path, size, cct);
  if (!pmem_dev.get()) {
    return nullptr;
  }
  return pmem_dev;
}

std::unique_ptr<PmemDev> PmemDev::pmem_open_dev(const char *path,
                                           CephContext *cct) {
  std::unique_ptr<PmemDev> pmem_dev;
  pmem_dev =std::make_unique<PmemDev>(path, cct);
  if (!pmem_dev.get()) {
    return nullptr;
  }
  return pmem_dev;
}

char *PmemDev::create_dev(const char *path, const uint64_t size) {
  int is_pmem = 0;
  char *head_addr = nullptr;

  /* create a pmem file and memory map it */
  if (!(head_addr = (char *)pmem_map_file(path, size, PMEM_FILE_CREATE, 0666,
                    &m_mapped_len, &is_pmem))) {
    lderr(m_cct) << "failed to map new pmem file." << dendl;
    return nullptr;
  }
  m_is_pmem = is_pmem ? true : false;

  return head_addr;
}

char *PmemDev::open_dev(const char *path) {
  int is_pmem = 0;
  char *head_addr = nullptr;

  /* Memory map an existing file */
  if (!(head_addr = (char *)pmem_map_file(path, 0, 0, 0, &m_mapped_len,
                    &is_pmem))) {
    lderr(m_cct) << "failed to map existing pmem file." << dendl;
    return nullptr;
  }
  m_is_pmem = is_pmem ? true : false;

  return head_addr;
}

void PmemDev::close_dev() {
  pmem_unmap(m_head_addr, m_mapped_len);
}

void PmemDev::init_data_bit(uint64_t metadata_size) {
  /* m_first_data_bit map to entry index[0] */
  m_first_data_bit = size_to_bit(metadata_size);
  m_first_free_bit = m_first_valid_bit = m_first_data_bit;
  ldout(m_cct, 5) << " init pmem space. "
                  << " m_first_data_bit: " << m_first_data_bit
                  << " m_first_free_bit: " << m_first_free_bit
                  << " m_first_valid_bit: " << m_first_valid_bit
                  << dendl;
}

void PmemDev::set_data_bit(uint64_t metadata_size, uint64_t first_valid_bit,
                  uint64_t last_valid_entry_start_bit,
                  uint64_t last_valid_entry_size) {
  m_first_data_bit = size_to_bit(metadata_size);
  m_first_valid_bit = first_valid_bit;
  m_first_free_bit = last_valid_entry_start_bit +
                      size_to_bit(last_valid_entry_size);
  ldout(m_cct, 5) << "successfully recovery pmem data space. "
                  << " m_first_data_bit: " << m_first_data_bit
                  << " m_first_free_bit: " << m_first_free_bit
                  << " m_first_valid_bit: " << m_first_valid_bit
                  << dendl;
}

uint64_t PmemDev::alloc(uint64_t size) {
  /* keep m_first_free_bit to m_first_valid_bit at least 1 bit free
   * to distinguish full allocated space from full free space */
  std::lock_guard locker(m_lock);
  if ((m_first_valid_bit <= m_first_free_bit &&
       m_first_free_bit + size_to_bit(size) < m_max_bit) ||
      (m_first_valid_bit > m_first_free_bit &&
       m_first_free_bit + size_to_bit(size) < m_first_valid_bit)) {
    m_first_free_bit += size_to_bit(size);
    ldout(m_cct, 20) << "alloc for space backward, offset in pmem: "
                     << m_first_free_bit - size_to_bit(size)
                     << " size: " << size
                     << " m_first_free_bit: "
                     << m_first_free_bit
                     << dendl;
    return m_first_free_bit - size_to_bit(size);
  } else if (m_first_valid_bit <= m_first_free_bit &&
             m_first_free_bit + size_to_bit(size) >= m_max_bit &&
             m_first_data_bit + size_to_bit(size) < m_first_valid_bit) {
    m_first_free_bit = m_first_data_bit + size_to_bit(size);
    ldout(m_cct, 20) << "alloc for space from the head, offset in pmem: "
                     << m_first_data_bit
                     << " size: " << size
                     << " m_first_free_bit: "
                     << m_first_free_bit
                     << dendl;
    return m_first_data_bit;
  }

  ldout(m_cct, 20) << "size:" << size << " alloc fail, no space." << dendl;
  return 0;
}

void PmemDev::release_to_here(uint64_t last_retire_entry_bit,
                              uint64_t last_retire_entry_size) {
  std::lock_guard locker(m_lock);
  m_first_valid_bit = last_retire_entry_bit +
                      size_to_bit(last_retire_entry_size);
  ldout(m_cct, 20) << "release to m_first_valid_bit: " << m_first_valid_bit
                   << " last_retire_entry_bit: " << last_retire_entry_bit
                   << " last_retire_entry_size: " << last_retire_entry_size
                   << dendl;
}

char *PmemDev::get_head_addr() {
  return m_head_addr;
}

size_t PmemDev::get_mapped_len() {
  return m_mapped_len;
}

bool PmemDev::is_pmem() {
  return m_is_pmem;
}

} // namespace librbd::cache::pwl::rwl