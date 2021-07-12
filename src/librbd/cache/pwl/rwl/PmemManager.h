// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_PMEMMANAGER_H
#define CEPH_LIBRBD_CACHE_RWL_PMEMMANAGER_H

#include <libpmem.h>
#include "librbd/cache/pwl/Types.h"

namespace librbd::cache::pwl::rwl {

class PmemDev {
 public:
  static std::unique_ptr<PmemDev> pmem_create_dev(const char *path,
      const uint64_t size, CephContext *cct);
  static std::unique_ptr<PmemDev> pmem_open_dev(const char *path,
      CephContext *cct);
  char *create_dev(const char *path, const uint64_t size);
  char *open_dev(const char *path);
  void close_dev();
  void init_data_bit(uint64_t metadata_size);
  void set_data_bit(uint64_t metadata_size, uint64_t first_valid_bit,
      uint64_t last_valid_entry_start_bit, uint64_t last_valid_entry_size);
  uint64_t alloc(uint64_t size);
  void release_to_here(uint64_t last_retire_entry_bit,
                                uint64_t last_retire_entry_size);
  char *get_head_addr();
  size_t get_mapped_len();
  bool is_pmem();
  PmemDev(const char * path, const uint64_t size, CephContext *cct);
  PmemDev(const char * path, CephContext *cct);
  ~PmemDev();
private:
  size_t m_mapped_len = 0;      /* size of the actual mapping, generally,
                                 * m_mapped_len == m_size */
  char *m_head_addr = nullptr;  /* the head addr of pmem device */
  bool m_is_pmem = false;
  mutable ceph::mutex m_lock;
  CephContext *m_cct;

  /*    |   root    | entries |                data area                      |
   *    -----------------------------------------------------------------------
   *    |root1/root2| enrties |      free      |   used         |    free     |
   *    -----------------------------------------------------------------------
   *    |           |         |                |                |             |
   *    0            m_first_data_bit  m_first_valid_bit  m_first_free_bit  m_max_bit
   *
   * space management, 1 bit = MIN_WRITE_ALLOC_SIZE Byte
   */
  uint64_t m_max_bit = 0;             /* the max bit */
  uint64_t m_first_data_bit = 0;      /* skip root and entry area */
  uint64_t m_first_free_bit = 0;      /* for alloc, data head +1 */
  uint64_t m_first_valid_bit = 0;     /* for free, data tail */
};

} // namespace librbd::cache::pwl::rwl
#endif // CEPH_LIBRBD_CACHE_RWL_PMEMMANAGER_H