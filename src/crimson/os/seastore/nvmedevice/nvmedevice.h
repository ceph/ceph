//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <linux/nvme_ioctl.h>
#include <libaio.h>

#include "crimson/osd/exceptions.h"
#include "crimson/common/layout.h"

namespace ceph {
  namespace buffer {
    class bufferptr;
  }
}

namespace crimson::os::seastore::nvme_device {


using read_ertr = crimson::errorator<
  crimson::ct_error::input_output_error,
  crimson::ct_error::invarg,
  crimson::ct_error::enoent,
  crimson::ct_error::erange>;

using write_ertr = crimson::errorator<
  crimson::ct_error::input_output_error,
  crimson::ct_error::invarg,
  crimson::ct_error::ebadf,
  crimson::ct_error::enospc>;

using open_ertr = crimson::errorator<
  crimson::ct_error::input_output_error,
  crimson::ct_error::invarg,
  crimson::ct_error::enoent>;

using nvme_command_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;

/*
 * Interface between NVMe SSD and its user.
 *
 * NVMeBlockDevice provides not only the basic APIs for IO, but also helper APIs
 * to accelerate SSD IO performance and reduce system overhead. By aggresively
 * utilizing and abstract useful features of latest NVMe SSD, it helps user ease
 * to get high performance of NVMe SSD and low system overhead.
 *
 * Various implementations with different interfaces such as POSIX APIs, Seastar,
 * and SPDK, are available.
 */
class NVMeBlockDevice {
protected:
  uint64_t size = 0;
  uint64_t block_size = 0;

  uint64_t write_granularity = 4096;
  uint64_t write_alignment = 4096;

public:
  NVMeBlockDevice() {}
  virtual ~NVMeBlockDevice() = default;

  template <typename T>
  static NVMeBlockDevice *create() {
    return new T();
  }

  /*
   * Service NVMe device relative size
   *
   * size : total size of device in byte.
   *
   * block_size : IO unit size in byte. Caller should follow every IO command
   * aligned with block size.
   *
   * preffered_write_granularity(PWG), preffered_write_alignment(PWA) : IO unit
   * size for write in byte. Caller should request every write IO sized multiple
   * times of PWG and aligned starting address by PWA. Available only if NVMe
   * Device supports NVMe protocol 1.4 or later versions.
   */
  uint64_t get_size() const { return size; }
  uint64_t get_block_size() const { return block_size; }
  uint64_t get_preffered_write_granularity() const { return write_granularity; }
  uint64_t get_preffered_write_alignment() const { return write_alignment; }

  virtual read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) = 0;

  virtual write_ertr::future<> write(
    uint64_t offset,
    bufferptr &bptr) = 0;

  // TODO
  virtual int discard(uint64_t offset, uint64_t len) { return 0; }

  virtual open_ertr::future<> open(
      const std::string& path,
      seastar::open_flags mode) = 0;

  virtual seastar::future<> close() = 0;

};


class TestMemory : public NVMeBlockDevice {
public:

  TestMemory(size_t size) : buf(nullptr), size(size) {}
  ~TestMemory() {
    if (buf) {
      ::munmap(buf, size);
      buf = nullptr;
    }
  }

  open_ertr::future<> open(const std::string &in_path, seastar::open_flags mode);

  write_ertr::future<> write(
    uint64_t offset,
    bufferptr &bptr);

  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr);

  seastar::future<> close();

  char *buf;
  size_t size;
};

}
