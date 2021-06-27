//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/buffer.h"
#include "crimson/os/seastore/random_block_manager/nvmedevice.h"
#include "test/crimson/gtest_seastar.h"
#include "include/stringify.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace nvme_device;

struct nvdev_test_t : seastar_test_suite_t {
  std::unique_ptr<NVMeBlockDevice> device;
  std::string dev_path;

  static const uint64_t DEV_SIZE = 1024 * 1024 * 1024;

  nvdev_test_t() :
    device(nullptr),
    dev_path("randomblock_manager.test_nvmedevice" + stringify(getpid())) {
    int fd = ::open(dev_path.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0644);
    ceph_assert(fd >= 0);
    ::ftruncate(fd, DEV_SIZE);
    ::close(fd);
  }
  ~nvdev_test_t() {
    ::unlink(dev_path.c_str());
  }
};

static const uint64_t BUF_SIZE = 1024;
static const uint64_t BLK_SIZE = 4096;

struct nvdev_test_block_t {
  uint8_t data[BUF_SIZE];

  DENC(nvdev_test_block_t, v, p) {
    DENC_START(1, 1, p);
    for (uint64_t i = 0 ; i < BUF_SIZE; i++)
    {
      denc(v.data[i], p);
    }
    DENC_FINISH(p);
  }
};

WRITE_CLASS_DENC_BOUNDED(
  nvdev_test_block_t
)

TEST_F(nvdev_test_t, write_and_verify_test)
{
  run_async([this] {
    device = NVMeBlockDevice::create<PosixNVMeDevice>();
    device->open(dev_path, seastar::open_flags::rw).unsafe_get();
    nvdev_test_block_t original_data;
    std::minstd_rand0 generator;
    uint8_t value = generator();
    memset(original_data.data, value, BUF_SIZE);
    uint64_t bl_length = 0;
    {
      bufferlist bl;
      encode(original_data, bl);
      bl_length = bl.length();
      auto write_buf = ceph::bufferptr(buffer::create_page_aligned(BLK_SIZE));
      bl.begin().copy(bl_length, write_buf.c_str());
      device->write(0, write_buf).unsafe_get();
    }

    nvdev_test_block_t read_data;
    {
      auto read_buf = ceph::bufferptr(buffer::create_page_aligned(BLK_SIZE));
      device->read(0, read_buf).unsafe_get();
      bufferlist bl;
      bl.push_back(read_buf);
      auto bliter = bl.cbegin();
      decode(read_data, bliter);
    }

    int ret = memcmp(original_data.data, read_data.data, BUF_SIZE);
    device->close().wait();
    ASSERT_TRUE(ret == 0);
    device.reset(nullptr);
  });
}

