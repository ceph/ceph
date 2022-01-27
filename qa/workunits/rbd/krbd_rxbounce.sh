#!/usr/bin/env bash

set -ex

rbd create --size 256 img

IMAGE_SIZE=$(rbd info --format=json img | python3 -c 'import sys, json; print(json.load(sys.stdin)["size"])')
OBJECT_SIZE=$(rbd info --format=json img | python3 -c 'import sys, json; print(json.load(sys.stdin)["object_size"])')
NUM_OBJECTS=$((IMAGE_SIZE / OBJECT_SIZE))
[[ $((IMAGE_SIZE % OBJECT_SIZE)) -eq 0 ]]
OP_SIZE=16384

DEV=$(sudo rbd map img)
{
    for ((i = 0; i < $NUM_OBJECTS; i++)); do
        echo pwrite -b $OP_SIZE -S $i $((i * OBJECT_SIZE)) $OP_SIZE
    done
    echo fsync
    echo quit
} | xfs_io $DEV
sudo rbd unmap $DEV

g++ -xc++ -o racereads - -lpthread <<EOF
#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <thread>
#include <vector>

const int object_size = $OBJECT_SIZE;
const int num_objects = $NUM_OBJECTS;
const int read_len = $OP_SIZE;
const int num_reads = 1024;

int main() {
  int fd = open("$DEV", O_DIRECT | O_RDONLY);
  assert(fd >= 0);

  void *buf;
  int r = posix_memalign(&buf, 512, read_len);
  assert(r == 0);

  std::vector<std::thread> threads;
  for (int i = 0; i < num_objects; i++) {
    threads.emplace_back(
        [fd, buf, read_off = static_cast<off_t>(i) * object_size]() {
          for (int i = 0; i < num_reads; i++) {
            auto len = pread(fd, buf, read_len, read_off);
            assert(len == read_len);
          }
        });
  }

  for (auto &t : threads) {
    t.join();
  }
}
EOF

DEV=$(sudo rbd map -o ms_mode=legacy img)
sudo dmesg -C
./racereads
[[ $(dmesg | grep -c 'libceph: osd.* bad crc/signature') -gt 100 ]]
sudo rbd unmap $DEV

DEV=$(sudo rbd map -o ms_mode=legacy,rxbounce img)
sudo dmesg -C
./racereads
[[ $(dmesg | grep -c 'libceph: osd.* bad crc/signature') -eq 0 ]]
sudo rbd unmap $DEV

DEV=$(sudo rbd map -o ms_mode=crc img)
sudo dmesg -C
./racereads
[[ $(dmesg | grep -c 'libceph: osd.* integrity error') -gt 100 ]]
sudo rbd unmap $DEV

DEV=$(sudo rbd map -o ms_mode=crc,rxbounce img)
sudo dmesg -C
./racereads
[[ $(dmesg | grep -c 'libceph: osd.* integrity error') -eq 0 ]]
sudo rbd unmap $DEV

# rxbounce is a no-op for secure mode
DEV=$(sudo rbd map -o ms_mode=secure img)
sudo dmesg -C
./racereads
[[ $(dmesg | grep -c 'libceph: osd.* integrity error') -eq 0 ]]
sudo rbd unmap $DEV

DEV=$(sudo rbd map -o ms_mode=secure,rxbounce img)
sudo dmesg -C
./racereads
[[ $(dmesg | grep -c 'libceph: osd.* integrity error') -eq 0 ]]
sudo rbd unmap $DEV

rbd rm img

echo OK
