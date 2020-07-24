#ifndef CEPH_MIRROR__ROLLING_CHECKSUM_H__
#define CEPH_MIRROR__ROLLING_CHECKSUM_H__

#include <algorithm>
#include <iomanip>
#include <iostream>

#include <sys/types.h>

#include "include/ceph_assert.h"

/* Reference: https://rsync.samba.org/tech_report/node3.html
 */
class RollingChecksum {
  public:
  static off_t const BLK_SIZE = 65536;

  RollingChecksum() {}

  RollingChecksum(char *buf, int len) {
    init(buf, len);
  }

  ~RollingChecksum() {}

  struct A_Sum {
    uint16_t sum = 0;
    void operator () (char c) {
      sum += c;
    }
  };

  struct B_Sum {
    uint16_t sum = 0;
    uint32_t remaining_len = static_cast<uint32_t>(BLK_SIZE);

    B_Sum() {}

    B_Sum(uint32_t len) : remaining_len(len) {}

    void operator () (char c) {
      sum += (remaining_len * c);
      --remaining_len;
    }
  };

  private:
  typedef uint32_t S_Sum;

  A_Sum a_sum;
  B_Sum b_sum;
  S_Sum s_sum;
  bool is_init = false;

  uint32_t make_s_sum(uint16_t a_sum, uint16_t b_sum) const
  {
    return (((uint32_t)b_sum << 16) + a_sum);
  }

  public:
  bool operator == (RollingChecksum const& rhs) const {
    return (s_sum == rhs.s_sum);
  }

  uint32_t get() const {
    return s_sum;
  }

  bool init_done() const {
    return is_init;
  }

  void init(char *buf, int len)
  {
    ceph_assert(buf);
    ceph_assert(len > 0);
    ceph_assert(len <= BLK_SIZE);

    a_sum = std::for_each(buf, buf+len, A_Sum());

    b_sum = std::for_each(buf, buf+len, B_Sum(len));

    s_sum = make_s_sum(a_sum.sum, b_sum.sum);

    is_init = true;
  }

  uint32_t step(char prev, char next)
  {
    a_sum.sum += -prev + next;
    b_sum.sum += (-BLK_SIZE * prev) + a_sum.sum;
    s_sum = make_s_sum(a_sum.sum, b_sum.sum);
    return s_sum;
  }
};


#endif
