#include <errno.h>

#include <gtest/gtest.h>

extern "C" {
#include "crush_compat.h"
#include "crush/hash.h"
#include "crush/balance.h"
#include "crush/crush_ln_table.h"
}

/* compute 2^44*log2(input+1) */
static __u64 crush_ln(unsigned int xin)
{
	unsigned int x = xin;
	int iexpon, index1, index2;
	__u64 RH, LH, LL, xl64, result;

	x++;

	/* normalize input */
	iexpon = 15;

	// figure out number of bits we need to shift and
	// do it in one step instead of iteratively
	if (!(x & 0x18000)) {
	  int bits = __builtin_clz(x & 0x1FFFF) - 16;
	  x <<= bits;
	  iexpon = 15 - bits;
	}

	index1 = (x >> 8) << 1;
	/* RH ~ 2^56/index1 */
	RH = __RH_LH_tbl[index1 - 256];
	/* LH ~ 2^48 * log2(index1/256) */
	LH = __RH_LH_tbl[index1 + 1 - 256];

	/* RH*x ~ 2^48 * (2^15 + xf), xf<2^8 */
	xl64 = (__s64)x * RH;
	xl64 >>= 48;

	result = iexpon;
	result <<= (12 + 32);

	index2 = xl64 & 0xff;
	/* LL ~ 2^48*log2(1.0+index2/2^15) */
	LL = __LL_tbl[index2];

	LH = LH + LL;

	LH >>= (48 - 12 - 32);
	result += LH;

	return result;
}


// crush_balance will fail if straws are all identical. Although it is not impossible,
// it is extremely rare and can be ignored.

TEST(balance, crush_balance_1) {
  int values_count = 5;
  int items_count = 2;
  __s64 straws[values_count * items_count] = {
    -100 * 0x10000, -600 * 0x10000,
    -200 * 0x10000, -700 * 0x10000,
    -300 * 0x10000, -800 * 0x10000,
    -400 * 0x10000, -900 * 0x10000,
    -500 * 0x10000, -1000 * 0x10000,
  };
  __u32 target_weights[items_count] = {
    1 * 0x10000, 1 * 0x10000,
  };
  __u32 weights[items_count];
  memcpy(weights, target_weights, items_count * sizeof(__u32));
  EXPECT_EQ(1, balance_values(values_count, items_count, straws, target_weights, weights));
  __u32 expected_weights[items_count] = {
    int(0.5 * 0x10000), int(1.5 * 0x10000),
  };
  for (int i = 0; i < items_count; i++) {
    std::cerr << "weight[" << i << "] => " << (double)weights[i] / 0x10000 << std::endl;
    EXPECT_EQ(expected_weights[i], weights[i]);
  }
}

TEST(balance, crush_balance_2) {
  int values_count = 25;
  int items_count = 5;
  __s64 straws[values_count * items_count];
  for (int i = 0; i < values_count; i++)
    for (int j = 0; j < items_count; j++) {
      unsigned int u = crush_hash32_2(CRUSH_HASH_DEFAULT, i, j);
      u &= 0xffff;
      straws[i * items_count + j] = crush_ln(u) - 0x1000000000000ll;
    }
  __u32 target_weights[items_count] = {
    3 * 0x10000, 0x10000, 0x10000, 0x10000, 0x10000,
  };
  __u32 weights[items_count];
  memcpy(weights, target_weights, items_count * sizeof(__u32));
  EXPECT_EQ(303, balance_values(values_count, items_count, straws, target_weights, weights));
  double expected_weights[items_count] = {
    int(1.0 * 0x10000), int(1.0 * 0x10000), int(1.0 * 0x10000), int(1.0 * 0x10000), int(1.0 * 0x10000),
  };
  for (int i = 0; i < items_count; i++) {
    std::cerr << "weight[" << i << "] => " << (double)weights[i] / 0x10000 << std::endl;
    EXPECT_EQ(expected_weights[i], weights[i]);
  }
}

#if 0
TEST(balance, crush_balance_3) {
  int values_count = 100;
  int items_count = 5;
  __s64 straws[values_count * items_count];
  for (int i = 0; i < values_count; i++)
    for (int j = 0; j < items_count; j++)
      straws[i * items_count + j] = crush_hash32_2(CRUSH_HASH_DEFAULT, i, j);
  __u32 target_weights[items_count] = {
    3 * 0x10000, 2 * 0x10000, 1 * 0x10000, 3 * 0x10000, 2 * 0x10000,
  };
  double weights[items_count];
  EXPECT_EQ(303, balance_values(values_count, items_count, straws, target_weights, weights));
  double expected_weights[items_count] = {
    1.309173617798661, 0.91518045300468509, 0.97854631696668815, 1.0, 0.85238703752753908
  };
  for (int i = 0; i < items_count; i++) {
    std::cerr << "weight[" << i << "]" << std::endl;
    EXPECT_DOUBLE_EQ(expected_weights[i], weights[i]);
  }
}
#endif

// Local Variables:
// compile-command: "cd ../build ; make unittest_balance && valgrind --tool=memcheck test/unittest_balance"
// End:
