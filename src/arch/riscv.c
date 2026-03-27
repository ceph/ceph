/**
 * Runtime detection of RISC-V vector crypto support.
 */

#include <asm/hwprobe.h>
#include <sys/syscall.h>
#include <unistd.h>

int ceph_arch_riscv_zbc = 0;
int ceph_arch_riscv_zvbc = 0;

#ifndef RISCV_HWPROBE_EXT_ZBC
#define RISCV_HWPROBE_EXT_ZBC (1ULL << 7)
#endif

#ifndef RISCV_HWPROBE_EXT_ZVBC
#define RISCV_HWPROBE_EXT_ZVBC (1ULL << 18)
#endif

static int do_hwprobe(struct riscv_hwprobe *pairs, size_t count)
{
    return syscall(__NR_riscv_hwprobe, pairs, count, 0, NULL, 0);
}

void ceph_arch_riscv_probe(void)
{
    struct riscv_hwprobe pairs[] = {
        { .key = RISCV_HWPROBE_KEY_IMA_EXT_0 },
    };

    if (do_hwprobe(pairs, 1) == 0) {
        unsigned long long ext = pairs[0].value;
        ceph_arch_riscv_zbc  = (ext & RISCV_HWPROBE_EXT_ZBC);
        ceph_arch_riscv_zvbc = (ext & RISCV_HWPROBE_EXT_ZVBC);
    }
}
