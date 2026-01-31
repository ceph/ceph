/**
 * Runtime detection of RISC-V vector crypto support.
 */
#include <sys/auxv.h>
#include <asm/hwcap.h>

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <linux/types.h>
#include <stdio.h>
#include <string.h>

 // riscv_hwprobe definitions for compatibility with older kernels
#ifndef __NR_riscv_hwprobe
#if defined(__riscv) && __riscv_xlen == 64
  #define __NR_riscv_hwprobe 258
#endif
#endif

// RISC-V hardware capability detection
#ifndef COMPAT_HWCAP_ISA_V
#define COMPAT_HWCAP_ISA_V  (1 << ('V' - 'A'))
#endif

// Define riscv_hwprobe structure if not available in headers
#ifndef HAVE_RISCV_HWPROBE
struct riscv_hwprobe {
    __s64 key;
    __u64 value;
};
#endif

// hwprobe key definitions
#ifndef RISCV_HWPROBE_KEY_IMA_EXT_0
#define RISCV_HWPROBE_KEY_IMA_EXT_0      4
#define RISCV_HWPROBE_EXT_ZBC            (1 << 7)
#define RISCV_HWPROBE_EXT_ZVBC           (1 << 18)
#endif

#define REQ_KERNEL_MAJOR 6
#define REQ_KERNEL_MINOR 4

/**
 * Parse kernel version from uname
 */
static int parse_kernel_version(int *major, int *minor, int *patch) {
    struct utsname uts;
    if (uname(&uts) != 0) {
        return -1;
    }

    *patch = 0; // Default value if not parsed
    if (sscanf(uts.release, "%d.%d.%d", major, minor, patch) >= 2) {
        return 0;
    }

    return -1;
}

/**
 * Check if kernel version supports hwprobe
 * hwprobe was introduced in Linux 6.4
 */
static int kernel_supports_hwprobe(void) {
    int major, minor, patch;
    if (parse_kernel_version(&major, &minor, &patch) != 0) {
        return 0; // Unknown, assume not supported
    }

    if (major > REQ_KERNEL_MAJOR) {
        return 1;
    }
    if (major == REQ_KERNEL_MAJOR && minor >= REQ_KERNEL_MINOR) {
        return 1;
    }

    return 0;
}

/**
 * Detect extensions using riscv_hwprobe
 * Returns: -1 on error
 */
static int detect_with_hwprobe(int *has_zbc, int *has_zvbc) {
   #ifdef __NR_riscv_hwprobe
   if (!kernel_supports_hwprobe()) {
       return -1;
   }

   struct riscv_hwprobe probe;
   probe.key = RISCV_HWPROBE_KEY_IMA_EXT_0;
   probe.value = 0;

   long ret = syscall(__NR_riscv_hwprobe, &probe, 1, 0, NULL, 0);
   if (ret != 0) {
       return -1;
   }

   *has_zbc  = (probe.value & RISCV_HWPROBE_EXT_ZBC)  ? 1 : 0;
   *has_zvbc = (probe.value & RISCV_HWPROBE_EXT_ZVBC) ? 1 : 0;

   return 0;
   #else
   return -1;
   #endif
}

/**
 * Check for RISC-V extension support via /proc/cpuinfo
 */
static int check_riscv_extension(const char *extension) {
    FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
    if (!cpuinfo) {
        return 0;
    }

    char line[512];
    int found = 0;

    while (fgets(line, sizeof(line), cpuinfo)) {
       if(strncmp(line, "isa", 3) == 0) {
       if (strstr(line, extension)) {
           found = 1;
       break;
       }
   }
   }

    fclose(cpuinfo);
    return found;
}

/**
 * Detect extensions via /proc/cpuinfo
 */
static int detect_with_cpuinfo(int *has_zbc, int *has_zvbc) {

    *has_zbc = check_riscv_extension("zbc");
    *has_zvbc = check_riscv_extension("zvbc");

    if (*has_zbc == 0 && *has_zvbc == 0) {
        return -1;
    }

    return 0;
}

/**
 * Main detection function with multiple fallback methods
 */
static int detect_crypto_extensions(int *has_zbc, int *has_zvbc) {
    if (has_zbc == NULL || has_zvbc == NULL){
        return -1;
    }

    static int cached_zbc = -1;
    static int cached_zvbc = -1;

    // Return cached results if available
    if (cached_zbc != -1 && cached_zvbc != -1) {
        *has_zbc = cached_zbc;
        *has_zvbc = cached_zvbc;
        return 0;
    }

    // Initialize to not found
    *has_zbc = 0;
    *has_zvbc = 0;

    if (detect_with_hwprobe(has_zbc, has_zvbc) == 0) {
        cached_zbc = *has_zbc;
        cached_zvbc = *has_zvbc;
        return 0;
    }

    if (detect_with_cpuinfo(has_zbc, has_zvbc) < 0) {
        return -1;
    }

    cached_zbc = *has_zbc;
    cached_zvbc = *has_zvbc;
    return 0;
}




/* flags we export */
int ceph_arch_riscv_crc32 = 0;

/* Supported starting from the IBM z13 */
int ceph_arch_riscv_probe(void)
{
  ceph_arch_riscv_crc32 = 0;
  int has_zbc = 0, has_zvbc = 0;
  
  if (getauxval(AT_HWCAP) & COMPAT_HWCAP_ISA_V) {
  	if (detect_crypto_extensions(&has_zbc, &has_zvbc) == 0 && has_zbc && has_zvbc)
    		ceph_arch_riscv_crc32 = 1;
  }

  return 0;
}

