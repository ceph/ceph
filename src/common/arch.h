#ifndef CEPH_ARCH_H
#define CEPH_ARCH_H

static const char *get_arch()
{
#if defined(__i386__)
  return "i386";
#elif defined(__x86_64__)
  return "x86-64";
#else
    return "unknown";
#endif
}

#endif
