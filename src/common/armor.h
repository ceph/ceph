#ifndef __CEPH_ARMOR
#define __CEPH_ARMOR

extern "C" {
int ceph_armor(char *dst, const char *src, const char *end);
int ceph_unarmor(char *dst, const char *src, const char *end);
}

#endif
