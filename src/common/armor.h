#ifndef CEPH_ARMOR_H
#define CEPH_ARMOR_H

extern "C" {
int ceph_armor(char *dst, const char *dst_end,
	       const char *src, const char *end);
int ceph_unarmor(char *dst, const char *dst_end,
		 const char *src, const char *end);
}

#endif
