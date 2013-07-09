#ifndef CEPH_ARMOR_H
#define CEPH_ARMOR_H

#ifdef __cplusplus
extern "C" {
#endif

int ceph_armor(char *dst, const char *dst_end,
	       const char *src, const char *end);

int ceph_armor_linebreak(char *dst, const char *dst_end,
	       const char *src, const char *end,
	       int line_width);
int ceph_unarmor(char *dst, const char *dst_end,
		 const char *src, const char *end);
#ifdef __cplusplus
}
#endif

#endif
