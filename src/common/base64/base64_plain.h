#ifndef PLAIN_ARMOR_H
#define PLAIN_ARMOR_H

#ifdef __cplusplus
extern "C" {
#endif

extern int plain_armor(char *dst, const char *dst_end, const char *src, const char *end);

extern int plain_unarmor(char *dst, const char *dst_end, const char *src, const char *end);
#ifdef __cplusplus
}
#endif

#endif
