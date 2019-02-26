#ifndef CEPH_RBD_GGATE_DEBUG_H
#define CEPH_RBD_GGATE_DEBUG_H

#ifdef __cplusplus
extern "C" {
#endif

void debug(int level, const char *fmt, ...) __printflike(2, 3);
void debugv(int level, const char *fmt, va_list ap) __printflike(2, 0);
void err(const char *fmt, ...) __printflike(1, 2);
void errx(const char *fmt, ...) __printflike(1, 2);

#ifdef __cplusplus
}
#endif

#endif // CEPH_RBD_GGATE_DEBUG_H
