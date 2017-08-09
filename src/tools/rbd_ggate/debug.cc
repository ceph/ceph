#include "common/debug.h"
#include "common/errno.h"
#include "debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd::ggate: "

extern "C" void debugv(int level, const char *fmt, va_list ap) {
    char *msg;
    int saved_errno = errno;

    if (g_ceph_context == nullptr) {
        return;
    }

    vasprintf(&msg, fmt, ap);

    dout(level) << msg << dendl;

    free(msg);
    errno = saved_errno;
}

extern "C" void debug(int level, const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    debugv(level, fmt, ap);
    va_end(ap);
}

extern "C" void errx(const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    debugv(-1, fmt, ap);
    va_end(ap);
}

extern "C" void err(const char *fmt, ...) {
    va_list ap;
    char *msg;
    int saved_errno = errno;

    va_start(ap, fmt);
    vasprintf(&msg, fmt, ap);
    va_end(ap);
    errno = saved_errno;

    errx("%s: %s", msg, cpp_strerror(errno).c_str());

    free(msg);
}
