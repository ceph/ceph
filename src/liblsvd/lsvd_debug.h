/*
 * file:        lsvd_debug.h
 * description: extern functions for unit tests
 */

#ifndef __LSVD_DEBUG_H__
#define __LSVD_DEBUG_H__

/* lightweight printf to buffer, retrieve via get_logbuf or lsvd.logbuf
 */
extern void do_log(const char *fmt, ...);
extern "C" int get_logbuf(char *buf);

#endif
