/*
 * file:        lsvd_types.h
 * description: basic types, not relying on any other ones
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#ifndef __LSVD_TYPES_H__
#define __LSVD_TYPES_H__

#include <stdint.h>
#include <vector>

typedef int64_t sector_t;
typedef int page_t;

enum lsvd_op {
    OP_READ = 2,
    OP_WRITE = 4
};

enum { LSVD_MAGIC = 0x4456534c };

/* if buf[offset]...buf[offset+len] contains an array of type T,
 * copy them into the provided output vector
 */
template<class T>
void decode_offset_len(char *buf, size_t offset, size_t len,
                       std::vector<T> &vals) {
    T *p = (T*)(buf + offset), *end = (T*)(buf + offset + len);
    for (; p < end; p++)
        vals.push_back(*p);
}

/* buf[offset ... offset+len] contains array of type T, with variable 
 * length field name_len.
 */
template<class T>
void decode_offset_len_ptr(char *buf, size_t offset, size_t len,
                       std::vector<T*> &vals) {
    T *p = (T*)(buf + offset), *end = (T*)(buf + offset + len);
    for (; p < end;) {
        vals.push_back(p);
        p = (T*)((char*)p + sizeof(T) + p->name_len);
    }
}

static inline int div_round_up(int n, int m) {
    return (n + m - 1) / m;
}

static inline int round_up(int n, int m) {
    return m * div_round_up(n, m);
}

static inline bool aligned(const void *ptr, int a) {
    return 0 == ((long)ptr & (a-1));
}

#endif

