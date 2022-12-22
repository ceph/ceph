/*
 * file:        objname.h
 * description: string-like structure for handling object names
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#ifndef __OBJNAME_H__
#define __OBJNAME_H__

class objname {
    char buf[128];
public:
    objname() {}
    objname(const char *prefix, uint32_t seq) {
        init(prefix, seq);
    }
    void init(const char *prefix, uint32_t seq) {
        size_t len = strlen(prefix);
        assert(len + 9 < sizeof(buf));
        memcpy(buf, prefix, len);
        sprintf(buf + len, ".%08x", seq);
    }
    const char *c_str() {
        return buf;
    }
};
        
#endif
