/*
 * file:        nvme.h
 * description: interface for request-driven interface to local SSD
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#ifndef NVME_H
#define NVME_H

class nvme {
public:
    nvme() {};
    virtual ~nvme() {};

    virtual int read(void *buf, size_t count, off_t offset) = 0;
    virtual int write(const void *buf, size_t count, off_t offset) = 0;

    virtual int writev(const struct iovec *iov, int iovcnt, off_t offset) = 0;
    virtual int readv(const struct iovec *iov, int iovcnt, off_t offset) = 0;

    virtual request* make_write_request(smartiov *iov, size_t offset) = 0;
    virtual request* make_write_request(char *buf, size_t len, size_t offset) = 0;
    virtual request* make_read_request(smartiov *iov, size_t offset) = 0;

    virtual request* make_read_request(char *buf, size_t len, size_t offset) = 0;
};

nvme *make_nvme(int fd, const char* name);

#endif
