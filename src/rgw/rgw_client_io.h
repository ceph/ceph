#ifndef CEPH_RGW_CLIENT_IO_H
#define CEPH_RGW_CLIENT_IO_H

#include <stdlib.h>

class RGWClientIO {
  bool account;

  size_t bytes_sent;
  size_t bytes_received;

protected:
  virtual int write_data(const char *buf, int len) = 0;
  virtual int read_data(char *buf, int max) = 0;

public:
  virtual ~RGWClientIO() {}
  RGWClientIO() : account(false), bytes_sent(0), bytes_received(0) {}

  int print(const char *format, ...);
  int write(const char *buf, int len);
  virtual void flush() = 0;
  int read(char *buf, int max, int *actual);

  virtual const char **envp() = 0;

  void set_account(bool _account) {
    account = _account;
  }
};

#endif
