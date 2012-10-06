// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef STATCOLLECTORH
#define STATCOLLECTORH

#include <stdint.h>

class StatCollector {
public:
  virtual uint64_t next_seq() = 0;
  virtual void start_write(uint64_t seq, uint64_t size) = 0;
  virtual void start_read(uint64_t seq, uint64_t size) = 0;
  virtual void write_applied(uint64_t seq) = 0;
  virtual void write_committed(uint64_t seq) = 0;
  virtual void read_complete(uint64_t seq) = 0;
  virtual ~StatCollector() {}
};

#endif
