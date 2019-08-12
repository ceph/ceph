// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

class HBHandle {
public:
  virtual void reset_tp_timeout() = 0;
  virtual void suspend_tp_timeout() = 0;
  virtual ~HBHandle() {}
};
