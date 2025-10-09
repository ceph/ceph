// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

class HBHandle {
public:
  virtual void reset_tp_timeout() = 0;
  virtual void suspend_tp_timeout() = 0;
  virtual ~HBHandle() {}
};
