// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sstream>

typedef enum {
  CLOG_DEBUG = 0,
  CLOG_INFO = 1,
  CLOG_SEC = 2,
  CLOG_WARN = 3,
  CLOG_ERROR = 4,
  CLOG_UNKNOWN = -1,
} clog_type;

class OstreamTemp
{
public:
  class OstreamTempSink {
  public:
    virtual void do_log(clog_type prio, std::stringstream& ss) = 0;
    virtual ~OstreamTempSink() {}
  };
  OstreamTemp(clog_type type_, OstreamTempSink *parent_);
  OstreamTemp(OstreamTemp &&rhs) = default;
  ~OstreamTemp();

  template<typename T>
  std::ostream& operator<<(const T& rhs)
  {
    return ss << rhs;
  }

private:
  clog_type type;
  OstreamTempSink *parent;
  std::stringstream ss;
};

class LoggerSinkSet : public OstreamTemp::OstreamTempSink {
public:
  virtual void info(std::stringstream &s) = 0;
  virtual void warn(std::stringstream &s) = 0;
  virtual void error(std::stringstream &s) = 0;
  virtual void sec(std::stringstream &s) = 0;
  virtual void debug(std::stringstream &s) = 0;
  virtual OstreamTemp info() = 0;
  virtual OstreamTemp warn() = 0;
  virtual OstreamTemp error() = 0;
  virtual OstreamTemp sec() = 0;
  virtual OstreamTemp debug() = 0;
  virtual void do_log(clog_type prio, std::stringstream& ss) = 0;
  virtual void do_log(clog_type prio, const std::string& ss) = 0;
  virtual ~LoggerSinkSet() {};
};
