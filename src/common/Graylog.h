// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_GRAYLOG_H
#define __CEPH_LOG_GRAYLOG_H


#include <memory>

#include <boost/asio.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#include "include/memory.h"
#include "include/assert.h"  // boost clobbers this

struct uuid_d;
class LogEntry;

namespace ceph {

class Formatter;

namespace log {

struct Entry;
class SubsystemMap;

// Graylog logging backend: Convert log datastructures (LogEntry, Entry) to
// GELF (http://www.graylog2.org/resources/gelf/specification) and send it
// to a GELF UDP receiver

class Graylog
{
 public:

  /**
   * Create Graylog with SubsystemMap. log_entry will resolve the subsystem
   * id to string. Logging will not be ready until set_destination is called
   * @param s SubsystemMap
   * @param logger Value for key "_logger" in GELF
   */
  Graylog(const SubsystemMap * const s, std::string logger);

  /**
   * Create Graylog without SubsystemMap. Logging will not be ready
   * until set_destination is called
   * @param logger Value for key "_logger" in GELF
   */
  explicit Graylog(std::string logger);
  virtual ~Graylog();

  void set_hostname(const std::string& host);
  void set_fsid(const uuid_d& fsid);

  void set_destination(const std::string& host, int port);

  void log_entry(Entry const * const e);
  void log_log_entry(LogEntry const * const e);

  typedef ceph::shared_ptr<Graylog> Ref;

 private:
  SubsystemMap const * const m_subs;

  bool m_log_dst_valid;

  std::string m_hostname;
  std::string m_fsid;
  std::string m_logger;

  boost::asio::ip::udp::endpoint m_endpoint;
  boost::asio::io_service m_io_service;

  std::unique_ptr<Formatter> m_formatter;
  std::unique_ptr<Formatter> m_formatter_section;
  std::stringstream m_ostream_section;
  std::stringstream m_ostream_compressed;
  boost::iostreams::filtering_ostream m_ostream;
  boost::iostreams::zlib_compressor m_compressor;

};

}
}

#endif
