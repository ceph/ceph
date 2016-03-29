// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_GRAYLOG_H
#define __CEPH_LOG_GRAYLOG_H


#include <memory>

#include <boost/thread/mutex.hpp>
#include <boost/asio.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#include "Entry.h"
#include "SubsystemMap.h"

namespace ceph {
namespace log {

class Graylog
{
 public:

  explicit Graylog(const SubsystemMap * const s);
  virtual ~Graylog();

  void set_hostname(const std::string& host);
  void set_fsid(uuid_d fsid);

  void set_destination(const std::string& host, int port);

  void log_entry(Entry const * const e);

  typedef ceph::shared_ptr<Graylog> Ref;

 private:
  SubsystemMap const * const m_subs;

  bool m_log_dst_valid;

  std::string m_hostname;
  std::string m_fsid;

  boost::asio::ip::udp::endpoint m_endpoint;
  boost::asio::io_service m_io_service;

  std::auto_ptr<Formatter> m_formatter;
  std::stringstream m_ostream_compressed;
  boost::iostreams::filtering_ostream m_ostream;
  boost::iostreams::zlib_compressor m_compressor;
};

}
}

#endif
