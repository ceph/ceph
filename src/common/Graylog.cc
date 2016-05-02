// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Graylog.h"

#include <iostream>
#include <sstream>

#include <arpa/inet.h>

#include "common/Formatter.h"
#include "common/LogEntry.h"
#include "log/Entry.h"
#include "log/SubsystemMap.h"
#include "include/uuid.h"

namespace ceph {
namespace log {

Graylog::Graylog(const SubsystemMap * const s, std::string logger)
    : m_subs(s),
      m_log_dst_valid(false),
      m_hostname(""),
      m_fsid(""),
      m_logger(logger),
      m_ostream_compressed(std::stringstream::in |
                           std::stringstream::out |
                           std::stringstream::binary)
{
  m_formatter = std::unique_ptr<Formatter>(Formatter::create("json"));
  m_formatter_section = std::unique_ptr<Formatter>(Formatter::create("json"));
}

Graylog::Graylog(std::string logger)
    : m_subs(NULL),
      m_log_dst_valid(false),
      m_hostname(""),
      m_fsid(""),
      m_logger(logger),
      m_ostream_compressed(std::stringstream::in |
                           std::stringstream::out |
                           std::stringstream::binary)
{
  m_formatter = std::unique_ptr<Formatter>(Formatter::create("json"));
  m_formatter_section = std::unique_ptr<Formatter>(Formatter::create("json"));
}

Graylog::~Graylog()
{
}

void Graylog::set_destination(const std::string& host, int port)
{
  try {
    boost::asio::ip::udp::resolver resolver(m_io_service);
    boost::asio::ip::udp::resolver::query query(host, std::to_string(port));
    m_endpoint = *resolver.resolve(query);
    m_log_dst_valid = true;
  } catch (boost::system::system_error const& e) {
    cerr << "Error resolving graylog destination: " << e.what() << std::endl;
    m_log_dst_valid = false;
  }
}

void Graylog::set_hostname(const std::string& host)
{
  m_hostname = host;
}

void Graylog::set_fsid(const uuid_d& fsid)
{
  std::vector<char> buf(40);
  fsid.print(&buf[0]);
  m_fsid = std::string(&buf[0]);
}

void Graylog::log_entry(Entry const * const e)
{
  if (m_log_dst_valid) {
    std::string s = e->get_str();

    m_formatter->open_object_section("");
    m_formatter->dump_string("version", "1.1");
    m_formatter->dump_string("host", m_hostname);
    m_formatter->dump_string("short_message", s);
    m_formatter->dump_string("_app", "ceph");
    m_formatter->dump_float("timestamp", e->m_stamp.sec() + (e->m_stamp.usec() / 1000000.0));
    m_formatter->dump_int("_thread", e->m_thread);
    m_formatter->dump_int("_level", e->m_prio);
    if (m_subs != NULL)
    m_formatter->dump_string("_subsys_name", m_subs->get_name(e->m_subsys));
    m_formatter->dump_int("_subsys_id", e->m_subsys);
    m_formatter->dump_string("_fsid", m_fsid);
    m_formatter->dump_string("_logger", m_logger);
    m_formatter->close_section();

    m_ostream_compressed.clear();
    m_ostream_compressed.str("");

    m_ostream.reset();

    m_ostream.push(m_compressor);
    m_ostream.push(m_ostream_compressed);

    m_formatter->flush(m_ostream);
    m_ostream << std::endl;

    m_ostream.reset();

    try {
      boost::asio::ip::udp::socket socket(m_io_service);
      socket.open(m_endpoint.protocol());
      socket.send_to(boost::asio::buffer(m_ostream_compressed.str()), m_endpoint);
    } catch (boost::system::system_error const& e) {
      cerr << "Error sending graylog message: " << e.what() << std::endl;
    }
  }
}

void Graylog::log_log_entry(LogEntry const * const e)
{
  if (m_log_dst_valid) {
    m_formatter->open_object_section("");
    m_formatter->dump_string("version", "1.1");
    m_formatter->dump_string("host", m_hostname);
    m_formatter->dump_string("short_message", e->msg);
    m_formatter->dump_float("timestamp", e->stamp.sec() + (e->stamp.usec() / 1000000.0));
    m_formatter->dump_string("_app", "ceph");

    m_formatter_section->open_object_section("");
    e->who.addr.dump(m_formatter_section.get());
    e->who.name.dump(m_formatter_section.get());
    m_formatter_section->close_section();

    m_ostream_section.clear();
    m_ostream_section.str("");
    m_formatter_section->flush(m_ostream_section);
    m_formatter->dump_string("_who", m_ostream_section.str());

    m_formatter->dump_int("_seq", e->seq);
    m_formatter->dump_string("_prio", clog_type_to_string(e->prio));
    m_formatter->dump_string("_channel", e->channel);
    m_formatter->dump_string("_fsid", m_fsid);
    m_formatter->dump_string("_logger", m_logger);
    m_formatter->close_section();

    m_ostream_compressed.clear();
    m_ostream_compressed.str("");

    m_ostream.reset();

    m_ostream.push(m_compressor);
    m_ostream.push(m_ostream_compressed);

    m_formatter->flush(m_ostream);
    m_ostream << std::endl;

    m_ostream.reset();

    try {
      boost::asio::ip::udp::socket socket(m_io_service);
      socket.open(m_endpoint.protocol());
      socket.send_to(boost::asio::buffer(m_ostream_compressed.str()), m_endpoint);
    } catch (boost::system::system_error const& e) {
      cerr << "Error sending graylog message: " << e.what() << std::endl;
    }
  }
}

} // ceph::log::
} // ceph::
