// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Log.h"

#include <errno.h>
#include <syslog.h>

#include <iostream>
#include <sstream>
#include <memory>

#include <boost/lexical_cast.hpp>
#include <boost/asio.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#include "common/errno.h"
#include "common/safe_io.h"
#include "common/Clock.h"
#include "common/Formatter.h"
#include "include/assert.h"
#include "include/compat.h"
#include "include/on_exit.h"
#include "include/uuid.h"

#define DEFAULT_MAX_NEW    100
#define DEFAULT_MAX_RECENT 10000

#define PREALLOC 1000000

namespace ceph {
namespace log {

static OnExitManager exit_callbacks;

static void log_on_exit(void *p)
{
  Log *l = *(Log **)p;
  if (l)
    l->flush();
  delete (Log **)p;// Delete allocated pointer (not Log object, the pointer only!)
}

Log::Log(SubsystemMap *s)
  : m_indirect_this(NULL),
    m_subs(s),
    m_queue_mutex_holder(0),
    m_flush_mutex_holder(0),
    m_new(), m_recent(),
    m_fd(-1),
    m_syslog_log(-2), m_syslog_crash(-2),
    m_stderr_log(1), m_stderr_crash(-1),
    m_graylog_log(-3), m_graylog_crash(-3),
    m_host(""), m_fsid(),
    m_graylog_endpoint(),
    m_graylog_io_service(),
    m_stop(false),
    m_max_new(DEFAULT_MAX_NEW),
    m_max_recent(DEFAULT_MAX_RECENT),
    m_inject_segv(false)
{
  int ret;

  ret = pthread_mutex_init(&m_flush_mutex, NULL);
  assert(ret == 0);

  ret = pthread_mutex_init(&m_queue_mutex, NULL);
  assert(ret == 0);

  ret = pthread_cond_init(&m_cond_loggers, NULL);
  assert(ret == 0);

  ret = pthread_cond_init(&m_cond_flusher, NULL);
  assert(ret == 0);

  // kludge for prealloc testing
  if (false)
    for (int i=0; i < PREALLOC; i++)
      m_recent.enqueue(new Entry);
}

Log::~Log()
{
  if (m_indirect_this) {
    *m_indirect_this = NULL;
  }

  assert(!is_started());
  if (m_fd >= 0)
    VOID_TEMP_FAILURE_RETRY(::close(m_fd));

  pthread_mutex_destroy(&m_queue_mutex);
  pthread_mutex_destroy(&m_flush_mutex);
  pthread_cond_destroy(&m_cond_loggers);
  pthread_cond_destroy(&m_cond_flusher);
}


///

void Log::set_flush_on_exit()
{
  // Make sure we flush on shutdown.  We do this by deliberately
  // leaking an indirect pointer to ourselves (on_exit() can't
  // unregister a callback).  This is not racy only becuase we
  // assume that exit() won't race with ~Log().
  if (m_indirect_this == NULL) {
    m_indirect_this = new (Log*)(this);
    exit_callbacks.add_callback(log_on_exit, m_indirect_this);
  }
}

void Log::set_max_new(int n)
{
  m_max_new = n;
}

void Log::set_max_recent(int n)
{
  m_max_recent = n;
}

void Log::set_log_file(string fn)
{
  m_log_file = fn;
}

void Log::reopen_log_file()
{
  if (m_fd >= 0)
    VOID_TEMP_FAILURE_RETRY(::close(m_fd));
  if (m_log_file.length()) {
    m_fd = ::open(m_log_file.c_str(), O_CREAT|O_WRONLY|O_APPEND, 0644);
  } else {
    m_fd = -1;
  }
}

void Log::set_syslog_level(int log, int crash)
{
  pthread_mutex_lock(&m_flush_mutex);
  m_syslog_log = log;
  m_syslog_crash = crash;
  pthread_mutex_unlock(&m_flush_mutex);
}

void Log::set_stderr_level(int log, int crash)
{
  pthread_mutex_lock(&m_flush_mutex);
  m_stderr_log = log;
  m_stderr_crash = crash;
  pthread_mutex_unlock(&m_flush_mutex);
}

void Log::set_graylog_level(int log, int crash)
{
  pthread_mutex_lock(&m_flush_mutex);
  m_graylog_log = log;
  m_graylog_crash = crash;
  pthread_mutex_unlock(&m_flush_mutex);
}

void Log::set_host(string host)
{
  m_host = host;
}

void Log::set_fsid(uuid_d fsid)
{
  m_fsid = fsid;
}

void Log::set_graylog_destination(string host, int port)
{
  boost::asio::ip::udp::resolver resolver(m_graylog_io_service);
  boost::asio::ip::udp::resolver::query query(host,
					      boost::lexical_cast<string>(port));
  m_graylog_endpoint = *resolver.resolve(query);
}

void Log::submit_entry(Entry *e)
{
  pthread_mutex_lock(&m_queue_mutex);
  m_queue_mutex_holder = pthread_self();

  if (m_inject_segv)
    *(int *)(0) = 0xdead;

  // wait for flush to catch up
  while (m_new.m_len > m_max_new)
    pthread_cond_wait(&m_cond_loggers, &m_queue_mutex);

  m_new.enqueue(e);
  pthread_cond_signal(&m_cond_flusher);
  m_queue_mutex_holder = 0;
  pthread_mutex_unlock(&m_queue_mutex);
}

Entry *Log::create_entry(int level, int subsys)
{
  if (true) {
    return new Entry(ceph_clock_now(NULL),
		   pthread_self(),
		   level, subsys);
  } else {
    // kludge for perf testing
    Entry *e = m_recent.dequeue();
    e->m_stamp = ceph_clock_now(NULL);
    e->m_thread = pthread_self();
    e->m_prio = level;
    e->m_subsys = subsys;
    return e;
  }
}

void Log::flush()
{
  pthread_mutex_lock(&m_flush_mutex);
  m_flush_mutex_holder = pthread_self();
  pthread_mutex_lock(&m_queue_mutex);
  m_queue_mutex_holder = pthread_self();
  EntryQueue t;
  t.swap(m_new);
  pthread_cond_broadcast(&m_cond_loggers);
  m_queue_mutex_holder = 0;
  pthread_mutex_unlock(&m_queue_mutex);
  _flush(&t, &m_recent, false);

  // trim
  while (m_recent.m_len > m_max_recent) {
    delete m_recent.dequeue();
  }

  m_flush_mutex_holder = 0;
  pthread_mutex_unlock(&m_flush_mutex);
}

void Log::_flush(EntryQueue *t, EntryQueue *requeue, bool crash)
{
  Entry *e;
  char buf[80];
  while ((e = t->dequeue()) != NULL) {
    unsigned sub = e->m_subsys;

    bool should_log = crash || m_subs->get_log_level(sub) >= e->m_prio;
    bool do_fd = m_fd >= 0 && should_log;
    bool do_syslog = m_syslog_crash >= e->m_prio && should_log;
    bool do_stderr = m_stderr_crash >= e->m_prio && should_log;
    bool do_graylog2 = m_graylog_crash >= e->m_prio && should_log;

    if (do_fd || do_syslog || do_stderr) {
      int buflen = 0;

      if (crash)
	buflen += snprintf(buf, sizeof(buf), "%6d> ", -t->m_len);
      buflen += e->m_stamp.sprintf(buf + buflen, sizeof(buf)-buflen);
      buflen += snprintf(buf + buflen, sizeof(buf)-buflen, " %lx %2d ",
			(unsigned long)e->m_thread, e->m_prio);

      // FIXME: this is slow
      string s = e->get_str();

      if (do_fd) {
	int r = safe_write(m_fd, buf, buflen);
	if (r >= 0)
	  r = safe_write(m_fd, s.data(), s.size());
	if (r >= 0)
	  r = write(m_fd, "\n", 1);
	if (r < 0)
	  cerr << "problem writing to " << m_log_file << ": " << cpp_strerror(r) << std::endl;
      }

      if (do_syslog) {
	syslog(LOG_USER, "%s%s", buf, s.c_str());
      }

      if (do_stderr) {
	cerr << buf << s << std::endl;
      }

      if (do_graylog2) {
	std::auto_ptr<Formatter> f(Formatter::create("json"));

	char fsid_str[40];
	m_fsid.print(fsid_str);

	// GELF format
	// http://www.graylog2.org/resources/gelf/specification
	f->open_object_section("");
	f->dump_string("version", "1.1");
	f->dump_string("host", m_host);
	f->dump_string("short_message", s);
	f->dump_string("_app", "ceph");
	f->dump_float("timestamp", e->m_stamp.sec() + (e->m_stamp.usec() / 1000000.0));
	f->dump_int("_thread", e->m_thread);
	f->dump_int("_prio", e->m_prio);
	f->dump_string("_subsys_name", m_subs->get_name(sub));
	f->dump_int("_subsys_id", sub);
	f->dump_int("_crash", crash);
	f->dump_string("_fsid", fsid_str);
	if (crash) f->dump_int("_crash_entry_queue_len", -t->m_len);
	f->close_section();

	std::stringstream zos(std::stringstream::in |
			      std::stringstream::out |
			      std::stringstream::binary);

	{
	  boost::iostreams::filtering_ostream os;
	  os.push(boost::iostreams::zlib_compressor());
	  os.push(zos);

	  f->flush(os);
	  os << std::endl;
	}

	try {
	  boost::asio::ip::udp::socket socket(m_graylog_io_service);
	  socket.open(m_graylog_endpoint.protocol());
	  socket.send_to(boost::asio::buffer(zos.str()), m_graylog_endpoint);
	} catch (boost::system::system_error const& e) {
	  /* The above code fails until the configuration is set */
	}
      }
    }
    requeue->enqueue(e);
  }
}

void Log::_log_message(const char *s, bool crash)
{
  if (m_fd >= 0) {
    int r = safe_write(m_fd, s, strlen(s));
    if (r >= 0)
      r = safe_write(m_fd, "\n", 1);
    if (r < 0)
      cerr << "problem writing to " << m_log_file << ": " << cpp_strerror(r) << std::endl;
  }
  if ((crash ? m_syslog_crash : m_syslog_log) >= 0) {
    syslog(LOG_USER, "%s", s);
  }
  
  if ((crash ? m_stderr_crash : m_stderr_log) >= 0) {
    cerr << s << std::endl;
  }
}

void Log::dump_recent()
{
  pthread_mutex_lock(&m_flush_mutex);
  m_flush_mutex_holder = pthread_self();

  pthread_mutex_lock(&m_queue_mutex);
  m_queue_mutex_holder = pthread_self();

  EntryQueue t;
  t.swap(m_new);

  m_queue_mutex_holder = 0;
  pthread_mutex_unlock(&m_queue_mutex);
  _flush(&t, &m_recent, false);

  EntryQueue old;
  _log_message("--- begin dump of recent events ---", true);
  _flush(&m_recent, &old, true);  

  char buf[4096];
  _log_message("--- logging levels ---", true);
  for (vector<Subsystem>::iterator p = m_subs->m_subsys.begin();
       p != m_subs->m_subsys.end();
       ++p) {
    snprintf(buf, sizeof(buf), "  %2d/%2d %s", p->log_level, p->gather_level, p->name.c_str());
    _log_message(buf, true);
  }

  sprintf(buf, "  %2d/%2d (syslog threshold)", m_syslog_log, m_syslog_crash);
  _log_message(buf, true);
  sprintf(buf, "  %2d/%2d (stderr threshold)", m_stderr_log, m_stderr_crash);
  _log_message(buf, true);
  sprintf(buf, "  max_recent %9d", m_max_recent);
  _log_message(buf, true);
  sprintf(buf, "  max_new    %9d", m_max_new);
  _log_message(buf, true);
  sprintf(buf, "  log_file %s", m_log_file.c_str());
  _log_message(buf, true);

  _log_message("--- end dump of recent events ---", true);

  m_flush_mutex_holder = 0;
  pthread_mutex_unlock(&m_flush_mutex);
}

void Log::start()
{
  assert(!is_started());
  pthread_mutex_lock(&m_queue_mutex);
  m_stop = false;
  pthread_mutex_unlock(&m_queue_mutex);
  create();
}

void Log::stop()
{
  assert(is_started());
  pthread_mutex_lock(&m_queue_mutex);
  m_stop = true;
  pthread_cond_signal(&m_cond_flusher);
  pthread_cond_broadcast(&m_cond_loggers);
  pthread_mutex_unlock(&m_queue_mutex);
  join();
}

void *Log::entry()
{
  pthread_mutex_lock(&m_queue_mutex);
  m_queue_mutex_holder = pthread_self();
  while (!m_stop) {
    if (!m_new.empty()) {
      m_queue_mutex_holder = 0;
      pthread_mutex_unlock(&m_queue_mutex);
      flush();
      pthread_mutex_lock(&m_queue_mutex);
      m_queue_mutex_holder = pthread_self();
      continue;
    }

    pthread_cond_wait(&m_cond_flusher, &m_queue_mutex);
  }
  m_queue_mutex_holder = 0;
  pthread_mutex_unlock(&m_queue_mutex);
  flush();
  return NULL;
}

bool Log::is_inside_log_lock()
{
  return
    pthread_self() == m_queue_mutex_holder ||
    pthread_self() == m_flush_mutex_holder;
}

void Log::inject_segv()
{
  m_inject_segv = true;
}

} // ceph::log::
} // ceph::
