// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Journald.h"

#include <endian.h>
#include <fcntl.h>
#include <iterator>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <syslog.h>
#include <unistd.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "include/ceph_assert.h"
#include "common/LogEntry.h"
#include "log/Entry.h"
#include "log/SubsystemMap.h"
#include "msg/msg_fmt.h"


namespace ceph::logging {

namespace {
const struct sockaddr_un sockaddr = {
  AF_UNIX,
  "/run/systemd/journal/socket",
};

ssize_t sendmsg_fd(int transport_fd, int fd)
{
  constexpr size_t control_len = CMSG_LEN(sizeof(int));
  char control[control_len];
  struct msghdr mh = {
    (struct sockaddr*)&sockaddr, // msg_name
    sizeof(sockaddr),            // msg_namelen
    nullptr,                     // msg_iov
    0,                           // msg_iovlen
    &control,                    // msg_control
    control_len,                 // msg_controllen
  };
  ceph_assert(transport_fd >= 0);

  struct cmsghdr *cmsg = CMSG_FIRSTHDR(&mh);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int));
  *reinterpret_cast<int *>(CMSG_DATA(cmsg)) = fd;

  return sendmsg(transport_fd, &mh, MSG_NOSIGNAL);
}

char map_prio(short ceph_prio)
{
  if (ceph_prio < 0)
    return LOG_ERR;
  if (ceph_prio == 0)
    return LOG_WARNING;
  if (ceph_prio < 5)
    return LOG_NOTICE;
  if (ceph_prio < 10)
    return LOG_INFO;
  return LOG_DEBUG;
}
}

namespace detail {
class EntryEncoderBase {
 public:
  EntryEncoderBase():
    m_msg_vec {
     {}, {}, {}, { (char *)"\n", 1 },
    }
  {
    std::string id = program_invocation_short_name;
    for (auto& c : id) {
      if (c == '\n')
        c = '_';
    }
    static_segment = "SYSLOG_IDENTIFIER=" + id + "\n";
    m_msg_vec[0].iov_base = static_segment.data();
    m_msg_vec[0].iov_len = static_segment.size();
  }

  constexpr struct iovec *iovec() { return this->m_msg_vec; }
  constexpr std::size_t iovec_len()
  {
    return sizeof(m_msg_vec) / sizeof(m_msg_vec[0]);
  }

 private:
  struct iovec m_msg_vec[4];
  std::string static_segment;

 protected:
  fmt::memory_buffer meta_buf;

  struct iovec &meta_vec() { return m_msg_vec[1]; }
  struct iovec &msg_vec() { return m_msg_vec[2]; }
};

class EntryEncoder : public EntryEncoderBase {
 public:
  void encode(const Entry& e, const SubsystemMap *s)
  {
    meta_buf.clear();
    fmt::format_to(std::back_inserter(meta_buf),
      R"(PRIORITY={:d}
CEPH_SUBSYS={}
TIMESTAMP={}
CEPH_PRIO={}
THREAD={:016x}
MESSAGE
)",
      map_prio(e.m_prio),
      s->get_name(e.m_subsys),
      e.m_stamp.time_since_epoch().count().count,
      e.m_prio,
      e.m_thread);

    uint64_t msg_len = htole64(e.size());
    meta_buf.resize(meta_buf.size() + sizeof(msg_len));
    memcpy(meta_buf.end() - sizeof(msg_len), &msg_len, sizeof(msg_len));

    meta_vec().iov_base = meta_buf.data();
    meta_vec().iov_len = meta_buf.size();

    msg_vec().iov_base = (void *)e.strv().data();
    msg_vec().iov_len = e.size();
  }
};

class LogEntryEncoder : public EntryEncoderBase {
 public:
  void encode(const LogEntry& le)
  {
    meta_buf.clear();
    fmt::format_to(std::back_inserter(meta_buf),
      R"(PRIORITY={:d}
TIMESTAMP={}
CEPH_NAME={}
CEPH_RANK={}
CEPH_SEQ={}
CEPH_CHANNEL={}
MESSAGE
)",
      clog_type_to_syslog_level(le.prio),
      le.stamp.to_nsec(),
      le.name.to_str(),
      le.rank,
      le.seq,
      le.channel);

    uint64_t msg_len = htole64(le.msg.size());
    meta_buf.resize(meta_buf.size() + sizeof(msg_len));
    *(reinterpret_cast<uint64_t*>(meta_buf.end()) - 1) = htole64(le.msg.size());

    meta_vec().iov_base = meta_buf.data();
    meta_vec().iov_len = meta_buf.size();

    msg_vec().iov_base = (void *)le.msg.data();
    msg_vec().iov_len = le.msg.size();
  }
};

enum class JournaldClient::MemFileMode {
  MEMFD_CREATE,
  OPEN_TMPFILE,
  OPEN_UNLINK,  
};

constexpr const char *mem_file_dir = "/dev/shm";

void JournaldClient::detect_mem_file_mode()
{
  int memfd = memfd_create("ceph-journald", MFD_ALLOW_SEALING | MFD_CLOEXEC);
  if (memfd >= 0) {
    mem_file_mode = MemFileMode::MEMFD_CREATE;
    close(memfd);
    return;
  }
  memfd = open(mem_file_dir, O_TMPFILE | O_EXCL | O_CLOEXEC, S_IRUSR | S_IWUSR);
  if (memfd >= 0) {
    mem_file_mode = MemFileMode::OPEN_TMPFILE;
    close(memfd);
    return;
  }
  mem_file_mode = MemFileMode::OPEN_UNLINK;
}

int JournaldClient::open_mem_file()
{
  switch (mem_file_mode) {
  case MemFileMode::MEMFD_CREATE:
    return memfd_create("ceph-journald", MFD_ALLOW_SEALING | MFD_CLOEXEC);
  case MemFileMode::OPEN_TMPFILE:
    return open(mem_file_dir, O_TMPFILE | O_EXCL | O_CLOEXEC, S_IRUSR | S_IWUSR);
  case MemFileMode::OPEN_UNLINK:
    char mem_file_template[] = "/dev/shm/ceph-journald-XXXXXX";
    int fd = mkostemp(mem_file_template, O_CLOEXEC);
    unlink(mem_file_template);
    return fd;
  }
  ceph_abort("Unexpected mem_file_mode");
}

JournaldClient::JournaldClient() :
  m_msghdr({
    (struct sockaddr*)&sockaddr, // msg_name
    sizeof(sockaddr),            // msg_namelen
  })
{
  fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  ceph_assertf(fd > 0, "socket creation failed: %s", strerror(errno));

  int sendbuf = 2 * 1024 * 1024;
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sendbuf, sizeof(sendbuf));

  detect_mem_file_mode();
}

JournaldClient::~JournaldClient()
{
  close(fd);
}

int JournaldClient::send()
{
  int ret = sendmsg(fd, &m_msghdr, MSG_NOSIGNAL);
  if (ret >= 0)
    return 0;

  /* Fail silently if the journal is not available */
  if (errno == ENOENT)
    return -1;

  if (errno != EMSGSIZE && errno != ENOBUFS) {
    std::cerr << "Failed to send log to journald: " << strerror(errno) << std::endl;
    return -1;
  }
  /* Message doesn't fit... Let's dump the data in a memfd and
   * just pass a file descriptor of it to the other side.
   */
  int buffer_fd = open_mem_file();
  if (buffer_fd < 0) {
    std::cerr << "Failed to open buffer_fd while sending log to journald: " << strerror(errno) << std::endl;
    return -1;
  }

  ret = writev(buffer_fd, m_msghdr.msg_iov, m_msghdr.msg_iovlen);
  if (ret < 0) {
    std::cerr << "Failed to write to buffer_fd while sending log to journald: " << strerror(errno) << std::endl;
    goto err_close_buffer_fd;
  }

  if (mem_file_mode == MemFileMode::MEMFD_CREATE) {
    ret = fcntl(buffer_fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE | F_SEAL_SEAL);
    if (ret) {
      std::cerr << "Failed to seal buffer_fd while sending log to journald: " << strerror(errno) << std::endl;
      goto err_close_buffer_fd;
    }
  }
  
  ret = sendmsg_fd(fd, buffer_fd);
  if (ret < 0) {
    /* Fail silently if the journal is not available */
    if (errno == ENOENT)
      goto err_close_buffer_fd;

    std::cerr << "Failed to send fd while sending log to journald: " << strerror(errno) << std::endl;
    goto err_close_buffer_fd;
  }
  close(buffer_fd);
  return 0;

err_close_buffer_fd:
  close(buffer_fd);
  return -1;
}

} // namespace ceph::logging::detail

JournaldLogger::JournaldLogger(const SubsystemMap *s) :
  m_entry_encoder(std::make_unique<detail::EntryEncoder>()),
  m_subs(s)
{
  client.m_msghdr.msg_iov = m_entry_encoder->iovec();
  client.m_msghdr.msg_iovlen = m_entry_encoder->iovec_len();
}

JournaldLogger::~JournaldLogger() = default;

int JournaldLogger::log_entry(const Entry& e)
{
  m_entry_encoder->encode(e, m_subs);
  return client.send();
}

JournaldClusterLogger::JournaldClusterLogger() :
  m_log_entry_encoder(std::make_unique<detail::LogEntryEncoder>())
{
  client.m_msghdr.msg_iov = m_log_entry_encoder->iovec();
  client.m_msghdr.msg_iovlen = m_log_entry_encoder->iovec_len();
}

JournaldClusterLogger::~JournaldClusterLogger() = default;

int JournaldClusterLogger::log_log_entry(const LogEntry &le)
{
  m_log_entry_encoder->encode(le);
  return client.send();
}

}
