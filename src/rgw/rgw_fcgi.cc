// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_fcgi.h"
#include "acconfig.h"

size_t RGWFCGX::write_data(const char* const buf, const size_t len)
{
 /* According to the documentation of FCGX_PutStr if there is no error
 * (signalised by negative return value), then always ret == len. */
 const auto ret = FCGX_PutStr(buf, len, fcgx->out);
  if (ret < 0) {
    throw rgw::io::Exception(-ret, std::system_category());
  }
  return ret;
}

size_t RGWFCGX::read_data(char* const buf, const size_t len)
{
  const auto ret = FCGX_GetStr(buf, len, fcgx->in);
  if (ret < 0) {
    throw rgw::io::Exception(-ret, std::system_category());
  }
  return ret;
}

void RGWFCGX::flush()
{
  txbuf.pubsync();
  FCGX_FFlush(fcgx->out);
}

int RGWFCGX::init_env(CephContext* const cct)
{
  env.init(cct, (char **)fcgx->envp);
  return 0;
}

size_t RGWFCGX::send_status(const int status, const char* const status_name)
{
  static constexpr size_t STATUS_BUF_SIZE = 128;

  char statusbuf[STATUS_BUF_SIZE];
  const auto statuslen = snprintf(statusbuf, sizeof(statusbuf),
                                  "Status: %d %s\r\n", status, status_name);

  return txbuf.sputn(statusbuf, statuslen);
}

size_t RGWFCGX::send_100_continue()
{
  const auto sent = send_status(100, "Continue");
  flush();
  return sent;
}

size_t RGWFCGX::send_header(const std::string_view& name,
                            const std::string_view& value)
{
  static constexpr char HEADER_SEP[] = ": ";
  static constexpr char HEADER_END[] = "\r\n";

  size_t sent = 0;

  sent += txbuf.sputn(name.data(), name.length());
  sent += txbuf.sputn(HEADER_SEP, sizeof(HEADER_SEP) - 1);
  sent += txbuf.sputn(value.data(), value.length());
  sent += txbuf.sputn(HEADER_END, sizeof(HEADER_END) - 1);

  return sent;
}

size_t RGWFCGX::send_content_length(const uint64_t len)
{
  static constexpr size_t CONLEN_BUF_SIZE = 128;

  char sizebuf[CONLEN_BUF_SIZE];
  const auto sizelen = snprintf(sizebuf, sizeof(sizebuf),
                                "Content-Length: %" PRIu64 "\r\n", len);

  return txbuf.sputn(sizebuf, sizelen);
}

size_t RGWFCGX::complete_header()
{
  static constexpr char HEADER_END[] = "\r\n";
  const size_t sent = txbuf.sputn(HEADER_END, sizeof(HEADER_END) - 1);

  flush();
  return sent;
}
