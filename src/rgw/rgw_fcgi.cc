// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_fcgi.h"
#include "acconfig.h"

size_t RGWFCGX::write_data(const char* const buf, const size_t len)
{
  const auto ret = FCGX_PutStr(buf, len, fcgx->out);
  if (ret < 0) {
    throw RGWRestfulIOEngine::Exception(-ret, std::system_category());
  }
  return ret;
}

size_t RGWFCGX::read_data(char* const buf, const size_t len)
{
  const auto ret = FCGX_GetStr(buf, len, fcgx->in);
  if (ret < 0) {
    throw RGWRestfulIOEngine::Exception(-ret, std::system_category());
  }
  return ret;
}

void RGWFCGX::flush()
{
  FCGX_FFlush(fcgx->out);
}

void RGWFCGX::init_env(CephContext* const cct)
{
  env.init(cct, (char **)fcgx->envp);
}

size_t RGWFCGX::send_status(const int status, const char* const status_name)
{
  static constexpr size_t STATUS_BUF_SIZE = 128;

  char statusbuf[STATUS_BUF_SIZE];
  const auto statuslen = snprintf(statusbuf, sizeof(statusbuf),
                                  "Status: %d %s\r\n", status, status_name);

  return write_data(statusbuf, statuslen);
}

size_t RGWFCGX::send_100_continue()
{
  const auto sent = send_status(100, "Continue");
  flush();
  return sent;
}

size_t RGWFCGX::send_header(const boost::string_ref& name,
                            const boost::string_ref& value)
{
  char hdrbuf[name.size() + 2 + value.size() + 2 + 1];
  const auto hdrlen = snprintf(hdrbuf, sizeof(hdrbuf),
                               "%.*s: %.*s\r\n",
                               static_cast<int>(name.length()),
                               name.data(),
                               static_cast<int>(value.length()),
                               value.data());

  return write_data(hdrbuf, hdrlen);
}

size_t RGWFCGX::send_content_length(const uint64_t len)
{
  static constexpr size_t CONLEN_BUF_SIZE = 128;

  char sizebuf[CONLEN_BUF_SIZE];
  const auto sizelen = snprintf(sizebuf, sizeof(sizebuf),
                                "Content-Length: %" PRIu64 "\r\n", len);

  return write_data(sizebuf, sizelen);
}

size_t RGWFCGX::complete_header()
{
  static constexpr char HEADER_END[] = "\r\n";
  return write_data(HEADER_END, sizeof(HEADER_END) - 1);
}
