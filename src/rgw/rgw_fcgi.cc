// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_fcgi.h"

#include "acconfig.h"

std::size_t RGWFCGX::write_data(const char* const buf, const std::size_t len)
{
  const auto ret = FCGX_PutStr(buf, len, fcgx->out);
  if (ret < 0) {
    throw RGWStreamIOEngine::Exception(ret);
  }
  return ret;
}

std::size_t RGWFCGX::read_data(char* const buf, const std::size_t len)
{
  const auto ret = FCGX_GetStr(buf, len, fcgx->in);
  if (ret < 0) {
    throw RGWStreamIOEngine::Exception(ret);
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

std::size_t RGWFCGX::send_status(const int status, const char* const status_name)
{
  static constexpr size_t STATUS_BUF_SIZE = 128;

  char statusbuf[STATUS_BUF_SIZE];
  const auto statuslen = snprintf(statusbuf, sizeof(statusbuf),
                                  "Status: %d %s\r\n", status, status_name);

  return write_data(statusbuf, statuslen);
}

std::size_t RGWFCGX::send_100_continue()
{
  return send_status(100, "Continue");
}

std::size_t RGWFCGX::send_content_length(const uint64_t len)
{
  static constexpr size_t CONLEN_BUF_SIZE = 128;

  char sizebuf[CONLEN_BUF_SIZE];
  const auto sizelen = snprintf(sizebuf, sizeof(sizebuf),
                                "Content-Length: %" PRIu64 "\r\n", len);

  return write_data(sizebuf, sizelen);
}

std::size_t RGWFCGX::complete_header()
{
  constexpr char HEADER_END[] = "\r\n";
  return write_data(HEADER_END, sizeof(HEADER_END) - 1);
}
