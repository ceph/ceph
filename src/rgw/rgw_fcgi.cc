// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_fcgi.h"

#include "acconfig.h"
#ifdef FASTCGI_INCLUDE_DIR
# include "fastcgi/fcgiapp.h"
#else
# include "fcgiapp.h"
#endif


int RGWFCGX::write_data(const char *buf, int len)
{
  return FCGX_PutStr(buf, len, fcgx->out);
}

int RGWFCGX::read_data(char *buf, int len)
{
  return FCGX_GetStr(buf, len, fcgx->in);
}

void RGWFCGX::flush()
{
  FCGX_FFlush(fcgx->out);
}

void RGWFCGX::init_env(CephContext *cct)
{
  env.init(cct, (char **)fcgx->envp);
}

int RGWFCGX::send_status(const char *status, const char *status_name)
{
  status_num = atoi(status);
  return print("Status: %s %s\r\n", status, status_name);
}

int RGWFCGX::send_100_continue()
{
  int r = send_status("100", "Continue");
  if (r >= 0) {
    flush();
  }
  return r;
}

int RGWFCGX::send_content_length(uint64_t len)
{
  /*
   * Status 204 should not include a content-length header
   * RFC7230 says so
   */
  if (status_num == 204)
    return 0;

  char buf[21];
  snprintf(buf, sizeof(buf), "%" PRIu64, len);
  return print("Content-Length: %s\r\n", buf);
}

int RGWFCGX::complete_header()
{
  return print("\r\n");
}
