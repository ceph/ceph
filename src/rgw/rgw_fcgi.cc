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

void RGWFCGX::flush(RGWClientIO& controller)
{
  FCGX_FFlush(fcgx->out);
}

void RGWFCGX::init_env(CephContext *cct)
{
  env.init(cct, (char **)fcgx->envp);
}

int RGWFCGX::send_status(RGWClientIO& controller,
                         const char * const status,
                         const char * const status_name)
{
  return controller.print("Status: %s %s\r\n", status, status_name);
}

int RGWFCGX::send_100_continue(RGWClientIO& controller)
{
  int r = send_status(controller, "100", "Continue");
  if (r >= 0) {
    flush(controller);
  }
  return r;
}

int RGWFCGX::send_content_length(RGWClientIO& controller,
                                 const uint64_t len)
{
  char buf[21];
  snprintf(buf, sizeof(buf), "%" PRIu64, len);
  return controller.print("Content-Length: %s\r\n", buf);
}

int RGWFCGX::complete_header(RGWClientIO& controller)
{
  return controller.print("\r\n");
}
