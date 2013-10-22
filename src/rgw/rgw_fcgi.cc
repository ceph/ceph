

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
  return print("Status: %s\n", status);
}

int RGWFCGX::send_100_continue()
{
  int r = send_status("100", "Continue");
  if (r >= 0) {
    flush();
  }
  return r;
}

int RGWFCGX::complete_header()
{
  return print("\r\n");
}

