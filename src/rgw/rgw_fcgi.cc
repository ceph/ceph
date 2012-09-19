

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

const char **RGWFCGX::envp()
{
  return (const char **)fcgx->envp;
}
