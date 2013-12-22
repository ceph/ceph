
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#include "rgw_client_io.h"


int RGWClientIO::print(const char *format, ...)
{
#define LARGE_ENOUGH 128
  int size = LARGE_ENOUGH;

  va_list ap;

  while(1) {
    char buf[size];
    va_start(ap, format);
    int ret = vsnprintf(buf, size, format, ap);
    va_end(ap);

    if (ret >= 0 && ret < size) {
      return write(buf, ret);
    }

    if (ret >= 0)
      size = ret + 1;
    else
      size *= 2;
  }

  /* not reachable */
}

int RGWClientIO::write(const char *buf, int len)
{
  int ret = write_data(buf, len);
  if (ret < 0)
    return ret;

  if (account)
    bytes_sent += len;

  return 0;
}


int RGWClientIO::read(char *buf, int max, int *actual)
{
  int ret = read_data(buf, max);
  if (ret < 0)
    return ret;

  *actual = ret;

  bytes_received += *actual;

  return 0;
}

