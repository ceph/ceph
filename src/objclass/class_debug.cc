#include "config.h"

#include "objclass/objclass.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

extern "C" int cls_log(const char *format, ...)
{
   int size = 256, n;
   va_list ap;
   while (1) {
     char buf[size];
     va_start(ap, format);
     n = vsnprintf(buf, size, format, ap);
     va_end(ap);
#define MAX_SIZE 8196
     if ((n > -1 && n < size) || size > MAX_SIZE) {
       dout(0) << buf << dendl;
       return n;
     }
     size *= 2;
   }
}
