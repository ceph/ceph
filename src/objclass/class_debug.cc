// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/config.h"

#include "common/debug.h"
#include "objclass/objclass.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#include <iostream>

#define dout_subsys ceph_subsys_objclass

int cls_log(int level, const char *format, ...)
{
   int size = 256;
   va_list ap;
   while (1) {
     char buf[size];
     va_start(ap, format);
     int n = vsnprintf(buf, size, format, ap);
     va_end(ap);
#define MAX_SIZE 8196
     if ((n > -1 && n < size) || size > MAX_SIZE) {
       dout(level) << buf << dendl;
       return n;
     }
     size *= 2;
   }
}
