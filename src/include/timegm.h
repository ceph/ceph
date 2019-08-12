//  (C) Copyright Howard Hinnant
//  (C) Copyright 2010-2011 Vicente J. Botet Escriba
//  Use, modification and distribution are subject to the Boost Software License,
//  Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt).

//===-------------------------- locale ------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

// This code was adapted by Vicente from Howard Hinnant's experimental work
// on chrono i/o to Boost and some functions from libc++/locale to emulate the missing time_get::get()

#ifndef BOOST_CHRONO_IO_TIME_POINT_IO_H
#define BOOST_CHRONO_IO_TIME_POINT_IO_H

#include <time.h>

static int32_t is_leap(int32_t year) {
  if(year % 400 == 0)
    return 1;
  if(year % 100 == 0)
    return 0;
  if(year % 4 == 0)
    return 1;
  return 0;
}

static int32_t days_from_0(int32_t year) {
  year--;
  return 365 * year + (year / 400) - (year/100) + (year / 4);
}

int32_t static days_from_1970(int32_t year) {
  static const int days_from_0_to_1970 = days_from_0(1970);
  return days_from_0(year) - days_from_0_to_1970;
}

static int32_t days_from_1jan(int32_t year,int32_t month,int32_t day) {
  static const int32_t days[2][12] =
  {
    { 0,31,59,90,120,151,181,212,243,273,304,334},
    { 0,31,60,91,121,152,182,213,244,274,305,335}
  };

  return days[is_leap(year)][month-1] + day - 1;
}

static  time_t internal_timegm(tm const *t) {
  int year = t->tm_year + 1900;
  int month = t->tm_mon;
  if(month > 11)
  {
    year += month/12;
    month %= 12;
  }
  else if(month < 0)
  {
    int years_diff = (-month + 11)/12;
    year -= years_diff;
    month+=12 * years_diff;
  }
  month++;
  int day = t->tm_mday;
  int day_of_year = days_from_1jan(year,month,day);
  int days_since_epoch = days_from_1970(year) + day_of_year ;

  time_t seconds_in_day = 3600 * 24;
  time_t result = seconds_in_day * days_since_epoch + 3600 * t->tm_hour + 60 * t->tm_min + t->tm_sec;

  return result;
}

#endif
