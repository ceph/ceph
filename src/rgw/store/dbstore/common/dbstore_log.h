// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DB_STORE_LOG_H
#define DB_STORE_LOG_H

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>
#include <fstream>

using namespace std;

#define L_ERR   0
#define L_EVENT 1  // Default LogLevel
#define L_DEBUG 2
#define L_FULLDEBUG 3

extern int LogLevel;
extern string LogFile;
extern ofstream fileout;
extern ostream *dbout;

#define dbout_prefix *dbout<<__PRETTY_FUNCTION__<<":-"

#define dbout(n) if (n <= LogLevel) dbout_prefix

#endif
