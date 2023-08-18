// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/Shell.h"

int main(int argc, const char **argv)
{
  #ifdef _WIN32
  SetConsoleOutputCP(CP_UTF8);
  #endif
  rbd::Shell shell;
  return shell.execute(argc, argv);
}
