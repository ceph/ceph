// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr_commands.h"

/* The set of statically defined (C++-handled) commands.  This
 * does not include the Python-defined commands, which are loaded
 * in PyModules */
const std::vector<MonCommand> mgr_commands = {
#define COMMAND(parsesig, helptext, module, perm, availability) \
  {parsesig, helptext, module, perm, availability, 0},
#include "MgrCommands.h"
#undef COMMAND
};
