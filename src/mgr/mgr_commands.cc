// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "mgr_commands.h"

/* The set of statically defined (C++-handled) commands.  This
 * does not include the Python-defined commands, which are loaded
 * in PyModules */
const std::vector<MonCommand> mgr_commands = {
#define COMMAND(parsesig, helptext, module, perm) \
  {parsesig, helptext, module, perm, 0},
#include "MgrCommands.h"
#undef COMMAND
};
