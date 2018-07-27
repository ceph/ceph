// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr_commands.h"

// an always_on module behaves as if it provides built-in functionality. it's
// always enabled, and always_on modules do not appear in the list of modules
// that can be enabled or disabled by the user. a health warning error is
// reported if a module in this set is cannot be loaded and started.
//
// NOTE: if a module is added to this set AND that module _may_ have been
// enabled in some cluster (e.g. an existing module is being upgraded to
// always-on status), then additional logic is needed in the monitor to trim the
// enabled set (or upgrade script to run `disable <module`). At the time of
// writing, the proposed always-on modules are being added within the same
// release.
const std::set<std::string> always_on_modules = {"crash"};

/* The set of statically defined (C++-handled) commands.  This
 * does not include the Python-defined commands, which are loaded
 * in PyModules */
const std::vector<MonCommand> mgr_commands = {
#define COMMAND(parsesig, helptext, module, perm, availability) \
  {parsesig, helptext, module, perm, availability, 0},
#include "MgrCommands.h"
#undef COMMAND
};
