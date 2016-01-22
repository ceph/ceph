// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#include <signal.h>

#include "dmclock_util.h"


void crimson::dmclock::debugger() {
    raise(SIGCONT);
}
