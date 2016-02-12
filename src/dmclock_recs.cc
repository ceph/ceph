// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "dmclock_recs.h"


std::ostream& crimson::dmclock::operator<<(std::ostream& out, PhaseType phase) {
    out << (PhaseType::reservation == phase ? "reservation" : "priority");
    return out;
}
