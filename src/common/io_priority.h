// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_IO_PRIORITY_H
#define CEPH_COMMON_IO_PRIORITY_H

#include <string>

extern pid_t ceph_gettid();

#ifndef IOPRIO_WHO_PROCESS
# define IOPRIO_WHO_PROCESS 1
#endif
#ifndef IOPRIO_PRIO_VALUE
# define IOPRIO_CLASS_SHIFT 13
# define IOPRIO_PRIO_VALUE(class, data) \
		(((class) << IOPRIO_CLASS_SHIFT) | (data))
#endif
#ifndef IOPRIO_CLASS_RT
# define IOPRIO_CLASS_RT 1
#endif
#ifndef IOPRIO_CLASS_BE
# define IOPRIO_CLASS_BE 2
#endif
#ifndef IOPRIO_CLASS_IDLE
# define IOPRIO_CLASS_IDLE 3
#endif

extern int ceph_ioprio_set(int whence, int who, int ioprio);

extern int ceph_ioprio_string_to_class(const std::string& s);

#endif
