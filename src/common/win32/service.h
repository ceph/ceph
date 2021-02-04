/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/compat.h"
#include "common/ceph_context.h"

class ServiceBase {

public:
  ServiceBase(CephContext *cct_);
  virtual ~ServiceBase() {};

  static int initialize(ServiceBase *service);
protected:
  static void run();
  static void control_handler(DWORD request);

  void shutdown(bool ignore_errors = false);
  void stop();

  void set_status(DWORD current_state, DWORD exit_code = NO_ERROR);

  /* Subclasses should implement the following service hooks. */
  virtual int run_hook() = 0;
  /* Invoked when the service is requested to stop. */
  virtual int stop_hook() = 0;
  /* Invoked when the system is shutting down. */
  virtual int shutdown_hook() = 0;

  CephContext *cct;

private:
  /* A handle used when reporting the current status. */
  SERVICE_STATUS_HANDLE hstatus;
  /* The current service status. */
  SERVICE_STATUS status;

  /* singleton service instance */
  static ServiceBase *s_service;
};
