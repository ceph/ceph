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

#define dout_context cct
#define dout_subsys ceph_subsys_

#include "common/debug.h"
#include "common/errno.h"
#include "common/win32/service.h"

// Initialize the singleton service instance.
ServiceBase *ServiceBase::s_service = NULL;

ServiceBase::ServiceBase(CephContext *cct_): cct(cct_)
{
  status.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
  status.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
  status.dwCurrentState = SERVICE_START_PENDING;
  status.dwWin32ExitCode = NO_ERROR;
  status.dwCheckPoint = 0;
  /* The estimated time required for the stop operation in ms. */
  status.dwWaitHint = 0;
}

/* Register service action callbacks */
int ServiceBase::initialize(ServiceBase *service)
{
  s_service = service;

  SERVICE_TABLE_ENTRY service_table[] = {
    {"", (LPSERVICE_MAIN_FUNCTION)run},
    {NULL, NULL}
  };

  /* StartServiceCtrlDispatcher blocks until the service is stopped. */
  if (!StartServiceCtrlDispatcher(service_table)) {
    int err = GetLastError();
    lderr(service->cct) << "StartServiceCtrlDispatcher error: "
                        << err << dendl;
    return -EINVAL;
  }
  return 0;
}

void WINAPI ServiceBase::run()
{
  assert(s_service != NULL);

  /* Register the control handler. This function is called by the service
   * manager to stop the service. The service name that we're passing here
   * doesn't have to be valid as we're using SERVICE_WIN32_OWN_PROCESS. */
  s_service->hstatus = RegisterServiceCtrlHandler(
    "", (LPHANDLER_FUNCTION)control_handler);
  if (!s_service->hstatus) {
    lderr(s_service->cct) << "Could not initialize service control handler. "
                          << "Error: " << GetLastError() << dendl;
    return;
  }

  s_service->set_status(SERVICE_START_PENDING);

  // TODO: should we expect exceptions?
  ldout(s_service->cct, 0) << "Starting service." << dendl;
  int err = s_service->run_hook();
  if (err) {
    lderr(s_service->cct) << "Failed to start service. Error code: "
                          << err << dendl;
    s_service->shutdown(true);
  } else {
    ldout(s_service->cct, 0) << "Successfully started service." << dendl;
    s_service->set_status(SERVICE_RUNNING);
  }
}

void ServiceBase::shutdown(bool ignore_errors)
{
  DWORD original_state = status.dwCurrentState;
  set_status(SERVICE_STOP_PENDING);

  int err = shutdown_hook();
  if (err) {
    derr << "Shutdown service hook failed. Error code: " << err << dendl;
    if (ignore_errors) {
      derr << "Ignoring shutdown hook failure, marking the service as stopped."
           << dendl;
      set_status(SERVICE_STOPPED);
    } else {
      derr << "Reverting to original service state." << dendl;
      set_status(original_state);
    }
  } else {
    dout(0) << "Shutdown hook completed." << dendl;
    set_status(SERVICE_STOPPED);
  }
}

void ServiceBase::stop()
{
  DWORD original_state = status.dwCurrentState;
  set_status(SERVICE_STOP_PENDING);

  int err = stop_hook();
  if (err) {
    derr << "Service stop hook failed. Error code: " << err << dendl;
    set_status(original_state);
  } else {
    dout(0) << "Successfully stopped service." << dendl;
    set_status(SERVICE_STOPPED);
  }
}

/* This function is registered with the Windows services manager through
 * a call to RegisterServiceCtrlHandler() and will be called by the Windows
 * service manager asynchronously to stop the service. */
void ServiceBase::control_handler(DWORD request)
{
  switch (request) {
  case SERVICE_CONTROL_STOP:
    s_service->stop();
    break;
  case SERVICE_CONTROL_SHUTDOWN:
    s_service->shutdown();
    break;
  default:
    break;
  }
}

void ServiceBase::set_status(DWORD current_state, DWORD exit_code) {
  static DWORD dwCheckPoint = 1;
  if (current_state == SERVICE_RUNNING || current_state == SERVICE_STOPPED) {
    status.dwCheckPoint = dwCheckPoint++;
  }

  status.dwCurrentState = current_state;
  status.dwWin32ExitCode = exit_code;

  if (hstatus) {
    dout(5) << "Updating service service status (" << current_state
             << ") and exit code(" << exit_code << ")." << dendl;
    ::SetServiceStatus(hstatus, &status);
  } else {
    derr << "Service control handler not initialized. Cannot "
         << "update service status (" << current_state
         << ") and exit code(" << exit_code << ")." << dendl;
  }
}
