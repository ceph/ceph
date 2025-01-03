/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2019 SUSE LLC
 * Copyright (C) 2022 Cloudbase Solutions
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once
#include <comutil.h>

#define _WIN32_DCOM
#include <wbemcli.h>

#include <string>
#include <vector>

#include "common/ceph_mutex.h"

// Convenience helper for initializing and cleaning up the
// Windows COM library using "COINIT_MULTITHREADED" concurrency mode.
// Any WMI objects (including connections, event subscriptions, etc)
// must be released before the COM library gets closed.
class COMBootstrapper
{
private:
  bool initialized = false;

  ceph::mutex init_lock = ceph::make_mutex("COMBootstrapper::InitLocker");

public:
  HRESULT initialize();
  void cleanup();

  ~COMBootstrapper()
  {
    cleanup();
  }
};

class WmiConnection
{
private:
  std::wstring ns;
public:
  IWbemLocator* wbem_loc;
  IWbemServices* wbem_svc;

  WmiConnection(std::wstring ns)
    : ns(ns)
    , wbem_loc(nullptr)
    , wbem_svc(nullptr)
  {
  }
  ~WmiConnection()
  {
    close();
  }

  HRESULT initialize();
  void close();
};

HRESULT get_property_str(
  IWbemClassObject* cls_obj,
  const std::wstring& property,
  std::wstring& value);
HRESULT get_property_int(
  IWbemClassObject* cls_obj,
  const std::wstring& property,
  uint32_t& value);

class WmiSubscription
{
private:
  std::wstring query;

  WmiConnection conn;
  IEnumWbemClassObject *event_enum;

public:
  WmiSubscription(std::wstring ns, std::wstring query)
    : query(query)
    , conn(WmiConnection(ns))
    , event_enum(nullptr)
  {
  }
  ~WmiSubscription()
  {
    close();
  }

  HRESULT initialize();
  void close();

  // IEnumWbemClassObject::Next wrapper
  HRESULT next(
    long timeout,
    ULONG count,
    IWbemClassObject **objects,
    ULONG *returned);
};

WmiSubscription subscribe_wnbd_adapter_events(uint32_t interval);
