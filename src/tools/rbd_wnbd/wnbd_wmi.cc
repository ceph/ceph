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

#include "wnbd_wmi.h"

#include "common/debug.h"
#include "common/win32/wstring.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd-wnbd: "

// Initializes the COM library for use by the calling thread using
// COINIT_MULTITHREADED.
static HRESULT co_initialize_basic()
{
  dout(10) << "initializing COM library" << dendl;

  HRESULT hres = CoInitializeEx(0, COINIT_MULTITHREADED);
  if (FAILED(hres)) {
    derr << "CoInitializeEx failed. HRESULT: " << hres << dendl;
    return hres;
  }

  // CoInitializeSecurity must be called once per process.
  static bool com_security_flags_set = false;

  if (!com_security_flags_set) {
    hres = CoInitializeSecurity(
      NULL, -1, NULL, NULL,
      RPC_C_AUTHN_LEVEL_DEFAULT,
      RPC_C_IMP_LEVEL_IMPERSONATE,
      NULL,
      EOAC_NONE,
      NULL);
    if (FAILED(hres)) {
      derr << "CoInitializeSecurity failed. HRESULT: " << hres << dendl;
      CoUninitialize();
      return hres;
    }

    com_security_flags_set = true;
  }

  return 0;
}

// co_uninitialize must be called once for every successful
// co_initialize_basic call. Any WMI objects (including connections,
// event subscriptions, etc) must be released beforehand.
static void co_uninitialize()
{
  dout(10) << "closing COM library" << dendl;
  CoUninitialize();
}

HRESULT COMBootstrapper::initialize()
{
  std::unique_lock l{init_lock};

  HRESULT hres = co_initialize_basic();
  if (!FAILED(hres)) {
    initialized = true;
  }
  return hres;
}

void COMBootstrapper::cleanup()
{
  if (initialized) {
    co_uninitialize();
    initialized = false;
  }
}

void WmiConnection::close()
{
  dout(20) << "closing wmi conn: " << this
      << ", svc: " << wbem_svc
      << ", loc: " << wbem_loc << dendl;
  if (wbem_svc != NULL) {
    wbem_svc->Release();
    wbem_svc = NULL;
  }
  if (wbem_loc != NULL) {
    wbem_loc->Release();
    wbem_loc = NULL;
  }
}

HRESULT WmiConnection::initialize()
{
  HRESULT hres = CoCreateInstance(
    CLSID_WbemLocator, 0, CLSCTX_INPROC_SERVER,
    IID_IWbemLocator, (LPVOID*)&wbem_loc);
  if (FAILED(hres)) {
    derr << "CoCreateInstance failed. HRESULT: " << hres << dendl;
    return hres;
  }

  hres = wbem_loc->ConnectServer(
    _bstr_t(ns.c_str()).GetBSTR(), NULL, NULL, NULL,
    WBEM_FLAG_CONNECT_USE_MAX_WAIT, NULL, NULL,
    &wbem_svc);
  if (FAILED(hres)) {
    derr << "Could not connect to WMI service. HRESULT: " << hres << dendl;
    return hres;
  }

  if (!wbem_svc) {
    hres = MAKE_HRESULT(SEVERITY_ERROR, FACILITY_WIN32,
                        ERROR_INVALID_HANDLE);
    derr << "WMI connection failed, no WMI service object received." << dendl;
    return hres;
  }

  hres = CoSetProxyBlanket(
    wbem_svc, RPC_C_AUTHN_WINNT, RPC_C_AUTHZ_NONE, NULL,
    RPC_C_AUTHN_LEVEL_CALL, RPC_C_IMP_LEVEL_IMPERSONATE, NULL, EOAC_NONE);
  if (FAILED(hres)) {
    derr << "CoSetProxyBlanket failed. HRESULT:" << hres << dendl;
  }

  return hres;
}

HRESULT get_property_str(
  IWbemClassObject* cls_obj,
  const std::wstring& property,
  std::wstring& value)
{
  VARIANT vt_prop;
  VariantInit(&vt_prop);
  HRESULT hres = cls_obj->Get(property.c_str(), 0, &vt_prop, 0, 0);
  if (!FAILED(hres)) {
    VARIANT vt_bstr_prop;
    VariantInit(&vt_bstr_prop);
    hres = VariantChangeType(&vt_bstr_prop, &vt_prop, 0, VT_BSTR);
    if (!FAILED(hres)) {
      value = vt_bstr_prop.bstrVal;
    }
    VariantClear(&vt_bstr_prop);
  }
  VariantClear(&vt_prop);

  if (FAILED(hres)) {
    derr << "Could not get WMI property: " << to_string(property)
         << ". HRESULT: " << hres << dendl;
  }
  return hres;
}

HRESULT get_property_int(
  IWbemClassObject* cls_obj,
  const std::wstring& property,
  uint32_t& value)
{
  VARIANT vt_prop;
  VariantInit(&vt_prop);
  HRESULT hres = cls_obj->Get(property.c_str(), 0, &vt_prop, 0, 0);
  if (!FAILED(hres)) {
    VARIANT vt_uint_prop;
    VariantInit(&vt_uint_prop);
    hres = VariantChangeType(&vt_uint_prop, &vt_prop, 0, VT_UINT);
    if (!FAILED(hres)) {
      value = vt_uint_prop.intVal;
    }
    VariantClear(&vt_uint_prop);
  }
  VariantClear(&vt_prop);

  if (FAILED(hres)) {
    derr << "Could not get WMI property: " << to_string(property)
         << ". HRESULT: " << hres << dendl;
  }
  return hres;
}

HRESULT WmiSubscription::initialize()
{
  HRESULT hres = conn.initialize();
  if (FAILED(hres)) {
    derr << "Could not create WMI connection" << dendl;
    return hres;
  }

  hres = conn.wbem_svc->ExecNotificationQuery(
    _bstr_t(L"WQL").GetBSTR(),
    _bstr_t(query.c_str()).GetBSTR(),
    WBEM_FLAG_FORWARD_ONLY | WBEM_FLAG_RETURN_IMMEDIATELY,
    NULL,
    &event_enum);

  if (FAILED(hres)) {
    derr << "Notification query failed, unable to subscribe to "
         << "WMI events. HRESULT: " << hres << dendl;
  } else {
    dout(20) << "wmi subscription initialized: " << this
      << ", event enum: " << event_enum
      << ", conn: " << &conn << ", conn svc: " << conn.wbem_svc << dendl;
  }

  return hres;
}

void WmiSubscription::close()
{
  dout(20) << "closing wmi subscription: " << this
    << ", event enum: " << event_enum << dendl;
  if (event_enum != NULL) {
    event_enum->Release();
    event_enum = NULL;
  }
}

HRESULT WmiSubscription::next(
  long timeout,
  ULONG count,
  IWbemClassObject **objects,
  ULONG *returned)
{
 if (!event_enum) {
    HRESULT hres = MAKE_HRESULT(
      SEVERITY_ERROR, FACILITY_WIN32,
      ERROR_INVALID_HANDLE);
    derr << "WMI subscription uninitialized." << dendl;
    return hres;
  }

  HRESULT hres = event_enum->Next(timeout, count, objects, returned);
  if (FAILED(hres)) {
    derr << "Unable to retrieve WMI events. HRESULT: "
         << hres << dendl;
  }
  return hres;
}

WmiSubscription subscribe_wnbd_adapter_events(
  uint32_t interval)
{
  std::wostringstream query_stream;
  query_stream
    << L"SELECT * FROM __InstanceOperationEvent "
    << L"WITHIN " << interval
    << L"WHERE TargetInstance ISA 'Win32_ScsiController' "
    << L"AND TargetInstance.Description="
    << L"'WNBD SCSI Virtual Adapter'";

  return WmiSubscription(L"root\\cimv2", query_stream.str());
}
