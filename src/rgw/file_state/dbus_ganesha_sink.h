// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

// Production-mode GaneshaSink: applies the desired export set
// to a colocated nfs-ganesha via its D-Bus admin interface
// (`org.ganesha.nfsd.exportmgr`). On apply():
//
//   1. Diff `desired` against the in-memory record of the last-
//      applied set (keyed by the (fs_id, ap_id, mt_id) triple).
//   2. For each addition: render an EXPORT block to a config
//      file under `cfg.export_config_dir`, then dbus-call
//      AddExport(file_path).
//   3. For each update: re-render the file, dbus-call
//      UpdateExport(file_path, export_id).
//   4. For each removal: dbus-call RemoveExport(export_id),
//      then unlink the file.
//
// Wire mechanism: subprocess execution of `dbus-send`. This is
// slower than a libdbus-1 client but adds zero build-system
// dependencies on Ceph as a whole — the same trade-off cephadm
// makes elsewhere. Swappable via the `DbusInvoker` callable.
//
// FSAL credentials: until the s3files mount-auth handshake +
// per-session sts:AssumeRole pathway is built (see the
// "Mount-auth and role assumption" section of
// doc/dev/radosgw/s3_files_api.rst), every export uses the
// statically-configured credentials in `Config`. The role
// associated with the FileSystem isn't yet assumed; the
// resulting EXPORT block carries the role ARN as a comment so
// future tooling can wire it up without breaking compatibility.
//
// On reconciler restart, the in-memory mapping is empty —
// existing Ganesha exports we created in a prior run will be
// re-discovered and replayed via ShowExports rebuild on the
// first apply(). For v1 we trust the reconciler to be the only
// thing manipulating exports under our config-dir prefix.

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <tuple>
#include <vector>

#include "ganesha_sink.h"

namespace rgw::file_state {

class DbusGaneshaSink : public GaneshaSink {
 public:
  struct Config {
    // Directory where per-export <export_id>.conf files are
    // written. The reconciler must own this directory; nothing
    // else should be placing config files here.
    std::string export_config_dir;

    // RGW endpoint URL (e.g. "http://127.0.0.1:8000") and
    // credentials used by Ganesha's RGW FSAL. Static for v1;
    // sts:AssumeRole per session is post-mount-auth-handshake
    // work.
    std::string rgw_endpoint;
    std::string rgw_user_id;
    std::string rgw_access_key;
    std::string rgw_secret_key;

    // D-Bus addressing. Defaults match upstream Ganesha's
    // standard system-bus shape; rare deployments override.
    std::string dbus_dest = "org.ganesha.nfsd";
    std::string dbus_object = "/org/ganesha/nfsd/ExportMgr";
    std::string dbus_iface = "org.ganesha.nfsd.exportmgr";
    bool system_bus = true;

    // Starting Export_ID. Ganesha reserves low ids for various
    // pseudo-FS purposes; we stay above. Each new (fs,ap,mt)
    // gets a fresh increment.
    std::uint16_t next_export_id = 1000;
  };

  // dbus-send invocation interface. Returns the subprocess
  // exit status and captured stdout for AddExport's id parse.
  // Tests inject a recording invoker; production uses
  // default_invoker() which fork+execs `dbus-send`.
  struct DbusResult {
    int exit_status = 0;
    std::string stdout_text;
  };
  using DbusInvoker = std::function<DbusResult(
      const std::vector<std::string>& argv)>;

  static DbusInvoker default_invoker();

  DbusGaneshaSink(Config cfg, DbusInvoker invoker = nullptr);

  void apply(std::vector<DesiredExport> desired) override;

  // Visible for tests.
  std::optional<std::uint16_t> export_id_for(
      std::string_view fs_id,
      std::string_view ap_id,
      std::string_view mt_id) const;

 private:
  using Key = std::tuple<std::string, std::string, std::string>;

  static Key key_of(const DesiredExport& e);

  // Render the per-export Ganesha config block.
  std::string render_export_block(
      std::uint16_t export_id, const DesiredExport& e) const;

  // Write the config file. Returns the absolute file path.
  std::string write_config_file(
      std::uint16_t export_id, const DesiredExport& e);

  // D-Bus method calls. Each returns true on success.
  bool dbus_add_export(const std::string& config_path);
  bool dbus_remove_export(std::uint16_t export_id);
  bool dbus_update_export(
      const std::string& config_path, std::uint16_t export_id);

  // dbus-send arg builder shared by all three calls.
  std::vector<std::string> base_dbus_args(
      const std::string& method) const;

  Config cfg_;
  DbusInvoker invoker_;

  // Mapping: (fs, ap, mt) → currently-installed Ganesha
  // export_id. Updated on Add (insert), Remove (erase),
  // Update (no change). Empty on first apply() of a process.
  std::map<Key, std::uint16_t> mapping_;

  // Last-applied DesiredExport keyed by (fs, ap, mt). Used to
  // detect "needs update" — if the new DesiredExport differs
  // from the prior one, re-render and call UpdateExport.
  std::map<Key, DesiredExport> last_applied_;
};

}  // namespace rgw::file_state
