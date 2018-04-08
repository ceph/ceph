#ifndef CEPH_RGW_ADMIN_MULTISITE_H
#define CEPH_RGW_ADMIN_MULTISITE_H

#include <boost/optional.hpp>
#include "rgw_sync.h"
#include "rgw_data_sync.h"
#include "rgw_admin_common.h"

// This header and the corresponding source file contain handling of the following commads / groups of commands:
// Period, realm, zone, zonegroup, data sync, metadata sync

/// search each zonegroup for a connection
boost::optional<RGWRESTConn> get_remote_conn(RGWRados *store, const RGWPeriodMap& period_map,
                                             const std::string& remote);

int send_to_url(const std::string& url, const std::string& access, const std::string& secret, req_info& info, bufferlist& in_data,
                JSONParser& parser);

int send_to_remote_or_url(RGWRESTConn *conn, const std::string& url, const std::string& access, const std::string& secret,
                          req_info& info, bufferlist& in_data, JSONParser& parser);

int commit_period(RGWRados *store, RGWRealm& realm, RGWPeriod& period, std::string remote, const std::string& url,
                  const std::string& access, const std::string& secret, bool force);

int update_period(RGWRados *store, const std::string& realm_id, const std::string& realm_name, const std::string& period_id,
                  const std::string& period_epoch, bool commit, const std::string& remote, const std::string& url,
                  const std::string& access, const std::string& secret, Formatter *formatter, bool force);

int do_period_pull(RGWRados *store, RGWRESTConn *remote_conn, const std::string& url,
                   const std::string& access_key, const std::string& secret_key,
                   const std::string& realm_id, const std::string& realm_name,
                   const std::string& period_id, const std::string& period_epoch,
                   RGWPeriod *period);

int handle_opt_period_delete(const std::string& period_id, CephContext *context, RGWRados *store);

int handle_opt_period_get(const std::string& period_epoch, std::string& period_id, bool staging, std::string& realm_id,
                          std::string& realm_name, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_period_get_current(const std::string& realm_id, const std::string& realm_name, RGWRados *store, Formatter *formatter);

int handle_opt_period_list(RGWRados *store, Formatter *formatter);

int handle_opt_period_pull(const std::string& period_id, const std::string& period_epoch, const std::string& realm_id,
                           const std::string& realm_name, const std::string& url, const std::string& access_key, const std::string& secret_key,
                           std::string& remote, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_period_push(const std::string& period_id, const std::string& period_epoch, const std::string& realm_id,
                           const std::string& realm_name, const std::string& url, const std::string& access_key, const std::string& secret_key,
                           CephContext *context, RGWRados *store);

int handle_opt_period_commit(const std::string& period_id, const std::string& period_epoch, const std::string& realm_id,
                             const std::string& realm_name, const std::string& url, const std::string& access_key,
                             const std::string& secret_key, const std::string& remote, bool yes_i_really_mean_it,
                             CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_realm_create(const std::string& realm_name, bool set_default, CephContext *context, RGWRados *store,
                            Formatter *formatter);

int handle_opt_realm_delete(const std::string& realm_id, const std::string& realm_name, CephContext *context, RGWRados *store);

int handle_opt_realm_get(const std::string& realm_id, const std::string& realm_name, CephContext *context, RGWRados *store,
                         Formatter *formatter);

int handle_opt_realm_get_default(CephContext *context, RGWRados *store);

int handle_opt_realm_list(CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_realm_list_periods(const std::string& realm_id, const std::string& realm_name, RGWRados *store, Formatter *formatter);

int handle_opt_realm_rename(const std::string& realm_id, const std::string& realm_name, const std::string& realm_new_name,
                            CephContext *context, RGWRados *store);

int handle_opt_realm_set(const std::string& realm_id, const std::string& realm_name, const std::string& infile,
                         bool set_default, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_realm_default(const std::string& realm_id, const std::string& realm_name, CephContext *context, RGWRados *store);

int handle_opt_realm_pull(const std::string& realm_id, const std::string& realm_name, const std::string& url, const std::string& access_key,
                          const std::string& secret_key, bool set_default, CephContext *context, RGWRados *store,
                          Formatter *formatter);

int handle_opt_zonegroup_add(const std::string& zonegroup_id, const std::string& zonegroup_name, const std::string& zone_id,
                             const std::string& zone_name, bool tier_type_specified, std::string *tier_type,
                             const map<std::string, std::string, ltstr_nocase>& tier_config_add, bool sync_from_all_specified,
                             bool *sync_from_all, bool redirect_zone_set, std::string *redirect_zone,
                             bool is_master_set, bool *is_master, bool is_read_only_set,
                             bool *read_only, const list<std::string>& endpoints, list<std::string>& sync_from,
                             list<std::string>& sync_from_rm, CephContext *context, RGWRados *store,
                             Formatter *formatter);

int handle_opt_zonegroup_create(const std::string& zonegroup_id, const std::string& zonegroup_name, const std::string& realm_id,
                                const std::string& realm_name, const std::string& api_name, bool set_default, bool is_master,
                                const list<std::string>& endpoints, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_default(const std::string& zonegroup_id, const std::string& zonegroup_name, CephContext *context,
                                 RGWRados *store);

int handle_opt_zonegroup_delete(const std::string& zonegroup_id, const std::string& zonegroup_name, CephContext *context,
                                RGWRados *store);

int handle_opt_zonegroup_get(const std::string& zonegroup_id, const std::string& zonegroup_name, CephContext *context,
                             RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_list(CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_modify(const std::string& zonegroup_id, const std::string& zonegroup_name, const std::string& realm_id,
                                const std::string& realm_name, const std::string& api_name, const std::string& master_zone,
                                bool is_master_set, bool is_master, bool set_default, const list<std::string>& endpoints,
                                CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_set(const std::string& zonegroup_id, const std::string& zonegroup_name, const std::string& realm_id,
                             const std::string& realm_name, const std::string& infile,  bool set_default, const list<std::string>& endpoints,
                             CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_remove(const std::string& zonegroup_id, const std::string& zonegroup_name, std::string& zone_id,
                                const std::string& zone_name, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_rename(const std::string& zonegroup_id, const std::string& zonegroup_name,
                                const std::string& zonegroup_new_name, CephContext *context, RGWRados *store);

int handle_opt_zonegroup_placement_list(const std::string& zonegroup_id, const std::string& zonegroup_name, CephContext *context,
                                        RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_placement_add(const std::string& placement_id, const std::string& zonegroup_id,
                                       const std::string& zonegroup_name, const list<std::string>& tags, CephContext *context,
                                       RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_placement_modify(const std::string& placement_id, const std::string& zonegroup_id,
                                          const std::string& zonegroup_name, const list<std::string>& tags,
                                          const list<std::string> tags_add, const list<std::string>& tags_rm,
                                          CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_placement_rm(const std::string& placement_id, const std::string& zonegroup_id,
                                      const std::string& zonegroup_name, CephContext *context,
                                      RGWRados *store, Formatter *formatter);

int handle_opt_zonegroup_placement_default(const std::string& placement_id, const std::string& zonegroup_id,
                                           const std::string& zonegroup_name, CephContext *context, RGWRados *store,
                                           Formatter *formatter);

int handle_opt_zone_create(const std::string& zone_id, const std::string& zone_name, const std::string& zonegroup_id,
                           const std::string& zonegroup_name, std::string& realm_id, const std::string& realm_name,
                           const std::string& access_key, const std::string& secret_key, bool tier_type_specified,
                           std::string *tier_type, const map<std::string, std::string, ltstr_nocase>& tier_config_add,
                           bool sync_from_all_specified, bool *sync_from_all, bool redirect_zone_set,
                           std::string *redirect_zone, bool is_master_set, bool *is_master, bool is_read_only_set,
                           bool *read_only, const list<std::string>& endpoints, list<std::string>& sync_from,
                           list<std::string>& sync_from_rm, bool set_default, CephContext *context, RGWRados *store,
                           Formatter *formatter);

int handle_opt_zone_default(const std::string& zone_id, const std::string& zone_name, const std::string& zonegroup_id,
                            const std::string& zonegroup_name, CephContext *context, RGWRados *store);

int handle_opt_zone_delete(const std::string& zone_id, const std::string& zone_name, const std::string& zonegroup_id,
                           const std::string& zonegroup_name, CephContext *context, RGWRados *store);

int handle_opt_zone_get(const std::string& zone_id, const std::string& zone_name, CephContext *context, RGWRados *store,
                        Formatter *formatter);

int handle_opt_zone_set(std::string& zone_name, const std::string& realm_id, const std::string& realm_name, const std::string& infile,
                        bool set_default, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zone_list(CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zone_modify(const std::string& zone_id, const std::string& zone_name, const std::string& zonegroup_id,
                           const std::string& zonegroup_name, std::string& realm_id, const std::string& realm_name,
                           const std::string& access_key, const std::string& secret_key, bool tier_type_specified,
                           std::string *tier_type, const map<std::string, std::string, ltstr_nocase>& tier_config_add,
                           const map<std::string, std::string, ltstr_nocase>& tier_config_rm,
                           bool sync_from_all_specified, bool *sync_from_all, bool redirect_zone_set,
                           std::string *redirect_zone, bool is_master_set, bool *is_master, bool is_read_only_set,
                           bool *read_only, const list<std::string>& endpoints, list<std::string>& sync_from,
                           list<std::string>& sync_from_rm, bool set_default, CephContext *context, RGWRados *store,
                           Formatter *formatter);

int handle_opt_zone_rename(const std::string& zone_id, const std::string& zone_name, const std::string& zone_new_name,
                           const std::string& zonegroup_id, const std::string& zonegroup_name,
                           CephContext *context, RGWRados *store);

int handle_opt_zone_placement_list(const std::string& zone_id, const std::string& zone_name, CephContext *context,
                                   RGWRados *store, Formatter *formatter);

int handle_opt_zone_placement_add(const std::string& placement_id, const std::string& zone_id, const std::string& zone_name,
                                  const boost::optional<std::string>& compression_type, const boost::optional<std::string>& index_pool,
                                  const boost::optional<std::string>& data_pool, const boost::optional<std::string>& data_extra_pool,
                                  bool index_type_specified, RGWBucketIndexType placement_index_type,
                                  CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zone_placement_modify(const std::string& placement_id, const std::string& zone_id, const std::string& zone_name,
                                     const boost::optional<std::string>& compression_type, const boost::optional<std::string>& index_pool,
                                     const boost::optional<std::string>& data_pool, const boost::optional<std::string>& data_extra_pool,
                                     bool index_type_specified, RGWBucketIndexType placement_index_type,
                                     CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_zone_placement_rm(const std::string& placement_id, const std::string& zone_id, const std::string& zone_name,
                                 const boost::optional<std::string>& compression_type, CephContext *context, RGWRados *store,
                                 Formatter *formatter);

int handle_opt_metadata_sync_status(RGWRados *store, Formatter *formatter);

int handle_opt_metadata_sync_init(RGWRados *store);

int handle_opt_metadata_sync_run(RGWRados *store);

int handle_opt_data_sync_status(const std::string& source_zone, RGWRados *store, Formatter *formatter);

int handle_opt_data_sync_init(const std::string& source_zone, const boost::intrusive_ptr<CephContext>& cct, RGWRados *store);

int handle_opt_data_sync_run(const std::string& source_zone, const boost::intrusive_ptr<CephContext>& cct, RGWRados *store);

class RgwAdminMetadataSyncCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  explicit RgwAdminMetadataSyncCommandsHandler(std::vector<const char*>& args,
                                               const std::vector<std::string>& prefix, RGWRados* store,
                                               Formatter* formatter)
      : RgwAdminCommandGroupHandler(args, prefix, {{"status", OPT_METADATA_SYNC_STATUS},
                                                   {"init",   OPT_METADATA_SYNC_INIT},
                                                   {"run",    OPT_METADATA_SYNC_RUN}}, store, formatter) {
    if (parse_command_and_parameters() == 0) {
      std::cout << "Parsed command: " << command << std::endl;
    }
  }

  ~RgwAdminMetadataSyncCommandsHandler() override = default;

  // If parameter parsing failed, the value of command is OPT_NO_CMD and a call of this method
  // will return EINVAL
  int execute_command() override {
    switch (command) {
      case (OPT_METADATA_SYNC_STATUS) :
        return handle_opt_metadata_sync_status();
      case (OPT_METADATA_SYNC_INIT) :
        return handle_opt_metadata_sync_init();
      case (OPT_METADATA_SYNC_RUN) :
        return handle_opt_metadata_sync_run();
      default:
        return EINVAL;
    }
  }

  RgwAdminCommandGroup get_type() const override { return METADATA_SYNC; }

private:
  int parse_command_and_parameters() override;

  int handle_opt_metadata_sync_status() {
    return ::handle_opt_metadata_sync_status(store, formatter);
  }
  int handle_opt_metadata_sync_init() {
    return ::handle_opt_metadata_sync_init(store);
  }
  int handle_opt_metadata_sync_run() {
    return ::handle_opt_metadata_sync_run(store);
  }
};

class RgwAdminPeriodCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  RgwAdminPeriodCommandsHandler(std::vector<const char*>& args,
                                const std::vector<std::string>& prefix, RGWRados* store,
                                Formatter* formatter)
      : RgwAdminCommandGroupHandler(args, prefix, {{"commit",      OPT_PERIOD_COMMIT},
                                                   {"delete",      OPT_PERIOD_DELETE},
                                                   {"get",         OPT_PERIOD_GET},
                                                   {"get-current", OPT_PERIOD_GET_CURRENT},
                                                   {"list",        OPT_PERIOD_LIST},
                                                   {"pull",        OPT_PERIOD_PULL},
                                                   {"push",        OPT_PERIOD_PUSH},
                                                   {"update",      OPT_PERIOD_UPDATE}},
                                    store, formatter) {
    if (parse_command_and_parameters() == 0) {
      std::cout << "Parsed command:" << command << std::endl;
    }
  }

  ~RgwAdminPeriodCommandsHandler() override = default;

  int execute_command() override {
    switch (command) {
      case (OPT_PERIOD_COMMIT) :
        return handle_opt_period_commit();
      case (OPT_PERIOD_DELETE) :
        return handle_opt_period_delete();
      case (OPT_PERIOD_GET) :
        return handle_opt_period_get();
      case (OPT_PERIOD_GET_CURRENT) :
        return handle_opt_period_get_current();
      case (OPT_PERIOD_LIST) :
        return handle_opt_period_list();
      case (OPT_PERIOD_PULL) :
        return handle_opt_period_pull();
      case (OPT_PERIOD_PUSH) :
        return handle_opt_period_push();
      case (OPT_PERIOD_UPDATE) :
        return handle_opt_period_update();
      default:
        return EINVAL;
    }
  }

  RgwAdminCommandGroup get_type() const override { return PERIOD; }

private:
  int parse_command_and_parameters() override;

  int handle_opt_period_commit() {
    return ::handle_opt_period_commit(period_id, period_epoch, realm_id, realm_name, url,
                                      access_key, secret_key, remote, yes_i_really_mean_it,
                                      g_ceph_context, store, formatter);
  }

  int handle_opt_period_delete() {
    return ::handle_opt_period_delete(period_id, g_ceph_context, store);
  }

  int handle_opt_period_get() {
    return ::handle_opt_period_get(period_epoch, period_id, staging, realm_id, realm_name,
                                   g_ceph_context, store, formatter);
  }

  int handle_opt_period_get_current() {
    return ::handle_opt_period_get_current(realm_id, realm_name, store, formatter);
  }

  int handle_opt_period_list() {
    return ::handle_opt_period_list(store, formatter);
  }

  int handle_opt_period_pull() {
    return ::handle_opt_period_pull(period_id, period_epoch, realm_id, realm_name, url, access_key,
                                    secret_key, remote, g_ceph_context, store, formatter);
  }

  int handle_opt_period_push() {
    return ::handle_opt_period_push(period_id, period_epoch, realm_id, realm_name, url,
                                    access_key, secret_key, g_ceph_context, store);
  }

  int handle_opt_period_update() {
    return update_period(store, realm_id, realm_name, period_id, period_epoch,
                         commit, remote, url, access_key, secret_key, formatter,
                         yes_i_really_mean_it);
  }

  // TODO: add an option to generate access key
  std::string access_key;
  std::string secret_key;
  bool commit = false;
  std::string period_epoch;
  std::string period_id;
  std::string realm_id;
  std::string realm_name;
  std::string remote;
  bool staging = false;
  std::string url;
  bool yes_i_really_mean_it = false;
};

class RgwAdminRealmCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  RgwAdminRealmCommandsHandler(std::vector<const char*>& args,
                               const std::vector<std::string>& prefix, RGWRados* store,
                               Formatter* formatter)
      : RgwAdminCommandGroupHandler(args, prefix, {{"create",       OPT_REALM_CREATE},
                                                   {"default",      OPT_REALM_DEFAULT},
                                                   {"delete",       OPT_REALM_DELETE},
                                                   {"get",          OPT_REALM_GET},
                                                   {"get-default",  OPT_REALM_GET_DEFAULT},
                                                   {"list",         OPT_REALM_LIST},
                                                   {"list-periods", OPT_REALM_LIST_PERIODS},
                                                   {"rename",       OPT_REALM_RENAME},
                                                   {"pull",         OPT_REALM_PULL},
                                                   {"set",          OPT_REALM_SET}},
                                    store, formatter) {
    if (parse_command_and_parameters() == 0) {
      std::cout << "Parsed command:" << command << std::endl;
    }
  }

  ~RgwAdminRealmCommandsHandler() = default;

  RgwAdminCommandGroup get_type() const override { return REALM; }

  int execute_command() override {
    switch (command) {
      case (OPT_REALM_CREATE) :
        return handle_opt_realm_create();
      case (OPT_REALM_DEFAULT) :
        return handle_opt_realm_default();
      case (OPT_REALM_DELETE) :
        return handle_opt_realm_delete();
      case (OPT_REALM_GET) :
        return handle_opt_realm_get();
      case (OPT_REALM_GET_DEFAULT) :
        return handle_opt_realm_get_default();
      case (OPT_REALM_LIST) :
        return handle_opt_realm_list();
      case (OPT_REALM_LIST_PERIODS) :
        return handle_opt_realm_list_periods();
      case (OPT_REALM_RENAME) :
        return handle_opt_realm_rename();
      case (OPT_REALM_PULL) :
        return handle_opt_realm_pull();
      case (OPT_REALM_SET) :
        return handle_opt_realm_set();
      default:
        return EINVAL;
    }
  }

private:
  int parse_command_and_parameters() override;

  int handle_opt_realm_create() {
    return ::handle_opt_realm_create(realm_name, set_default, g_ceph_context, store, formatter);
  }

  int handle_opt_realm_default() {
    return ::handle_opt_realm_default(realm_id, realm_name, g_ceph_context, store);
  }

  int handle_opt_realm_delete() {
    return ::handle_opt_realm_delete(realm_id, realm_name, g_ceph_context, store);
  }

  int handle_opt_realm_get() {
    return ::handle_opt_realm_get(realm_id, realm_name, g_ceph_context, store, formatter);
  }

  int handle_opt_realm_get_default() {
    return ::handle_opt_realm_get_default(g_ceph_context, store);
  }

  int handle_opt_realm_list() {
    return ::handle_opt_realm_list(g_ceph_context, store, formatter);
  }

  int handle_opt_realm_list_periods() {
    return ::handle_opt_realm_list_periods(realm_id, realm_name, store, formatter);
  }

  int handle_opt_realm_rename() {
    return ::handle_opt_realm_rename(realm_id, realm_name, realm_new_name, g_ceph_context, store);
  }

  int handle_opt_realm_pull() {
    return ::handle_opt_realm_pull(realm_id, realm_name, url, access_key, secret_key,
                                   set_default, g_ceph_context, store, formatter);
  }

  int handle_opt_realm_set() {
    return ::handle_opt_realm_set(realm_id, realm_name, infile, set_default, g_ceph_context,
                                  store, formatter);
  }

  // TODO: support generating access key
  std::string access_key;
  std::string secret_key;
  std::string infile;
  std::string realm_id;
  std::string realm_name;
  std::string realm_new_name;
  bool set_default = false;
  std::string url;
};

class RgwAdminDataSyncCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  RgwAdminDataSyncCommandsHandler(std::vector<const char*>& args,
                                  const std::vector<std::string>& prefix, RGWRados* store,
                                  Formatter* formatter) : RgwAdminCommandGroupHandler(args, prefix,
                                                                                      {{"init",   OPT_DATA_SYNC_INIT},
                                                                                       {"run",    OPT_DATA_SYNC_RUN},
                                                                                       {"status", OPT_DATA_SYNC_STATUS},},
                                                                                      store, formatter) {
    if (parse_command_and_parameters() == 0) {
      std::cout << "Parsed command: " << command << std::endl;
    }
  }

  ~RgwAdminDataSyncCommandsHandler() override = default;

  RgwAdminCommandGroup get_type() const override { return DATA_SYNC; }

  int execute_command() override {
    switch (command) {
      case OPT_DATA_SYNC_INIT :
        return handle_opt_data_sync_init();
      case OPT_DATA_SYNC_RUN :
        return handle_opt_data_sync_run();
      case OPT_DATA_SYNC_STATUS :
        return handle_opt_data_sync_status();
      default:
        return EINVAL;
    }
  }

private:
  int parse_command_and_parameters() override;

  int handle_opt_data_sync_init() {
    return ::handle_opt_data_sync_init(source_zone, cct, store);
  }

  int handle_opt_data_sync_run() {
    return ::handle_opt_data_sync_run(source_zone, cct, store);
  }

  int handle_opt_data_sync_status() {
    return ::handle_opt_data_sync_status(source_zone, store, formatter);
  }

  std::string source_zone;
// TODO: initialize the following:
  boost::intrusive_ptr<CephContext> cct;
};

class RgwAdminZoneCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  RgwAdminZoneCommandsHandler(std::vector<const char*>& args,
                              const std::vector<std::string>& prefix, RGWRados* store,
                              Formatter* formatter) : RgwAdminCommandGroupHandler(args, prefix,
                                                                                  {{"create",  OPT_ZONE_CREATE},
                                                                                   {"default", OPT_ZONE_DEFAULT},
                                                                                   {"delete",  OPT_ZONE_DELETE},
                                                                                   {"get",     OPT_ZONE_GET},
                                                                                   {"list",    OPT_ZONE_LIST},
                                                                                   {"modify",  OPT_ZONE_MODIFY},
                                                                                   {"rename",  OPT_ZONE_RENAME},
                                                                                   {"set",     OPT_ZONE_SET},},
                                                                                  store, formatter) {
    if (parse_command_and_parameters() == 0) {
      std::cout << "Parsed command: " << command << std::endl;
    }
  }

  ~RgwAdminZoneCommandsHandler() override = default;

  RgwAdminCommandGroup get_type() const override { return ZONE; }

  int execute_command() override {
    switch (command) {
      case OPT_ZONE_CREATE :
        return handle_opt_zone_create();
      case OPT_ZONE_DEFAULT :
        return handle_opt_zone_default();
      case OPT_ZONE_DELETE :
        return handle_opt_zone_delete();
      case OPT_ZONE_GET :
        return handle_opt_zone_get();
      case OPT_ZONE_LIST :
        return handle_opt_zone_list();
      case OPT_ZONE_MODIFY :
        return handle_opt_zone_modify();
      case OPT_ZONE_RENAME :
        return handle_opt_zone_rename();
      case OPT_ZONE_SET :
        return handle_opt_zone_set();
      default:
        return EINVAL;
    }
  }

private:
  int parse_command_and_parameters() override;

  int handle_opt_zone_create() {
    return ::handle_opt_zone_create(zone_id, zone_name, zonegroup_id, zonegroup_name, realm_id,
                                    realm_name, access_key, secret_key, tier_type.is_initialized(),
                                    &tier_type.value(), tier_config_add, sync_from_all.is_initialized(),
                                    &sync_from_all.value(), redirect_zone.is_initialized(), &redirect_zone.value(),
                                    is_master.is_initialized(), &is_master.value(), read_only.is_initialized(),
                                    &read_only.value(),
                                    endpoints, sync_from, sync_from_rm, set_default,
                                    g_ceph_context, store, formatter);
  }

  int handle_opt_zone_default() {
    return ::handle_opt_zone_default(zone_id, zone_name, zonegroup_id, zonegroup_name,
                                     g_ceph_context, store);
  }

  int handle_opt_zone_delete() {
    return ::handle_opt_zone_delete(zone_id, zone_name, zonegroup_id, zonegroup_name,
                                    g_ceph_context, store);
  }

  int handle_opt_zone_get() {
    return ::handle_opt_zone_get(zone_id, zone_name, g_ceph_context, store, formatter);
  }

  int handle_opt_zone_list() {
    return ::handle_opt_zone_list(g_ceph_context, store, formatter);
  }

  int handle_opt_zone_modify() {
    return ::handle_opt_zone_modify(zone_id, zone_name, zonegroup_id, zonegroup_name, realm_id,
                                    realm_name, access_key, secret_key, tier_type.is_initialized(),
                                    &tier_type.value(), tier_config_add, tier_config_rm,
                                    sync_from_all.is_initialized(), &sync_from_all.value(),
                                    redirect_zone.is_initialized(),
                                    &redirect_zone.value(), is_master.is_initialized(), &is_master.value(),
                                    read_only.is_initialized(),
                                    &read_only.value(), endpoints, sync_from, sync_from_rm, set_default,
                                    g_ceph_context, store, formatter);
  }

  int handle_opt_zone_rename() {
    return ::handle_opt_zone_rename(zone_id, zone_name, zone_new_name, zonegroup_id,
                                    zonegroup_name, g_ceph_context, store);
  }

  int handle_opt_zone_set() {
    return ::handle_opt_zone_set(zone_name, realm_id, realm_name, infile, set_default,
                                 g_ceph_context, store, formatter);
  }

  std::string access_key;
  std::string infile;
  boost::optional<bool> is_master; // default value: false
  boost::optional<bool> read_only; // default value: false
  std::string realm_id;
  std::string realm_name;
  boost::optional<std::string> redirect_zone;
  std::string secret_key;
  bool set_default = false;
  boost::optional<bool> sync_from_all; // default value: false
  boost::optional<std::string> tier_type;
  std::string zone_id;
  std::string zone_name;
  std::string zone_new_name;
  std::string zonegroup_id;
  std::string zonegroup_name;

  std::list<std::string> endpoints;
  std::list<std::string> sync_from;
  std::list<std::string> sync_from_rm;
  std::map<std::string, std::string, ltstr_nocase> tier_config_add;
  std::map<std::string, std::string, ltstr_nocase> tier_config_rm;
};

class RgwAdminZonePlacementCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  RgwAdminZonePlacementCommandsHandler(std::vector<const char*>& args,
                                       const std::vector<std::string>& prefix, RGWRados* store,
                                       Formatter* formatter) : RgwAdminCommandGroupHandler(args, prefix,
                                                                                           {{"add",    OPT_ZONE_PLACEMENT_ADD},
                                                                                            {"list",   OPT_ZONE_PLACEMENT_LIST},
                                                                                            {"modify", OPT_ZONE_PLACEMENT_MODIFY},
                                                                                            {"rm",     OPT_ZONE_PLACEMENT_RM},},
                                                                                           store, formatter) {
    if (parse_command_and_parameters() == 0) {
      std::cout << "Parsed command: " << command << std::endl;
    }
  }

  ~RgwAdminZonePlacementCommandsHandler() override = default;

  RgwAdminCommandGroup get_type() const override { return ZONE_PLACEMENT; }

  int execute_command() override {
    switch (command) {
      case OPT_ZONE_PLACEMENT_ADD :
        return handle_opt_zone_placement_add();
      case OPT_ZONE_PLACEMENT_LIST :
        return handle_opt_zone_placement_list();
      case OPT_ZONE_PLACEMENT_MODIFY :
        return handle_opt_zone_placement_modify();
      case OPT_ZONE_PLACEMENT_RM :
        return handle_opt_zone_placement_rm();
      default:
        return EINVAL;
    }
  }

private:
  int parse_command_and_parameters() override;

  int handle_opt_zone_placement_add() {
    return ::handle_opt_zone_placement_add(placement_id, zone_id, zone_name, compression_type,
                                           index_pool, data_pool, data_extra_pool,
                                           placement_index_type.is_initialized(), placement_index_type.value(),
                                           g_ceph_context, store, formatter);
  }

  int handle_opt_zone_placement_list() {
    return ::handle_opt_zone_placement_list(zone_id, zone_name, g_ceph_context, store, formatter);
  }

  int handle_opt_zone_placement_modify() {
    return ::handle_opt_zone_placement_modify(placement_id, zone_id, zone_name, compression_type,
                                              index_pool, data_pool, data_extra_pool,
                                              placement_index_type.is_initialized(), placement_index_type.value(),
                                              g_ceph_context, store, formatter);
  }

  int handle_opt_zone_placement_rm() {
    return ::handle_opt_zone_placement_rm(placement_id, zone_id, zone_name, compression_type,
                                          g_ceph_context, store, formatter);
  }

  boost::optional<std::string> compression_type;
  boost::optional<std::string> data_extra_pool;
  boost::optional<std::string> data_pool;
  boost::optional<std::string> index_pool;
  std::string placement_id;
  std::string zone_id;
  std::string zone_name;

  boost::optional<RGWBucketIndexType> placement_index_type = RGWBIType_Normal;
};

class RgwAdminZonegroupCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  RgwAdminZonegroupCommandsHandler(std::vector<const char*>& args,
                                   const std::vector<std::string>& prefix, RGWRados* store,
                                   Formatter* formatter) : RgwAdminCommandGroupHandler(args, prefix,
                                                                                       {{"add",     OPT_ZONEGROUP_ADD},
                                                                                        {"create",  OPT_ZONEGROUP_CREATE},
                                                                                        {"default", OPT_ZONEGROUP_DEFAULT},
                                                                                        {"delete",  OPT_ZONEGROUP_DELETE},
                                                                                        {"get",     OPT_ZONEGROUP_GET},
                                                                                        {"list",    OPT_ZONEGROUP_LIST},
                                                                                        {"modify",  OPT_ZONEGROUP_MODIFY},
                                                                                        {"remove",  OPT_ZONEGROUP_REMOVE},
                                                                                        {"rename",  OPT_ZONEGROUP_RENAME},
                                                                                        {"set",     OPT_ZONEGROUP_SET},},
                                                                                       store, formatter) {
    if (parse_command_and_parameters() == 0) {
      std::cout << "Parsed command: " << command << std::endl;
    }
  }

  ~RgwAdminZonegroupCommandsHandler() override = default;

  RgwAdminCommandGroup get_type() const override { return ZONEGROUP; }

  int execute_command() override {
    switch (command) {
      case OPT_ZONEGROUP_ADD :
        return handle_opt_zonegroup_add();
      case OPT_ZONEGROUP_CREATE :
        return handle_opt_zonegroup_create();
      case OPT_ZONEGROUP_DEFAULT :
        return handle_opt_zonegroup_default();
      case OPT_ZONEGROUP_DELETE :
        return handle_opt_zonegroup_delete();
      case OPT_ZONEGROUP_GET :
        return handle_opt_zonegroup_get();
      case OPT_ZONEGROUP_LIST :
        return handle_opt_zonegroup_list();
      case OPT_ZONEGROUP_MODIFY :
        return handle_opt_zonegroup_modify();
      case OPT_ZONEGROUP_REMOVE :
        return handle_opt_zonegroup_remove();
      case OPT_ZONEGROUP_RENAME :
        return handle_opt_zonegroup_rename();
      case OPT_ZONEGROUP_SET :
        return handle_opt_zonegroup_set();
      default:
        return EINVAL;
    }
  }

private:
  int parse_command_and_parameters() override;

  int handle_opt_zonegroup_add() {
    return ::handle_opt_zonegroup_add(zonegroup_id, zonegroup_name, zone_id, zone_name,
                                      tier_type.is_initialized(), &tier_type.value(), tier_config_add,
                                      sync_from_all.is_initialized(), &sync_from_all.value(),
                                      redirect_zone.is_initialized(),
                                      &redirect_zone.value(), is_master.is_initialized(), &is_master.value(),
                                      read_only.is_initialized(),
                                      &read_only.value(), endpoints, sync_from, sync_from_rm,
                                      g_ceph_context, store, formatter);
  }

  int handle_opt_zonegroup_create() {
    return ::handle_opt_zonegroup_create(zonegroup_id, zonegroup_name, realm_id, realm_name,
                                         api_name, set_default, is_master.get_value_or(false), endpoints,
                                         g_ceph_context, store, formatter);
  }

  int handle_opt_zonegroup_default() {
    return ::handle_opt_zonegroup_default(zonegroup_id, zonegroup_name, g_ceph_context, store);
  }

  int handle_opt_zonegroup_delete() {
    return ::handle_opt_zonegroup_delete(zonegroup_id, zonegroup_name, g_ceph_context, store);
  }

  int handle_opt_zonegroup_get() {
    return ::handle_opt_zonegroup_get(zonegroup_id, zonegroup_name, g_ceph_context, store,
                                      formatter);
  }

  int handle_opt_zonegroup_list() {
    return ::handle_opt_zonegroup_list(g_ceph_context, store, formatter);
  }

  int handle_opt_zonegroup_modify() {
    return ::handle_opt_zonegroup_modify(zonegroup_id, zonegroup_name, realm_id, realm_name,
                                         api_name, master_zone, is_master.is_initialized(),
                                         is_master.get_value_or(false),
                                         set_default, endpoints, g_ceph_context, store,
                                         formatter);
  }

  int handle_opt_zonegroup_remove() {
    return ::handle_opt_zonegroup_remove(zonegroup_id, zonegroup_name, zone_id, zone_name,
                                         g_ceph_context, store, formatter);
  }

  int handle_opt_zonegroup_rename() {
    return ::handle_opt_zonegroup_rename(zonegroup_id, zonegroup_name, zonegroup_new_name,
                                         g_ceph_context, store);
  }

  int handle_opt_zonegroup_set() {
    return ::handle_opt_zonegroup_set(zonegroup_id, zonegroup_name, realm_id, realm_name, infile,
                                      set_default, endpoints, g_ceph_context, store, formatter);
  }

  std::string api_name;
  std::string infile;
  boost::optional<bool> is_master = false;
  std::string master_zone;
  boost::optional<bool> read_only = false;
  std::string realm_id;
  std::string realm_name;
  boost::optional<std::string> redirect_zone;
  bool set_default = false;
  boost::optional<bool> sync_from_all = false;
  boost::optional<std::string> tier_type;
  std::string zone_id;
  std::string zone_name;
  std::string zonegroup_id;
  std::string zonegroup_name;
  std::string zonegroup_new_name;

  std::list<std::string> endpoints;
  std::list<std::string> sync_from;
  std::list<std::string> sync_from_rm;
  std::map<std::string, std::string, ltstr_nocase> tier_config_add;
};

class RgwAdminZonegroupPlacementCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  RgwAdminZonegroupPlacementCommandsHandler(std::vector<const char*>& args,
                                            const std::vector<std::string>& prefix, RGWRados* store,
                                            Formatter* formatter) : RgwAdminCommandGroupHandler(args, prefix,
                                                                                                {{"add",     OPT_ZONEGROUP_PLACEMENT_ADD},
                                                                                                 {"default", OPT_ZONEGROUP_PLACEMENT_DEFAULT},
                                                                                                 {"list",    OPT_ZONEGROUP_PLACEMENT_LIST},
                                                                                                 {"modify",  OPT_ZONEGROUP_PLACEMENT_MODIFY},
                                                                                                 {"rm",      OPT_ZONEGROUP_PLACEMENT_RM},},
                                                                                                store, formatter) {
    if (parse_command_and_parameters() == 0) {
      std::cout << "Parsed command: " << command << std::endl;
    }
  }

  ~RgwAdminZonegroupPlacementCommandsHandler() override = default;

  RgwAdminCommandGroup get_type() const override { return ZONEGROUP_PLACEMENT; }

  int execute_command() override {
    switch (command) {
      case OPT_ZONEGROUP_PLACEMENT_ADD :
        return handle_opt_zonegroup_placement_add();
      case OPT_ZONEGROUP_PLACEMENT_DEFAULT :
        return handle_opt_zonegroup_placement_default();
      case OPT_ZONEGROUP_PLACEMENT_LIST :
        return handle_opt_zonegroup_placement_list();
      case OPT_ZONEGROUP_PLACEMENT_MODIFY :
        return handle_opt_zonegroup_placement_modify();
      case OPT_ZONEGROUP_PLACEMENT_RM :
        return handle_opt_zonegroup_placement_rm();
      default:
        return EINVAL;
    }
  }

private:
  int parse_command_and_parameters() override;

  int handle_opt_zonegroup_placement_add() {
    return ::handle_opt_zonegroup_placement_add(placement_id, zonegroup_id, zonegroup_name, tags,
                                                g_ceph_context, store, formatter);
  }

  int handle_opt_zonegroup_placement_default() {
    return ::handle_opt_zonegroup_placement_default(placement_id, zonegroup_id, zonegroup_name,
                                                    g_ceph_context, store, formatter);
  }

  int handle_opt_zonegroup_placement_list() {
    return ::handle_opt_zonegroup_placement_list(zonegroup_id, zonegroup_name, g_ceph_context,
                                                 store, formatter);
  }

  int handle_opt_zonegroup_placement_modify() {
    return ::handle_opt_zonegroup_placement_modify(placement_id, zonegroup_id, zonegroup_name,
                                                   tags, tags_add, tags_rm, g_ceph_context, store,
                                                   formatter);
  }

  int handle_opt_zonegroup_placement_rm() {
    return ::handle_opt_zonegroup_placement_rm(placement_id, zonegroup_id, zonegroup_name,
                                               g_ceph_context, store, formatter);
  }

  std::string placement_id;
  std::string zonegroup_id;
  std::string zonegroup_name;

  std::list<std::string> tags;
  std::list<std::string> tags_add;
  std::list<std::string> tags_rm;
};



#endif //CEPH_RGW_ADMIN_MULTISITE_H
