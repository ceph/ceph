//
// Created by cache-nez on 07.01.18.
//

#ifndef CEPH_RGW_ADMIN_MULTISITE_H
#define CEPH_RGW_ADMIN_MULTISITE_H

#include <boost/optional.hpp>
#include "rgw_rest_conn.h"

// Period, realm, zone , zonegroup , data sync, metadata sync

/// search each zonegroup for a connection
boost::optional<RGWRESTConn> get_remote_conn(RGWRados *store, const RGWPeriodMap& period_map,
                                             const std::string& remote);

int send_to_url(const string& url, const string& access, const string& secret, req_info& info, bufferlist& in_data,
                JSONParser& parser);

int send_to_remote_or_url(RGWRESTConn *conn, const string& url, const string& access, const string& secret,
                          req_info& info, bufferlist& in_data, JSONParser& parser);

int commit_period(RGWRados *store, RGWRealm& realm, RGWPeriod& period, string remote, const string& url,
                  const string& access, const string& secret, bool force);

int update_period(RGWRados *store, const string& realm_id, const string& realm_name, const string& period_id,
                  const string& period_epoch, bool commit, const string& remote, const string& url,
                  const string& access, const string& secret, Formatter *formatter, bool force);

int do_period_pull(RGWRados *store, RGWRESTConn *remote_conn, const string& url,
                   const string& access_key, const string& secret_key,
                   const string& realm_id, const string& realm_name,
                   const string& period_id, const string& period_epoch,
                   RGWPeriod *period);


#endif //CEPH_RGW_ADMIN_MULTISITE_H
