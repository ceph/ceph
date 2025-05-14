#pragma once
#include "CDentry.h"
#include "CInode.h"
#include "MDSRank.h"
#include "common/ceph_context.h"
#include "include/buffer.h"
#include <bits/stdc++.h>

#ifdef WITH_CEPHFS_NOTIFICATION
#include "MDSKafka.h"
#include "MDSNotificationMessage.h"
#include "MDSUDPEndpoint.h"
#include "messages/MNotificationInfoKafkaTopic.h"
#include "messages/MNotificationInfoUDPEndpoint.h"

class MDSKafkaManager;
class MDSUDPManager;
#endif

class MDSNotificationManager {
public:
  MDSNotificationManager(MDSRank *mds);
  void init();

  // incoming notification endpoints
  int dispatch(const cref_t<Message> &m);
  int add_kafka_topic(const std::string &topic_name,
                      const std::string &endpoint_name,
                      const std::string &broker, bool use_ssl,
                      const std::string &user, const std::string &password,
                      const std::optional<std::string> &ca_location,
                      const std::optional<std::string> &mechanism,
                      bool write_into_disk, bool send_peers);
  int remove_kafka_topic(const std::string &topic_name,
                         const std::string &endpoint_name, bool write_into_disk,
                         bool send_peers);
  int add_udp_endpoint(const std::string &name, const std::string &ip, int port,
                       bool write_into_disk, bool send_peers);
  int remove_udp_endpoint(const std::string &name, bool write_into_disk,
                          bool send_peers);

  void push_notification(int32_t whoami, CInode *in, uint64_t notify_mask,
                         bool projected, bool is_dir);
  void push_notification_link(int32_t whoami, CInode *targeti, CDentry *destdn,
                              uint64_t notify_mask_for_target,
                              uint64_t notify_mask_for_link, bool is_dir);
  void push_notification_move(int32_t whoami, CDentry *srcdn, CDentry *destdn,
                              bool is_dir);
  void push_notification_snap(int32_t whoami, CInode *in,
                              const std::string &snapname, uint64_t notify_mask,
                              bool is_dir);

private:
#ifdef WITH_CEPHFS_NOTIFICATION
  std::unique_ptr<MDSKafkaManager> kafka_manager;
  std::unique_ptr<MDSUDPManager> udp_manager;
  void
  push_notification(const std::shared_ptr<MDSNotificationMessage> &message);
#endif

  CephContext *cct;
  std::atomic<uint64_t> cur_notification_seq_id;
  std::string session_id;
  MDSRank *mds;
};