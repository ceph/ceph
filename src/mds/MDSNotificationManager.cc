#include "MDSNotificationManager.h"
#include "include/uuid.h"
#define dout_subsys ceph_subsys_mds
#define ALL_BIT_ON_MASK 1048575
#define ALL_BIT_ON_MASK_EXCEPT_ONLY_DIR 524287


MDSNotificationManager::MDSNotificationManager(MDSRank *mds)
    : cct(mds->cct), mds(mds), cur_notification_seq_id(0) {
#ifdef WITH_CEPHFS_NOTIFICATION
  uuid_d uid;
  uid.generate_random();
  session_id = uid.to_string();
  kafka_manager = std::make_unique<MDSKafkaManager>(mds);
  udp_manager = std::make_unique<MDSUDPManager>(mds);
#endif
}

void MDSNotificationManager::init() {
#ifdef WITH_CEPHFS_NOTIFICATION
  int r = kafka_manager->init();
  if (r < 0) {
    kafka_manager = nullptr;
  }
  r = udp_manager->init();
  if (r < 0) {
    udp_manager = nullptr;
  }
#endif
}

int MDSNotificationManager::dispatch(const cref_t<Message> &m) {
#ifdef WITH_CEPHFS_NOTIFICATION
  if (m->get_type() == MSG_MDS_NOTIFICATION_INFO_KAFKA_TOPIC) {
    const auto &req = ref_cast<MNotificationInfoKafkaTopic>(m);
    if (!req->is_remove) {
      add_kafka_topic(req->topic_name, req->endpoint_name, req->broker,
                      req->use_ssl, req->user, req->password, req->ca_location,
                      req->mechanism, false, false);
    } else {
      remove_kafka_topic(req->topic_name, req->endpoint_name, false, false);
    }
    return 0;
  } else if (m->get_type() == MSG_MDS_NOTIFICATION_INFO_UDP_ENDPOINT) {
    const auto &req = ref_cast<MNotificationInfoUDPEndpoint>(m);
    if (!req->is_remove) {
      add_udp_endpoint(req->name, req->ip, req->port, false, false);
    } else {
      remove_udp_endpoint(req->name, false, false);
    }
    return 0;
  }
  return -CEPHFS_EOPNOTSUPP;
#else
  return -CEPHFS_EOPNOTSUPP;
#endif
}

int MDSNotificationManager::add_kafka_topic(
    const std::string &topic_name, const std::string &endpoint_name,
    const std::string &broker, bool use_ssl, const std::string &user,
    const std::string &password, const std::optional<std::string> &ca_location,
    const std::optional<std::string> &mechanism, bool write_into_disk,
    bool send_peers) {
#ifdef WITH_CEPHFS_NOTIFICATION
  if (!kafka_manager) {
    ldout(cct, 1)
        << "Kafka topic '" << topic_name
        << "' creation failed as kafka manager is not initialized correctly"
        << dendl;
    return -CEPHFS_EFAULT;
  }
  int r = kafka_manager->add_topic(topic_name, endpoint_name,
                                   MDSKafkaConnection(broker, use_ssl, user,
                                                      password, ca_location,
                                                      mechanism),
                                   write_into_disk);
  if (send_peers && r == 0) {
    auto m = make_message<MNotificationInfoKafkaTopic>(
        topic_name, endpoint_name, broker, use_ssl, user, password, ca_location,
        mechanism, false);
    mds->send_to_peers(m);
  }
  return r;
#else
  return -CEPHFS_EOPNOTSUPP;
#endif
}

int MDSNotificationManager::remove_kafka_topic(const std::string &topic_name,
                                               const std::string &endpoint_name,
                                               bool write_into_disk,
                                               bool send_peers) {
#ifdef WITH_CEPHFS_NOTIFICATION
  if (!kafka_manager) {
    ldout(cct, 1)
        << "Kafka topic '" << topic_name
        << "' removal failed as kafka manager is not initialized correctly"
        << dendl;
    return -CEPHFS_EFAULT;
  }
  int r =
      kafka_manager->remove_topic(topic_name, endpoint_name, write_into_disk);
  if (send_peers && r == 0) {
    auto m = make_message<MNotificationInfoKafkaTopic>(topic_name,
                                                       endpoint_name, true);
    mds->send_to_peers(m);
  }
  return r;
#else
  return -CEPHFS_EOPNOTSUPP;
#endif
}

int MDSNotificationManager::add_udp_endpoint(const std::string &name,
                                             const std::string &ip, int port,
                                             bool write_into_disk,
                                             bool send_peers) {
#ifdef WITH_CEPHFS_NOTIFICATION
  if (!udp_manager) {
    ldout(cct, 1)
        << "UDP endpoint '" << name
        << "' creation failed as udp manager is not initialized correctly"
        << dendl;
    return -CEPHFS_EFAULT;
  }
  int r = udp_manager->add_endpoint(name, MDSUDPConnection(ip, port),
                                    write_into_disk);
  if (send_peers && r == 0) {
    auto m = make_message<MNotificationInfoUDPEndpoint>(name, ip, port, false);
    mds->send_to_peers(m);
  }
  return r;
#else
  return -CEPHFS_EOPNOTSUPP;
#endif
}

int MDSNotificationManager::remove_udp_endpoint(const std::string &name,
                                                bool write_into_disk,
                                                bool send_peers) {
#ifdef WITH_CEPHFS_NOTIFICATION
  if (!udp_manager) {
    ldout(cct, 1)
        << "UDP endpoint '" << name
        << "' removal failed as udp manager is not initialized correctly"
        << dendl;
    return -CEPHFS_EFAULT;
  }
  int r = udp_manager->remove_endpoint(name, write_into_disk);
  if (send_peers && r == 0) {
    auto m = make_message<MNotificationInfoUDPEndpoint>(name, true);
    mds->send_to_peers(m);
  }
  return r;
#else
  return -CEPHFS_EOPNOTSUPP;
#endif
}

#ifdef WITH_CEPHFS_NOTIFICATION
void MDSNotificationManager::push_notification(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  if (kafka_manager) {
    kafka_manager->send(message);
  }
  if (udp_manager) {
    udp_manager->send(message);
  }
}
#endif

void MDSNotificationManager::push_notification(int32_t whoami, CInode *in,
                                               uint64_t notify_mask,
                                               bool projected, bool is_dir) {
#ifdef WITH_CEPHFS_NOTIFICATION
  std::string path;
  in->make_path_string(path, projected);
  std::shared_ptr<MDSNotificationMessage> message =
      std::make_shared<MDSNotificationMessage>(
          cur_notification_seq_id.fetch_add(1));
  uint64_t filter_mask = ALL_BIT_ON_MASK;
  if (is_dir) {
    filter_mask = cct->_conf.get_val<uint64_t>("mds_notification_dir_mask") |
                  CEPH_MDS_NOTIFY_ONLYDIR;
    notify_mask |= CEPH_MDS_NOTIFY_ONLYDIR;
  } else {
    filter_mask = cct->_conf.get_val<uint64_t>("mds_notification_file_mask");
  }
  notify_mask &= filter_mask;
  uint64_t check_mask = notify_mask & ALL_BIT_ON_MASK_EXCEPT_ONLY_DIR;
  if (check_mask) {
    message->create_message(whoami, session_id, notify_mask, path);
    push_notification(message);
  }
#endif
}

void MDSNotificationManager::push_notification_link(
    int32_t whoami, CInode *targeti, CDentry *destdn,
    uint64_t notify_mask_for_target, uint64_t notify_mask_for_link,
    bool is_dir) {
#ifdef WITH_CEPHFS_NOTIFICATION
  std::string target_path;
  targeti->make_path_string(target_path, true, nullptr);
  std::string link_path;
  destdn->make_path_string(link_path, true);
  std::shared_ptr<MDSNotificationMessage> message =
      std::make_shared<MDSNotificationMessage>(
          cur_notification_seq_id.fetch_add(1));
  uint64_t filter_mask = ALL_BIT_ON_MASK;
  if (is_dir) {
    filter_mask = cct->_conf.get_val<uint64_t>("mds_notification_dir_mask") |
                  CEPH_MDS_NOTIFY_ONLYDIR;
    notify_mask_for_target |= CEPH_MDS_NOTIFY_ONLYDIR;
    notify_mask_for_link |= CEPH_MDS_NOTIFY_ONLYDIR;
  } else {
    filter_mask = cct->_conf.get_val<uint64_t>("mds_notification_file_mask");
  }
  notify_mask_for_target &= filter_mask;
  notify_mask_for_link &= filter_mask;
  uint64_t check_mask = (notify_mask_for_target | notify_mask_for_link) &
                        ALL_BIT_ON_MASK_EXCEPT_ONLY_DIR;
  if (check_mask) {
    if (target_path == link_path) {
      message->create_message(whoami, session_id, notify_mask_for_link,
                              target_path);
      push_notification(message);
      return;
    }
    message->create_link_message(whoami, session_id, notify_mask_for_target,
                                 notify_mask_for_link, target_path, link_path);
    push_notification(message);
  }
#endif
}

void MDSNotificationManager::push_notification_move(int32_t whoami,
                                                    CDentry *srcdn,
                                                    CDentry *destdn,
                                                    bool is_dir) {
#ifdef WITH_CEPHFS_NOTIFICATION
  std::string dest_path, src_path;
  srcdn->make_path_string(src_path, true);
  destdn->make_path_string(dest_path, true);
  uint64_t mask = CEPH_MDS_NOTIFY_MOVE_SELF;
  uint64_t filter_mask = ALL_BIT_ON_MASK;
  if (is_dir) {
    mask |= CEPH_MDS_NOTIFY_ONLYDIR;
    filter_mask = cct->_conf.get_val<uint64_t>("mds_notification_dir_mask") |
                  CEPH_MDS_NOTIFY_ONLYDIR;
  } else {
    filter_mask = cct->_conf.get_val<uint64_t>("mds_notification_file_mask");
  }
  mask &= filter_mask;
  uint64_t check_mask = mask & ALL_BIT_ON_MASK_EXCEPT_ONLY_DIR;
  if (check_mask) {
    std::shared_ptr<MDSNotificationMessage> message =
        std::make_shared<MDSNotificationMessage>(
            cur_notification_seq_id.fetch_add(1));
    message->create_move_message(whoami, session_id, mask, src_path, dest_path);
    push_notification(message);
  }
#endif
}

void MDSNotificationManager::push_notification_snap(int32_t whoami, CInode *in,
                                                    const std::string &snapname,
                                                    uint64_t notify_mask,
                                                    bool is_dir) {
#ifdef WITH_CEPHFS_NOTIFICATION
  std::string path;
  in->make_path_string(path, true, nullptr);
  std::shared_ptr<MDSNotificationMessage> message =
      std::make_shared<MDSNotificationMessage>(
          cur_notification_seq_id.fetch_add(1));
  uint64_t filter_mask = ALL_BIT_ON_MASK;
  if (is_dir) {
    notify_mask |= CEPH_MDS_NOTIFY_ONLYDIR;
    filter_mask = cct->_conf.get_val<uint64_t>("mds_notification_dir_mask") |
                  CEPH_MDS_NOTIFY_ONLYDIR;
  } else {
    filter_mask = cct->_conf.get_val<uint64_t>("mds_notification_file_mask");
  }
  notify_mask &= filter_mask;
  uint64_t check_mask = notify_mask & ALL_BIT_ON_MASK_EXCEPT_ONLY_DIR;
  if (check_mask) {
    message->create_snap_message(whoami, session_id, notify_mask, path,
                                 std::string(snapname));
    push_notification(message);
  }
#endif
}