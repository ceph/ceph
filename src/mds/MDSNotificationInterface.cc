#include "MDSNotificationInterface.h"

MDSNotificationManager *MDSNotificationManager::manager = nullptr;

MDSNotificationManager::MDSNotificationManager()
    : runner(&MDSNotificationManager::run, this) {}

void MDSNotificationManager::create() {
  if (manager == nullptr) {
    manager = new MDSNotificationManager();
  }
}

void MDSNotificationManager::run() {
  while (true) {
    
    while (true) {
      std::unique_lock<std::mutex> lock(queue_mutex);
      if (message_queue.empty()) {
        break;
      }
      std::shared_ptr<bufferlist> message = message_queue.front();
      lock.unlock();
      publish(message);
    }
    uint64_t reply_count = polling();
    std::unique_lock<std::mutex> lock(queue_mutex);
    if (reply_count == 0 && message_queue.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(IDLE_TIME_MS));
    }
  }
}

bool MDSNotificationManager::interface_exist(uint64_t hash_key) {
  if (manager == nullptr) {
    return false;
  }
  std::shared_lock<std::shared_mutex> lock(manager->interface_mutex);
  return (manager->interfaces.find(hash_key) != manager->interfaces.end());
}

bool MDSNotificationManager::is_full() {
  if (manager == nullptr) {
    return false;
  }
  std::shared_lock<std::shared_mutex> lock(manager->interface_mutex);
  return (manager->interfaces.size() >= MAX_CONNECTIONS_DEFAULT);
}

void MDSNotificationManager::add_interface(
    uint64_t hash_key, std::shared_ptr<MDSNotificationInterface> interface) {
  if (manager == nullptr) {
    return;
  }
  std::unique_lock<std::shared_mutex> lock(manager->interface_mutex);
  manager->interfaces[hash_key] = interface;
}

int MDSNotificationManager::send(std::shared_ptr<bufferlist> &message) {
  if (manager == nullptr) {
    return -EACCES;
  }
  std::unique_lock<std::mutex> lock(manager->queue_mutex);
  if (manager->message_queue.size() >= MAX_QUEUE_DEFAULT) {
    return -EBUSY;
  }
  manager->message_queue.push(message);
  return 0;
}

uint64_t MDSNotificationManager::publish(std::shared_ptr<bufferlist> &message) {
  std::shared_lock<std::shared_mutex> lock(interface_mutex);
  uint64_t reply_count = 0;
  for (auto &[key, interface] : interfaces) {
    reply_count += interface->publish_internal(message);
  }
  return reply_count;
}

uint64_t MDSNotificationManager::polling() {
  std::shared_lock<std::shared_mutex> lock(interface_mutex);
  uint64_t reply_count = 0;
  for (auto &[key, interface] : interfaces) {
    reply_count += interface->poll();
  }
  return reply_count;
}
