#include "include/types.h"

#include <map>

#include "OSD.h"
#include "ReplicatedPG.h"
#include "Watch.h"

#include "config.h"

bool Watch::ack_notification(entity_name_t& watcher, Notification *notif)
{
  map<entity_name_t, WatcherState>::iterator iter = notif->watchers.find(watcher);

  if (iter == notif->watchers.end()) // client was not suppose to ack this notification
    return false;

  notif->watchers.erase(iter);

  return notif->watchers.empty(); // true if there are no more watchers
}

void Watch::C_NotifyTimeout::finish(int r)
{
  osd->handle_notify_timeout(notif);
}

