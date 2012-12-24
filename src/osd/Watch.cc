#include "include/types.h"

#include "PG.h"
#include "ReplicatedPG.h"

#include "Watch.h"

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

void Watch::C_WatchTimeout::finish(int r)
{
  osd->handle_watch_timeout(obc, static_cast<ReplicatedPG *>(pg), entity,
			    expire);
}

