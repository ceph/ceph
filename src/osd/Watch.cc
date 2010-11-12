#include "include/types.h"

#include <map>

#include "OSD.h"
#include "ReplicatedPG.h"
#include "Watch.h"

#include "config.h"

void Watch::add_notification(Notification *notif)
{
  notif->id = ++notif_id;

  notifs.insert(pair<entity_name_t, Notification *>(notif->name, notif));
  itn[notif->id] = notif;

  map<entity_name_t, WatcherState>::iterator iter = notif->watchers.begin();
  for (; iter != notif->watchers.end(); ++iter) {
    wtn.insert(pair<entity_name_t, Notification *>(iter->first, notif));
  }
}

void Watch::remove_notification(Notification *notif)
{
  map<uint64_t, Notification *>::iterator iter = itn.find(notif->id);
  if (iter != itn.end())
    itn.erase(iter);

  map<entity_name_t, WatcherState>::iterator witer;
  for (witer = notif->watchers.begin(); witer != notif->watchers.end(); ++iter) {
    const entity_name_t& watcher = witer->first;

    multimap<entity_name_t, Notification *>::iterator niter = wtn.find(watcher);
    for (; niter != wtn.end(); ++niter) {
      if (niter->second == notif) {
        wtn.erase(niter);
        break;
      }
    }
  }

  multimap<entity_name_t, Notification *>::iterator niter = notifs.find(notif->name);
  for (; niter != wtn.end(); ++niter) {
    if (niter->second == notif) {
      notifs.erase(niter);
      break;
    }
  }
}

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

