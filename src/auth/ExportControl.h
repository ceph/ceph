#ifndef CEPH_EXPORTCONTROL_H
#define CEPH_EXPORTCONTROL_H

#include <map>
#include "config.h"
#include "common/Mutex.h"

#define MOUNT_SEC_NAME "mount"
#define GROUP_SEC_NAME "group"
#define CLIENT_SEC_NAME "client"

class ExportEntry;
class ConfFile;
class GroupEntry;


class ExportControl {
	map<string,ExportEntry*> exports;
	Mutex lock;

	void _cleanup();	
	ExportEntry *_find(entity_addr_t *addr, const char *path);
public:
	ExportControl() : lock("ExportControl", false, false, false) {}
	~ExportControl();

	void load(ConfFile *cf);

	bool is_authorized(entity_addr_t *addr, const char *path);
	GroupEntry *get_properties(entity_addr_t *addr, const char *path);
	void put_properties(GroupEntry *props);
};


#endif
