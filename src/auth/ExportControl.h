#ifndef __EXPORTCONTROL_H
#define __EXPORTCONTROL_H

#include <map>
#include "config.h"
#include "common/Mutex.h"

#define MNT_SEC_NAME "mount"

class ExportEntry;
class ConfFile;


class ExportControl {
	std::map<const char *, ExportEntry *, ltstr> exports;
	Mutex lock;

	void _cleanup();	
public:
	ExportControl() : lock("export_control") {}
	~ExportControl();

	void load(ConfFile *cf);

	bool is_authorized(entity_addr_t *addr, const char *path);
};


#endif
