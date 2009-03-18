// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "ExportControl.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

using namespace std;

#include "config.h"
#include "common/ConfUtils.h"
#include "common/common_init.h"


/*
 * a host addr entry: ip[/{maskbits}
 * examples:
 *    1.2.3.4
 *    1.2.3.4/16
 */
class ExportAddrEntry {
	entity_addr_t ent_addr;
	entity_addr_t ent_mask;
	bool valid;
public:
	ExportAddrEntry(const char *str);

	bool is_authorized(entity_addr_t *addr);
	bool is_valid() { return valid; }
};

ExportAddrEntry::ExportAddrEntry(const char *str)
{
	unsigned char ip[4], mask[4];

	char *mask_str = strdup(str);
	int ret;

	valid = false;

	ret = sscanf(str, "%hhd.%hhd.%hhd.%hhd/%s", &ip[0], &ip[1], &ip[2], &ip[3], mask_str);

	if (ret < 4) {
		return;
	} else if (ret == 4) {
		mask[0] = mask[1] = mask[2] = mask[3] = 255;
	} else {
		ret = sscanf(mask_str, "%hhd.%hhd.%hhd.%hhd", &mask[0], &mask[1], &mask[2], &mask[3]);

		if (ret == 1) {
			int mask_bits = atoi(mask_str);
			int i;
			int mask_int = 0;

			for (i=0; i<32; i++) {
				mask_int <<= 1;
				if (i < mask_bits)
					mask_int |= 1;
			}
			mask[0] = (mask_int & 0xff000000) >> 24;
			mask[1] = (mask_int & 0x00ff0000) >> 16;
			mask[2] = (mask_int & 0x0000ff00) >> 8;
			mask[3] = (mask_int & 0x000000ff);
		} else if (ret != 4) {
			return;
		}
		
	}
	memcpy(&ent_addr.ipaddr.sin_addr, ip, 4);
	memcpy(&ent_mask.ipaddr.sin_addr, mask, 4);

	valid = true;
}

#define GET_IP(addr) (addr)->ipaddr.sin_addr.s_addr

bool ExportAddrEntry::is_authorized(entity_addr_t *addr)
{
	if (!valid)
		return true;

	return (GET_IP(addr) & GET_IP(&ent_mask)) == (GET_IP(&ent_addr) & GET_IP(&ent_mask));
}

class ExportEntry {
	char *name;
	ExportAddrEntry *addr;
	vector<ExportAddrEntry *> addr_vec;
public:
	ExportEntry(const char *str);
	bool is_authorized(entity_addr_t *addr);
};

ExportEntry::ExportEntry(const char *str)
{
	char *s = strdup(str);
	char *orig_s = s;
	char *val;

	do {
		val = strsep(&s, ", \t");

		ExportAddrEntry *ent = new ExportAddrEntry(val);

		if (ent->is_valid()) {
			addr_vec.push_back(ent);
		} else {
			delete ent;
		}
	} while (s);

	free(orig_s);
}

bool ExportEntry::is_authorized(entity_addr_t *addr)
{
	vector<ExportAddrEntry *>::iterator iter, last_iter;

	last_iter = addr_vec.end();

	for (iter = addr_vec.begin(); iter != last_iter; ++iter) {
		ExportAddrEntry *ent = *iter;

		if (ent->is_authorized(addr))
			return true;
	}

	return false;
}

ExportControl::~ExportControl()
{
	Mutex::Locker locker(lock);

	_cleanup();
}

void ExportControl::load(ConfFile *conf)
{
  Mutex::Locker locker(lock);
  int len = strlen(MNT_SEC_NAME);
  char *allow_str = NULL;
  char *allow_def = NULL;
  int ret;

  ret = conf->read("mount", "allow client", &allow_def, "0.0.0.0/0");

  for (std::list<ConfSection*>::const_iterator p = conf->get_section_list().begin();
	p != conf->get_section_list().end();
	p++) {
    if (strncmp(MNT_SEC_NAME, (*p)->get_name().c_str(), len) == 0) {
	const char *section = (*p)->get_name().c_str();
	char *tmp_sec = strdup(section);
	char *mnt;
	char *orig_tmp_sec = tmp_sec;

	allow_str = NULL;
	ret = conf->read(section, "allow client", &allow_str, allow_def);

	/* it is guaranteed that there's only one space character */
	strsep(&tmp_sec, " ");
	mnt = tmp_sec;

	if (mnt) {
		ExportEntry *exp = new ExportEntry(allow_str);
		free(allow_str);

		exports[strdup(mnt)] = exp;
	}


	free(orig_tmp_sec);
    }
  }
}

bool ExportControl::is_authorized(entity_addr_t *addr, const char *path)
{
	Mutex::Locker locker(lock);
	char *p = strdup(path);
	int len = strlen(p);
	
	map<const char *, ExportEntry *, ltstr>::iterator iter, exp_end;

	/* only absolute paths! */
	if (*path != '/')
		return false;

	exp_end = exports.end();
	iter = exports.find(p);

	while (iter == exp_end) {

		do {
			p[len] = '\0';
			len--;
		} while (len && p[len] != '/');

		if (len > 0)
			p[len] = '\0';

		iter = exports.find(p);
	}

	free(p);

	ExportEntry *ent = iter->second;

	return ent->is_authorized(addr);
}

void ExportControl::_cleanup()
{
	map<const char *, ExportEntry *, ltstr>::iterator iter, exp_end;

	exp_end = exports.end();

	for (iter = exports.begin(); iter != exp_end; ++iter) {
		const char *mnt = iter->first;
		ExportEntry *ent = iter->second;

		free((void *)mnt);
		delete ent;
	}
}

