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

typedef struct subnet_addr {
	entity_addr_t addr;
	entity_addr_t mask;
} subnet_addr_t;

class Subnet
{
  subnet_addr_t subnet;
  bool valid;
  char *orig_str;
  
  void parse(const char *str);
public:
  Subnet(const char *str) : orig_str(NULL) {
    valid = false;
    parse(str);
  }
  ~Subnet() {
    free(orig_str);
  }

  bool is_valid() { return valid; }
  bool contains(entity_addr_t *addr);
  subnet_addr_t& get_subnet() { return subnet; }
};

#define GET_IP(addr) (addr)->in4_addr().sin_addr.s_addr

bool Subnet::contains(entity_addr_t *addr)
{
	bool ret;
	if (!valid)
		return false;

	dout(30) << "subnet: " << orig_str << dendl;

	dout(30) << hex << GET_IP(addr) << " --- " << GET_IP(&subnet.addr) << dec << dendl;
	dout(30) << hex <<  (GET_IP(addr) & GET_IP(&subnet.mask)) << " -- " << (GET_IP(&subnet.addr) & GET_IP(&subnet.mask)) << dec << dendl;

	ret = (GET_IP(addr) & GET_IP(&subnet.mask)) == (GET_IP(&subnet.addr) & GET_IP(&subnet.mask));

	dout(30) << "ret=" << ret << dendl;

	return ret;
}


void Subnet::parse(const char *str)
{
	unsigned char ip[4], mask[4];

	char *mask_str = strdup(str);
	int ret;

	dout(30) << "Subnet::parse str=" << str << dendl;

	orig_str = strdup(str);

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

	dout(30) << "parsed str=" << str << dendl;
	memcpy(&GET_IP(&subnet.addr), ip, 4);
	memcpy(&GET_IP(&subnet.mask), mask, 4);

	dout(30) << "--> " << (int)ip[0] << "." << (int)ip[1] << "." << (int)ip[2] << "." << (int)ip[3] << "/"
		<< (int)mask[0] << "." << (int)mask[1] << "." << (int)mask[2] << "." << (int)mask[3] << dendl;
	dout(30) << hex << GET_IP(&subnet.addr) << dec << dendl;

	valid = true;
}

class GroupEntry;

class GroupsManager {
	map<string,GroupEntry*> groups_map;

public:
	GroupEntry *get_group(const char  *name);
};

static GroupsManager groups_mgr;

#define GROUP_DEFINED_RDONLY 0x1

class GroupEntry {
  vector<Subnet *> subnets;
  vector<GroupEntry *> groups;
  int flags;
  bool readonly;
  
  void parse_single_addr(const char *str);
  void initialize();
public:
  GroupEntry() { initialize(); }
  GroupEntry(Subnet *subnet);
  GroupEntry(GroupEntry *);
  ~GroupEntry() {
  }

  void parse_addr_line(const char *str);
  bool get_readonly() { return readonly; }
  void set_readonly(bool ro) { flags |= GROUP_DEFINED_RDONLY; readonly = ro; }
  void init(ConfFile *cf, const char *section, const char *options);
  bool contains(entity_addr_t *addr);
  GroupEntry *get_properties(entity_addr_t *addr);
  bool is_valid();
};

void GroupEntry::initialize()
{
	readonly = false;
	flags = 0;
}

GroupEntry::GroupEntry(Subnet *subnet)
{
	initialize();

	if (subnet) {
		subnets.push_back(subnet);
	}
}

GroupEntry::GroupEntry(GroupEntry *group)
{
	initialize();

	groups.push_back(group);
}

void GroupEntry::parse_addr_line(const char *str)
{
	char *s = strdup(str);
	char *orig_s = s;
	char *val;

	dout(30) << "parse_addr_line str=" << str << dendl;

	do {
		val = strsep(&s, ", \t");
		if (*val) {
			GroupEntry *group = groups_mgr.get_group(val);
			if (group)
				groups.push_back(group);
		}
	} while (s);

	free(orig_s);
}

void GroupEntry::parse_single_addr(const char *str)
{
	Subnet *subnet = new Subnet(str);

	if (subnet->is_valid() )
		subnets.push_back(subnet);
	else
		delete subnet;
}

bool GroupEntry::is_valid()
{
	vector<Subnet *>::iterator sub_iter;
	vector<GroupEntry *>::iterator ent_iter;

	for (sub_iter = subnets.begin(); sub_iter != subnets.end(); ++sub_iter) {
		if ((*sub_iter)->is_valid())
			return true;
	}

	for (ent_iter = groups.begin(); ent_iter != groups.end(); ++ent_iter) {
		if ((*ent_iter)->is_valid())
			return true;
	}

	return false;
}

bool GroupEntry::contains(entity_addr_t *addr)
{
	vector<Subnet *>::iterator sub_iter;
	vector<GroupEntry *>::iterator ent_iter;

	for (sub_iter = subnets.begin(); sub_iter != subnets.end(); ++sub_iter) {
		if ((*sub_iter)->contains(addr))
			return true;
	}

	for (ent_iter = groups.begin(); ent_iter != groups.end(); ++ent_iter) {
		if ((*ent_iter)->contains(addr))
			return true;
	}

	return false;
}

GroupEntry *GroupEntry::get_properties(entity_addr_t *addr)
{
	GroupEntry *group = NULL;
	vector<Subnet *>::iterator sub_iter;
	vector<GroupEntry *>::iterator ent_iter;

	for (sub_iter = subnets.begin(); sub_iter != subnets.end(); ++sub_iter) {
		if ((*sub_iter)->contains(addr)) {
			return new GroupEntry(*this);
		}
	}

	for (ent_iter = groups.begin(); ent_iter != groups.end(); ++ent_iter) {
		group = (*ent_iter)->get_properties(addr);

		if (group) {
			/* override properties */
			if (flags & GROUP_DEFINED_RDONLY) {
				group->set_readonly(readonly);
			}
		}
	}

	return group;
}

GroupEntry *GroupsManager::get_group(const char *name)
{
	map<string, GroupEntry *>::iterator iter;
	GroupEntry *group = NULL, *orig_group;
	char *tmp = strdup(name);
	char *orig_tmp = tmp;
	char *group_name;
	char *options = NULL;
	Subnet *subnet = NULL;

	if (isdigit(*name)) {
		subnet = new Subnet(name);
	} else {
		if (*name == '%') {
			++name;
			++tmp;
		}

		if (*name == '\0')
			goto done;
	}

	/* first try to look the raw name.. if we have it,
	   just return what we've got */
	iter = groups_map.find(name);

	if (iter != groups_map.end()) {
		group = iter->second;
		goto done;
	}

	group_name = strsep(&tmp, "(");

	if (tmp) {
		options = strsep(&tmp, ")");
	}

	if (!options) {
		dout(30) << "** creating new group with no options (" << group_name << ")" << dendl;
		/* no options --> group_name = name, create a new
		   one and exit */
		group = new GroupEntry(subnet);
		subnet = NULL;
		groups_map[group_name] = group;
		goto done;
	}

	iter = groups_map.find(group_name);
	if (iter == groups_map.end() ) {
		orig_group = new GroupEntry(subnet);
		subnet = NULL;
		groups_map[group_name] = orig_group;
	} else {
		orig_group = iter->second;
	}

	dout(30) << "** creating new group with options (" << group_name <<  " options=" << options << ")" << dendl;
	group = new GroupEntry(orig_group);
	group->init(NULL, NULL, options);
	groups_map[name] = group;

done:
	free(orig_tmp);
	delete subnet;
	return group;
}

void GroupEntry::init(ConfFile *cf, const char *section, const char *options)
{
	char *group_str;
	int ret;
	bool rdonly;

	if (cf) {
		ret = cf->read(section, "copy", &group_str, NULL);
		if (ret) {
			GroupEntry *group = groups_mgr.get_group(group_str);

			groups.push_back(group);
		} else {
			ret = cf->read(section, "addr", &group_str, NULL);

			if (group_str) {
				parse_addr_line(group_str);
				free(group_str);
			}

			ret = cf->read(section, "read only", &rdonly, 0);

			if (ret)
				set_readonly(rdonly);
		}
	}

	if (options) {
		char *tmp = strdup(options);
		char *orig_tmp = tmp;
		char  *op;

		op = strsep(&tmp, ", ");

		while (op) {
			if (strcmp(op, "rw") == 0) {
				set_readonly(false);
			} else if (strcmp(op, "ro") == 0) {
				set_readonly(true);
			} else {
				derr(0) << "Error: unknown option '" << op << "'" << dendl;
			}
			op = strsep(&tmp, ", ");
		}

		free(orig_tmp);
	}
}

class ExportAddrEntry {
	GroupEntry *group;
	bool valid;
public:
	ExportAddrEntry(const char *str);
	~ExportAddrEntry();

	bool is_valid() { return (group && group->is_valid()); }
	bool is_authorized(entity_addr_t *addr);
	GroupEntry *properties(entity_addr_t *addr);
};

ExportAddrEntry::ExportAddrEntry(const char *str)
{
	group = new GroupEntry();

	group->parse_addr_line(str);
}

ExportAddrEntry::~ExportAddrEntry()
{
	delete group;
}

bool ExportAddrEntry::is_authorized(entity_addr_t *addr)
{
	GroupEntry *props = NULL;
	bool ret;

	if (!group)
		return false;

	if (group)
		props = group->get_properties(addr);

	ret = (props != NULL);
	delete props;

	return ret;
}

GroupEntry *ExportAddrEntry::properties(entity_addr_t *addr)
{
	GroupEntry *props = NULL;

	if (group)
		props = group->get_properties(addr);

	return props;
}

class ExportEntry {
	vector<ExportAddrEntry *> addr_vec;
public:
	ExportEntry() {}
	~ExportEntry();
	ExportEntry(const char *str);
	void init(const char *str);
	bool is_authorized(entity_addr_t *addr);
	GroupEntry *properties(entity_addr_t *addr);
};

ExportEntry::ExportEntry(const char *str)
{
	init(str);
}

ExportEntry::~ExportEntry()
{
	vector<ExportAddrEntry *>::iterator iter;

	for (iter = addr_vec.begin(); iter != addr_vec.end(); ++iter)
	{
		delete *iter;
	}
}

void ExportEntry::init(const char *str)
{
	char *val;
	char *s = strdup(str);
	char *orig_s = s;

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

GroupEntry *ExportEntry::properties(entity_addr_t *addr)
{
	vector<ExportAddrEntry *>::iterator iter, last_iter;

	last_iter = addr_vec.end();

	for (iter = addr_vec.begin(); iter != last_iter; ++iter) {
		ExportAddrEntry *ent = *iter;
		GroupEntry *props = ent->properties(addr);

		if (props)
			return props;
	}

	return NULL;
}

class ExportsManager {
	map<string,ExportEntry*> exports_map;

public:
	ExportEntry *get_export(const char  *name);
	~ExportsManager();
};

ExportEntry *ExportsManager::get_export(const char *name)
{
	map<string, ExportEntry *> ::iterator iter;
	ExportEntry *exp;

	iter = exports_map.find(name);

	if (iter == exports_map.end()) {
		exp = new ExportEntry();
		exports_map[name] = exp;
	} else {
		exp = iter->second;
	}

	return exp;
}

ExportsManager::~ExportsManager()
{
	for (map<string, ExportEntry *> ::iterator iter = exports_map.begin();
	     iter != exports_map.end();
	     ++iter)
		delete iter->second;
}

static ExportsManager exports_mgr;

ExportControl::~ExportControl()
{
	Mutex::Locker locker(lock);

	_cleanup();
}

void ExportControl::load(ConfFile *conf)
{
  Mutex::Locker locker(lock);
  int mnt_len = strlen(MOUNT_SEC_NAME);
  int grp_len = strlen(GROUP_SEC_NAME);
  int client_len = strlen(CLIENT_SEC_NAME);
  char *allow_str = NULL;
  char *allow_def = NULL;
  int ret;

#define EVERYONE "0.0.0.0/0"
   if (!conf)
      return;

  ret = conf->read("mount", "allow", &allow_def, EVERYONE);

  for (std::list<ConfSection*>::const_iterator p = conf->get_section_list().begin();
	p != conf->get_section_list().end();
	p++) {
    /* is it a 'mount' sections */
    if (strncmp(MOUNT_SEC_NAME, (*p)->get_name().c_str(), mnt_len) == 0) {
	const char *section = (*p)->get_name().c_str();
	char *tmp_sec = strdup(section);
	char *mnt;
	char *orig_tmp_sec = tmp_sec;
	char *copy_str = NULL;

	strsep(&tmp_sec, " ");
	mnt = tmp_sec;
	if (mnt) {
		ret = conf->read(section, "copy", &copy_str, NULL);

		if (ret) {
			ExportEntry *exp = exports_mgr.get_export(copy_str);

			exports[mnt] = exp;
		} else {
			allow_str = NULL;
			ret = conf->read(section, "allow", &allow_str, allow_def);

			/* it is guaranteed that there's only one space character */
			ExportEntry *exp = exports_mgr.get_export(mnt);
			exp->init(allow_str);
			exports[mnt] = exp;
			free(allow_str);
		}
	}

	free(orig_tmp_sec);
    } else if ((strncmp(GROUP_SEC_NAME, (*p)->get_name().c_str(), grp_len) == 0) ||
        (strncmp(CLIENT_SEC_NAME, (*p)->get_name().c_str(), client_len) == 0)) {
	/* it's either a 'client' or a 'group' section */
	const char *section = (*p)->get_name().c_str();
	char *tmp_sec = strdup(section);
	char *name;
	char *orig_tmp_sec = tmp_sec;

	/* it is guaranteed that there's only one space character */
	strsep(&tmp_sec, " ");
	name = tmp_sec;

	if (name) {
		GroupEntry *exp = groups_mgr.get_group(name);
		if (exp)
			exp->init(conf, section, NULL);
	}

	free(orig_tmp_sec);
    }
  }
}

bool ExportControl::is_authorized(entity_addr_t *addr, const char *path)
{
	ExportEntry *ent = _find(addr, path);

	if (ent)
		return ent->is_authorized(addr);

	return false;
}

GroupEntry *ExportControl::get_properties(entity_addr_t *addr, const char *path)
{
	ExportEntry *ent = _find(addr, path);

	if (ent)
		return ent->properties(addr);

	return false;
}

void ExportControl::put_properties(GroupEntry *props)
{
	delete props;
}

ExportEntry *ExportControl::_find(entity_addr_t *addr, const char *path)
{
	Mutex::Locker locker(lock);
	char *p = strdup(path);
	int len = strlen(p);
	ExportEntry *ent = NULL;
	
	map<string, ExportEntry *>::iterator iter, exp_end;

	/* only absolute paths! */
	if (*path != '/')
		goto out;

	if (exports.size() == 0)
		goto out;

	exp_end = exports.end();
	iter = exports.find(p);

	while (iter == exp_end) {

		do {
			if (len == 0)
				goto out;

			p[len] = '\0';
			len--;
		} while (len && p[len] != '/');

		if (len > 0)
			p[len] = '\0';

		iter = exports.find(p);
	}


	ent = iter->second;

out:
	free(p);
	return ent;
}

void ExportControl::_cleanup()
{
  for (map<string, ExportEntry*>::iterator iter = exports.begin();
       iter != exports.end();
       ++iter)
    delete iter->second;
}

#if 0

int test(ExportControl *ec, const char *ip, const char *path)
{
	Subnet sub(ip);
	GroupEntry *group;

	subnet_addr& addr = sub.get_subnet();

	cout << "addr " << ip << ": result=" << ec->is_authorized(&addr.addr, path) << std::endl;

	group = ec->get_properties(&addr.addr, path);

	if (group) {
		cout << "readonly=" << group->get_readonly() << std::endl;
	}

	return 0;
}


int main(int argc, char *argv[])
{
	ConfFile cf("ceph.conf");

	cf.parse();
	ExportControl ec;

	ec.load(&cf);
	test(&ec, "10.0.0.1", "/home");
	test(&ec, "10.0.1.1", "/home");
	test(&ec, "10.0.1.1", "/");
	test(&ec, "10.1.1.1", "/");
	test(&ec, "10.2.1.1", "/");
	test(&ec, "10.9.1.1", "/");
	test(&ec, "10.0.1.1", "/home");
}

#endif
