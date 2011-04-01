#ifndef CEPH_CONFUTILS_H
#define CEPH_CONFUTILS_H


#include <deque>
#include <string.h>
#include <map>
#include <string>
#include <list>

#include "include/buffer.h"
#include "common/Mutex.h"

class ConfLine {
	char *prefix;
	char *var;
	char *mid;
	char *val;
	char *suffix;
	char *section;
	char *norm_var;

	void _set(char **dst, const char *val, bool alloc);
public:
	ConfLine() : prefix(NULL), var(NULL), mid(NULL), val(NULL),
		   suffix(NULL), section(NULL), norm_var(NULL) {}
	~ConfLine();

	void set_prefix(const char *val, bool alloc = true);
	void set_var(const char *val, bool alloc = true);
	void set_mid(const char *val, bool alloc = true);
	void set_val(const char *val, bool alloc = true);
	void set_suffix(const char *val, bool alloc = true);
	void set_section(const char *val, bool alloc = true);

	char *get_prefix() { return prefix; }
	char *get_var() { return var; }
	char *get_mid() { return mid; }
	char *get_val() { return val; }
	char *get_suffix() { return suffix; }
	char *get_section() { return section; }
	char *get_norm_var();

	int output(char *line, int max_len);
};

typedef std::map<std::string, ConfLine *> ConfMap;
typedef std::list<ConfLine *> ConfList;

class ConfFile;

class ConfSection
{
	friend class ConfFile;

	std::string name;
	ConfList conf_list;
	ConfMap conf_map;
public:
	~ConfSection();
	ConfSection(std::string sec_name) : name(sec_name) { }

	const std::string& get_name() { return name; }
        ConfList& get_list() { return conf_list; }
};

typedef std::map<std::string, ConfSection *> SectionMap;
typedef std::list<ConfSection *> SectionList;

class ConfFile {
	char *filename;

	ceph::bufferlist *pbl;
	size_t buf_pos;
	Mutex parse_lock;
	bool default_global;

	void (*post_process_func)(std::string &);

	SectionMap sections;
	SectionList sections_list;
	ConfList global_list;

	ConfLine *_add_var(const char *section, const char* var);

	ConfSection *_add_section(const char *section, ConfLine *cl);
	void _dump(int fd);
	int _parse(const char *filename, ConfSection **psection);
	void common_init();

	int _read(int fd, char *buf, size_t size);
	int _close(int fd);
	int parse();
public:
        ConfFile();
	~ConfFile();

	const SectionList& get_section_list() { return sections_list; }
	const char *get_filename() { return filename; }

	ConfLine *_find_var(const char *section, const char* var);

	int parse_file(const char *fname, std::deque<std::string> *parse_errors);
	int parse_bufferlist(ceph::bufferlist *bl,
			     std::deque<std::string> *parse_errors);

	int read(const char *section, const char *var, std::string &val);

	void dump();
	int flush();
};

#endif
