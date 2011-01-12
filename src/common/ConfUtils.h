#ifndef CEPH_CONFUTILS_H
#define CEPH_CONFUTILS_H


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

	char *(*post_process_func)(const char *);

	SectionMap sections;
	SectionList sections_list;
	ConfList global_list;

	ConfLine *_find_var(const char *section, const char* var);
	ConfLine *_add_var(const char *section, const char* var);

	template<typename T>
	int _read(const char *section, const char *var, T *val, const T def_val);

	template<typename T>
	int _write(const char *section, const char *var, const T val);

	ConfSection *_add_section(const char *section, ConfLine *cl);
	void _dump(int fd);
	bool _parse(const char *filename, ConfSection **psection);
	void common_init();

	int _read(int fd, char *buf, size_t size);
	int _open();
	int _close(int fd);
public:
        ConfFile(const char *fname);
        ConfFile(ceph::bufferlist *bl);
	~ConfFile();

	const SectionList& get_section_list() { return sections_list; }
	const char *get_filename() { return filename; }

	bool parse();
	int read(const char *section, const char *var, int *val, int def_val);
	int read(const char *section, const char *var, unsigned int *val, unsigned int def_val);
	int read(const char *section, const char *var, long long *val, long long def_val);
	int read(const char *section, const char *var, unsigned long long *val, unsigned long long def_val);
	int read(const char *section, const char *var, bool *val, bool def_val);
	int read(const char *section, const char *var, char **val, const char *def_val);
	int read(const char *section, const char *var, float *val, float def_val);
	int read(const char *section, const char *var, double *val, double def_val);

	int write(const char *section, const char *var, int val);
	int write(const char *section, const char *var, unsigned int val);
	int write(const char *section, const char *var, long long val);
	int write(const char *section, const char *var, unsigned long long val);
	int write(const char *section, const char *var, bool val);
	int write(const char *section, const char *var, float val);
	int write(const char *section, const char *var, double val);
	int write(const char *section, const char *var, char *val);

	void dump();
	int flush();
	void set_post_process_func(char *(*func)(const char *)) {post_process_func = func; };
	void set_global(bool global) { default_global = global; }
};

#endif
