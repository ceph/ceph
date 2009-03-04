#ifndef __CONFUTILS_H
#define __CONFUTILS_H


#include <string.h>
#include <map>
#include <string>
#include <list>

class ConfLine {
	char *prefix;
	char *var;
	char *mid;
	char *val;
	char *suffix;
	char *section;
	char *norm_var;

	void _set(char **dst, const char *val);
public:
	ConfLine() : prefix(NULL), var(NULL), mid(NULL), val(NULL),
		   suffix(NULL), section(NULL), norm_var(NULL) {}
	~ConfLine();

	void set_prefix(const char *val);
	void set_var(const char *val);
	void set_mid(const char *val);
	void set_val(const char *val);
	void set_suffix(const char *val);
	void set_section(const char *val);

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
};

typedef std::map<std::string, ConfSection *> SectionMap;
typedef std::list<ConfSection *> SectionList;

class ConfFile {
	char *filename;
	bool auto_update;

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
	int _parse(char *filename, ConfSection **psection);
public:
	ConfFile(const char *fname) : filename(strdup(fname)), auto_update(false) {}
	~ConfFile();

	const SectionList& get_section_list() { return sections_list; }
	const char *get_filename() { return filename; }

	int parse();
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
	void set_auto_update(bool update) { auto_update = update; }
};

#endif
