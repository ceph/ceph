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

	void _set(char **dst, const char *val);
public:
	ConfLine() : prefix(NULL), var(NULL), mid(NULL), val(NULL),
		   suffix(NULL), section(NULL) {}
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
};

typedef std::map<std::string, ConfSection *> SectionMap;
typedef std::list<ConfSection *> SectionList;

class ConfFile {
	int fd;
	char *filename;

	SectionMap sections;
	SectionList sections_list;
	ConfList global_list;

	ConfLine *_find_var(const char *section, const char* var);
	ConfLine *_add_var(const char *section, const char* var);

	template<typename T>
	int _read(const char *section, const char *var, T *val, T def_val);

	template<typename T>
	int _write(const char *section, const char *var, T val);
public:
	ConfFile(char *fname) : filename(strdup(fname)) {}
	~ConfFile();

	int parse();
	int read(const char *section, const char *var, int *val, int def_val);
	int read(const char *section, const char *var, bool *val, bool def_val);
/* 	int read(const char *section, const char *var, char *val, int size, char *def_val); */
	int read(const char *section, const char *var, char **val, char *def_val); /* allocates new val */
	int read(const char *section, const char *var, float *val, float def_val);

	int write(const char *section, const char *var, int val);
	int write(const char *section, const char *var, bool val);
	int write(const char *section, const char *var, float val);
	int write(const char *section, const char *var, char *val);

	void dump();
};

#endif
