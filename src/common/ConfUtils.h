#ifndef __CONFUTILS_H
#define __CONFUTILS_H


#include <string.h>
#include <map>
#include <string>
#include <list>

typedef std::map<std::string, struct conf_line *> ConfMap;
typedef std::list<struct conf_line *> ConfList;

class ConfFile;

class ConfSection
{
	friend class ConfFile;

	std::string name;
	ConfList conf_list;
	ConfMap conf_map;
public:
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

	struct conf_line *_find_var(const char *section, const char* var);
	struct conf_line *_add_var(const char *section, const char* var);

	template<typename T>
	int _read(const char *section, const char *var, T *val, T def_val);

	template<typename T>
	int _write(const char *section, const char *var, T val);
public:
	ConfFile(char *fname) : filename(strdup(fname)) {}
	~ConfFile() { free(filename); }

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
