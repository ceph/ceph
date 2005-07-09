#ifndef __CLIENT_TRACE_H
#define __CLIENT_TRACE_H

#include <cassert>
#include <list>
#include <string>
using namespace std;

/*

 this class is more like an iterator over a constant tokenlist (which 
 is protected by a mutex, see Trace.cc)

 */

class Trace {
  class TokenList *tl;
  
 public:
  Trace(const char* filename);
  ~Trace();
  
  list<const char*>& get_list();

  list<const char*>::iterator _cur;
  list<const char*>::iterator _end;

  void start() {
	_cur = get_list().begin();
	_end = get_list().end();
	ns = 0;
  }

  char strings[10][200];
  int ns;
  const char *get_string(const char *prefix = 0) {
	assert(_cur != _end);
	const char *s = *_cur;
	_cur++;
	if (prefix) {
	  if (strstr(s, "/prefix") == s ||
		  strstr(s, "/prefix") == s+1) {
		strcpy(strings[ns], prefix);
		strcpy(strings[ns] + strlen(prefix),
			   s + strlen("/prefix"));
		s = (const char*)strings[ns];
		ns++;
		if (ns == 10) ns = 0;
	  }
	} 
	return s;
  }
  __int64_t get_int() {
	return atoll(get_string());
  }
  bool end() {
	return _cur == _end;
  }
};

#endif
