#ifndef __FILEPATH_H
#define __FILEPATH_H

#include <iostream>
#include <ext/rope>
using namespace std;

class filepath {
  string path;
  vector<string> bits;

  void parse() {
	bits.clear();
	int off = 0;
	while (off < path.length()) {
	  // skip trailing/duplicate slash(es)
	  int nextslash = path.find('/', off);
	  if (nextslash == off) {
		off++;
		continue;
	  }
	  if (nextslash < 0) 
		nextslash = path.length();  // no more slashes
	  
	  bits.push_back( path.substr(off,nextslash-off) );
	  off = nextslash+1;
	}
  }

 public:
  filepath() {}
  filepath(string& s) {
	set_path(s);
  }
  filepath(char* s) {
	set_path(s);
  }

  void set_path(string& s) {
	path = s;
	parse();
  }
  void set_path(const char *s) {
	path = s;
	parse();
  }

  string& get_path() {
	return path;
  }
  const char *c_str() {
	return path.c_str();
  }


  filepath subpath(int s) {
	filepath t;
	for (int i=s; i<bits.size(); i++) {
	  t.add_dentry(bits[i]);
	}
	return t;
  }
  void add_dentry(string& s) {
	bits.push_back(s);
	if (path.length())
	  path += "/";
	path += s;
  }

  void clear() {
    path = "";
    bits.clear();
  }

  string& operator[](int i) {
	return bits[i];
  }

  string& last_bit() {
	return bits[ bits.size()-1 ];
  }

  int depth() {
	return bits.size();
  }
  bool empty() {
    return bits.size() == 0;
  }

  
  crope _rope() {
    crope r;
    char n = bits.size();
    r.append((char*)&n, sizeof(char));
    for (vector<string>::iterator it = bits.begin();
         it != bits.end();
         it++) { 
      r.append((*it).c_str());
      r.append((char)0);
    }
    return r;
  }

  int _unrope(crope r, int off = 0) {
    clear();
    char n;
    r.copy(off, sizeof(char), (char*)&n);
    off += sizeof(char);
    for (int i=0; i<n; i++) {
      string s = r.c_str() + off;
      off += s.length() + 1;
	  add_dentry(s);
    }
    return off;
  }

};

inline ostream& operator<<(ostream& out, filepath& path)
{
  return out << path.get_path();
}

#endif
