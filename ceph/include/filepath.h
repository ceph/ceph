#ifndef __FILEPATH_H
#define __FILEPATH_H

#include <iostream>
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
	path = s;
	parse();
  }
  filepath(char* s) {
	path = s;
	parse();
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

  string& get_path() {
	return path;
  }
};

inline ostream& operator<<(ostream& out, filepath& path)
{
  return out << path.get_path();
}

#endif
