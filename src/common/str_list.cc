#include <iostream>
#include <list>
#include <set>

using namespace std;

static bool get_next_token(string s, size_t& pos, string& token)
{
  int start = s.find_first_not_of(" \t", pos);
  int end;

  if (s[start]== ',') {
    end = start + 1;
  } else {
    end = s.find_first_of(";,= \t", start+1);
  }

  if (start < 0) {
    return false; 
  }

  if (end < 0) {
    end=s.size();
  }

  token = s.substr(start, end - start);

  pos = end;

  return true;
}

bool get_str_list(string& str, list<string>& str_list)
{
  size_t pos = 0;
  string token;

  str_list.clear();

  while (pos < str.size()) {
    if (get_next_token(str, pos, token)) {
      if (token.compare(",") != 0 && token.size() > 0) {
        str_list.push_back(token);
      }
    }
  }

  return true;
}


bool get_str_set(string& str, set<string>& str_set)
{
  size_t pos = 0;
  string token;

  str_set.clear();

  while (pos < str.size()) {
    if (get_next_token(str, pos, token)) {
      if (token.compare(",") != 0 && token.size() > 0) {
        str_set.insert(token);
      }
    }
  }

  return true;
}


