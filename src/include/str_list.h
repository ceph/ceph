#ifndef CEPH_STRLIST_H
#define CEPH_STRLIST_H

#include <list>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace ceph {

/// Split a string using the given delimiters, passing each piece as a
/// (non-null-terminated) std::string_view to the callback.
template <typename Func> // where Func(std::string_view) is a valid call
void for_each_substr(std::string_view s, const char *delims, Func&& f)
{
  auto pos = s.find_first_not_of(delims);
  while (pos != s.npos) {
    s.remove_prefix(pos); // trim delims from the front
    auto end = s.find_first_of(delims);
    f(s.substr(0, end));
    pos = s.find_first_not_of(delims, end);
  }
}

} // namespace ceph

/**
 * Split **str** into a list of strings, using the ";,= \t" delimiters and output the result in **str_list**.
 * 
 * @param [in] str String to split and save as list
 * @param [out] str_list List modified containing str after it has been split
**/
extern void get_str_list(const std::string& str,
			 std::list<std::string>& str_list);

/**
 * Split **str** into a list of strings, using the **delims** delimiters and output the result in **str_list**.
 * 
 * @param [in] str String to split and save as list
 * @param [in] delims characters used to split **str**
 * @param [out] str_list List modified containing str after it has been split
**/
extern void get_str_list(const std::string& str,
                         const char *delims,
			 std::list<std::string>& str_list);

std::list<std::string> get_str_list(const std::string& str,
                                    const char *delims = ";,= \t");

/**
 * Split **str** into a list of strings, using the ";,= \t" delimiters and output the result in **str_vec**.
 * 
 * @param [in] str String to split and save as Vector
 * @param [out] str_vec Vector modified containing str after it has been split
**/
extern void get_str_vec(const std::string& str,
			 std::vector<std::string>& str_vec);

/**
 * Split **str** into a list of strings, using the **delims** delimiters and output the result in **str_vec**.
 * 
 * @param [in] str String to split and save as Vector
 * @param [in] delims characters used to split **str**
 * @param [out] str_vec Vector modified containing str after it has been split
**/
extern void get_str_vec(const std::string& str,
                         const char *delims,
			 std::vector<std::string>& str_vec);

std::vector<std::string> get_str_vec(const std::string& str,
                                     const char *delims = ";,= \t");
/**
 * Split **str** into a list of strings, using the ";,= \t" delimiters and output the result in **str_list**.
 * 
 * @param [in] str String to split and save as Set
 * @param [out] str_list Set modified containing str after it has been split
**/
extern void get_str_set(const std::string& str,
			std::set<std::string>& str_list);

/**
 * Split **str** into a list of strings, using the **delims** delimiters and output the result in **str_list**.
 * 
 * @param [in] str String to split and save as Set
 * @param [in] delims characters used to split **str**
 * @param [out] str_list Set modified containing str after it has been split
**/
template<class Compare = std::less<std::string> >
void get_str_set(const std::string& str,
                 const char *delims,
                 std::set<std::string, Compare>& str_list)
{
  str_list.clear();
  for_each_substr(str, delims, [&str_list] (auto token) {
                  str_list.emplace(token.begin(), token.end());
                  });
}

std::set<std::string> get_str_set(const std::string& str,
                                  const char *delims = ";,= \t");



/**
 * Return a String containing the vector **v** joined with **sep**
 * 
 * If **v** is empty, the function returns an empty string
 * For each element in **v**,
 * it will concatenate this element and **sep** with result
 * 
 * @param [in] v Vector to join as a String
 * @param [in] sep String used to join each element from **v**
 * @return empty string if **v** is empty or concatenated string
**/
inline std::string str_join(const std::vector<std::string>& v, const std::string& sep)
{
  if (v.empty())
    return std::string();
  std::vector<std::string>::const_iterator i = v.begin();
  std::string r = *i;
  for (++i; i != v.end(); ++i) {
    r += sep;
    r += *i;
  }
  return r;
}

#endif
