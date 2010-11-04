#include "config.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "common/common_init.h"

#include <iostream>
#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <sstream>
#include <vector>

using std::cout;
using std::cerr;
using std::string;
using std::vector;

static int multimap_ctor_count_tests(void);

typedef std::multimap < int, std::string > multimap_t;
typedef multimap_t::value_type my_val_ty;

static std::ostream& operator<<(std::ostream& oss, const multimap_t &multimap)
{
  for (multimap_t::const_iterator m = multimap.begin();
       m != multimap.end();
       ++m)
  {
    oss << m->first << "->" << m->second << " ";
  }
  return oss;
}

template < typename T >
static int test_encode_and_decode(const T& src)
{
  bufferlist bl(1000000);

  try {
    // source
    encode(src, bl);

    // dest
    T dst;
    bufferlist::iterator i(bl.begin());
    decode(dst, i);
    if (src != dst) {
      cout << "src = " << src << ", but dst = " << dst << std::endl;
      return 1;
    }
//    else {
//      cout << "src = " << src << ", and dst = " << dst << std::endl;
//    }
  }
  catch (const ceph::buffer::malformed_input &e) {
    cout << "got exception " << e.what() << std::endl;
    return 1;
  }
  catch (const std::exception &e) {
    cout << "got exception " << e.what() << std::endl;
    return 1;
  }
  catch (...) {
    cout << "unknown exception!" << std::endl;
    return 1;
  }
  return 0;
}

static int test_string_sz(const char *str)
{
  string my_str(str);
  return test_encode_and_decode < std::string >(my_str);
}

static int multimap_tests(void)
{
  multimap_t multimap;
  multimap.insert( my_val_ty(1, "foo") );
  multimap.insert( my_val_ty(2, "bar") );
  multimap.insert( my_val_ty(2, "baz") );
  multimap.insert( my_val_ty(3, "lucky number 3") );
  multimap.insert( my_val_ty(10000, "large number") );

  return test_encode_and_decode < multimap_t >(multimap);
}

int main(int argc, const char **argv)
{
  int ret = 0;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  ceph_set_default_id("admin");
  common_set_defaults(false);
  common_init(args, "ceph", true);

  ret = test_string_sz("I am the very model of a modern major general");
  if (ret)
    goto done;

  ret = test_string_sz("");
  if (ret)
    goto done;

  ret = test_string_sz("foo bar baz\n");
  if (ret)
    goto done;

  ret = multimap_tests();
  if (ret)
    goto done;

  ret = multimap_ctor_count_tests();
  if (ret)
    goto done;

done:
  if (ret) {
    cout << "FAILURE" << std::endl;
    return(EXIT_FAILURE);
  }
  cout << "SUCCESS" << std::endl;
  return(EXIT_SUCCESS);
}

///////////////////////////////////////////////////////
// ConstructorCounter
///////////////////////////////////////////////////////
template < typename T >
int test_encode_and_decode(const T& src);

template <typename T>
class ConstructorCounter
{
public:
  ConstructorCounter()
  {
    default_ctor++;
  }

  ConstructorCounter(const T& data_)
    : data(data_)
  {
    one_arg_ctor++;
  }

  ConstructorCounter(const ConstructorCounter &rhs)
    : data(rhs.data)
  {
    copy_ctor++;
  }

  ConstructorCounter &operator=(const ConstructorCounter &rhs)
  {
    data = rhs.data;
    assigns++;
    return *this;
  }

  static void init(void)
  {
    default_ctor = 0;
    one_arg_ctor = 0;
    copy_ctor = 0;
    assigns = 0;
  }

  static std::string get_totals(void)
  {
    std::ostringstream oss;
    oss << "default_ctor = " << default_ctor
	<< ", one_arg_ctor = " << one_arg_ctor
	<< ", copy_ctor = " << copy_ctor
	<< ", assigns = " << assigns << std::endl;
    return oss.str();
  }

  bool operator<(const ConstructorCounter &rhs) const
  {
    return data < rhs.data;
  }

  bool operator==(const ConstructorCounter &rhs) const
  {
    return data == rhs.data;
  }

  friend void decode(ConstructorCounter &s, bufferlist::iterator& p)
  {
    ::decode(s.data, p);
  }

  friend void encode(const ConstructorCounter &s, bufferlist& p)
  {
    ::encode(s.data, p);
  }

  friend ostream& operator<<(ostream &oss, const ConstructorCounter &cc)
  {
    oss << cc.data;
    return oss;
  }

  T data;
private:
  static int default_ctor;
  static int one_arg_ctor;
  static int copy_ctor;
  static int assigns;
};

template class ConstructorCounter <int32_t>;
template class ConstructorCounter <int16_t>;

typedef ConstructorCounter <int32_t> my_key_t;
typedef ConstructorCounter <int16_t> my_val_t;
typedef std::multimap < my_key_t, my_val_t > multimap2_t;
typedef multimap2_t::value_type val2_ty;

template <class T> int ConstructorCounter<T>::default_ctor = 0;
template <class T> int ConstructorCounter<T>::one_arg_ctor = 0;
template <class T> int ConstructorCounter<T>::copy_ctor = 0;
template <class T> int ConstructorCounter<T>::assigns = 0;

static std::ostream& operator<<(std::ostream& oss, const multimap2_t &multimap)
{
  for (multimap2_t::const_iterator m = multimap.begin();
       m != multimap.end();
       ++m)
  {
    oss << m->first << "->" << m->second << " ";
  }
  return oss;
}

static int multimap_ctor_count_tests(void)
{
  multimap2_t multimap2;
  multimap2.insert( val2_ty(my_key_t(1), my_val_t(10)) );
  multimap2.insert( val2_ty(my_key_t(2), my_val_t(20)) );
  multimap2.insert( val2_ty(my_key_t(2), my_val_t(30)) );
  multimap2.insert( val2_ty(my_key_t(3), my_val_t(40)) );
  multimap2.insert( val2_ty(my_key_t(10000), my_val_t(1)) );

  my_key_t::init();
  my_val_t::init();
  int ret = test_encode_and_decode < multimap2_t >(multimap2);
  if (ret)
    goto done;
  cout << "totals for key type: " << my_key_t::get_totals() << std::endl;
  cout << "totals for value type: " << my_val_t::get_totals() << std::endl;
  my_val_t::get_totals();

done:
  return ret;
}
