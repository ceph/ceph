#include "common/config.h"
#include "include/buffer.h"
#include "include/encoding.h"

#include "gtest/gtest.h"

template < typename T >
static void test_encode_and_decode(const T& src)
{
  bufferlist bl(1000000);
  encode(src, bl);
  T dst;
  bufferlist::iterator i(bl.begin());
  decode(dst, i);
  ASSERT_EQ(src, dst) << "Encoding roundtrip changed the string: orig=" << src << ", but new=" << dst;
}

TEST(EncodingRoundTrip, StringSimple) {
  string my_str("I am the very model of a modern major general");
  test_encode_and_decode < std::string >(my_str);
}

TEST(EncodingRoundTrip, StringEmpty) {
  string my_str("");
  test_encode_and_decode < std::string >(my_str);
}

TEST(EncodingRoundTrip, StringNewline) {
  string my_str("foo bar baz\n");
  test_encode_and_decode < std::string >(my_str);
}

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

TEST(EncodingRoundTrip, Multimap) {
  multimap_t multimap;
  multimap.insert( my_val_ty(1, "foo") );
  multimap.insert( my_val_ty(2, "bar") );
  multimap.insert( my_val_ty(2, "baz") );
  multimap.insert( my_val_ty(3, "lucky number 3") );
  multimap.insert( my_val_ty(10000, "large number") );

  test_encode_and_decode < multimap_t >(multimap);
}



///////////////////////////////////////////////////////
// ConstructorCounter
///////////////////////////////////////////////////////
template <typename T>
class ConstructorCounter
{
public:
  ConstructorCounter() : data(0)
  {
    default_ctor++;
  }

  explicit ConstructorCounter(const T& data_)
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

  static int get_default_ctor(void)
  {
    return default_ctor;
  }

  static int get_one_arg_ctor(void)
  {
    return one_arg_ctor;
  }

  static int get_copy_ctor(void)
  {
    return copy_ctor;
  }

  static int get_assigns(void)
  {
    return assigns;
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

TEST(EncodingRoundTrip, MultimapConstructorCounter) {
  multimap2_t multimap2;
  multimap2.insert( val2_ty(my_key_t(1), my_val_t(10)) );
  multimap2.insert( val2_ty(my_key_t(2), my_val_t(20)) );
  multimap2.insert( val2_ty(my_key_t(2), my_val_t(30)) );
  multimap2.insert( val2_ty(my_key_t(3), my_val_t(40)) );
  multimap2.insert( val2_ty(my_key_t(10000), my_val_t(1)) );

  my_key_t::init();
  my_val_t::init();
  test_encode_and_decode < multimap2_t >(multimap2);

  EXPECT_EQ(my_key_t::get_default_ctor(), 5);
  EXPECT_EQ(my_key_t::get_one_arg_ctor(), 0);
  EXPECT_EQ(my_key_t::get_copy_ctor(), 5);
  EXPECT_EQ(my_key_t::get_assigns(), 0);

  EXPECT_EQ(my_val_t::get_default_ctor(), 5);
  EXPECT_EQ(my_val_t::get_one_arg_ctor(), 0);
  EXPECT_EQ(my_val_t::get_copy_ctor(), 5);
  EXPECT_EQ(my_val_t::get_assigns(), 0);
}

const char* expected_what[] = {
  "buffer::malformed_input: void lame_decoder(int) unknown encoding version > 100",
  "buffer::malformed_input: void lame_decoder(int) no longer understand old encoding version < 100",
  "buffer::malformed_input: void lame_decoder(int) decode past end of struct encoding",
};

void lame_decoder(int which) {
  switch (which) {
  case 0:
    throw buffer::malformed_input(DECODE_ERR_VERSION(__PRETTY_FUNCTION__, 100));
  case 1:
    throw buffer::malformed_input(DECODE_ERR_OLDVERSION(__PRETTY_FUNCTION__, 100));
  case 2:
    throw buffer::malformed_input(DECODE_ERR_PAST(__PRETTY_FUNCTION__));
  }
}

TEST(EncodingException, Macros) {
  for (unsigned i = 0; i < sizeof(expected_what)/sizeof(expected_what[0]); i++) {
    try {
      lame_decoder(i);
    } catch (const exception& e) {
      ASSERT_EQ(string(expected_what[i]), string(e.what()));
    }
  }
}
