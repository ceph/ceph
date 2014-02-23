#include "common/config.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "msg/msg_types.h"

#include "gtest/gtest.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

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
  EXPECT_EQ(my_key_t::get_copy_ctor(), 10);
  EXPECT_EQ(my_key_t::get_assigns(), 0);

  EXPECT_EQ(my_val_t::get_default_ctor(), 5);
  EXPECT_EQ(my_val_t::get_one_arg_ctor(), 0);
  EXPECT_EQ(my_val_t::get_copy_ctor(), 10);
  EXPECT_EQ(my_val_t::get_assigns(), 0);
}

inline bool operator==(struct sockaddr_storage l, struct sockaddr_storage r) {
  return (0 == memcmp(&l, &r, sizeof(l)));
}

inline bool operator==(struct sockaddr_storage& l, struct sockaddr_storage& r) {
  return (0 == memcmp(&l, &r, sizeof(l)));
}


TEST(EncodingRoundTrip, struct_sockaddr_in) {
  struct sockaddr_storage ss;
  struct sockaddr_in &sin = reinterpret_cast<struct sockaddr_in&>(ss);

  // encode may skip don't care fields.  For testing purposes, we zero them.
  bzero(&ss, sizeof(ss));
#ifdef HAVE_SS_LEN_IN_SOCKADDR_STORAGE
  sin.sin_len = sizeof(sin);
#endif
  sin.sin_family = AF_INET;
  sin.sin_port = htons(0x9abc);
  EXPECT_EQ(1, inet_pton(AF_INET, "18.52.86.120", &sin.sin_addr));

  test_encode_and_decode < struct sockaddr_storage >(ss);
}

TEST(EncodingRoundTrip, struct_sockaddr_in6) {
  struct sockaddr_storage ss;
  struct sockaddr_in6 &sin6 = reinterpret_cast<struct sockaddr_in6&>(ss);

  // encode may skip don't care fields.  For testing purposes, we zero them.
  bzero(&ss, sizeof(ss));
#ifdef HAVE_SS_LEN_IN_SOCKADDR_STORAGE
  sin6.sin6_len = sizeof(sin6);
#endif
  sin6.sin6_family = AF_INET6;
  sin6.sin6_port = htons(0x9abc);
  sin6.sin6_scope_id = 0xe;    //Global scope
  sin6.sin6_flowinfo = 0xfedcb;
  EXPECT_EQ(1, inet_pton(AF_INET6, "2001:1234:5678:9abc:def0:cba9:8765:4321",
      &sin6.sin6_addr));
  test_encode_and_decode < struct sockaddr_storage >(ss);
}

// Test that we can encode a struct ceph_sockaddr_storage and get the same
// binary layout that Linux amd64 produces when casting a sockaddr_in to
// sockaddr_storage
TEST(SockaddrStoragePortability, encode_in) {
  bufferlist bl;
  struct sockaddr_storage ss;
  struct sockaddr_in &sin = reinterpret_cast<struct sockaddr_in&>(ss);
  uint8_t encoded[128];
  uint8_t expected[128] = {
    0,                                              //Upper byte of sin_family
    AF_INET,                                        //Lower byte of sin_family
    0x9a, 0xbc,                                     //sin_port in big endian
    0x12, 0x34, 0x56, 0x78,                         //sin_addr in big endian
    0, 0, 0, 0,                                     //sin_zero
    0, 0, 0, 0, 0, 0, 0, 0,                         //__ss_padding[8:16]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[16:32]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[32:64]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[64:80]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[80:96]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[96:112]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[112:128]
  };

  // encode may skip don't care fields.  For testing purposes, we zero them.
  bzero(&ss, sizeof(ss));
#ifdef HAVE_SS_LEN_IN_SOCKADDR_STORAGE
  sin.sin_len = sizeof(sin);
#endif
  sin.sin_family = AF_INET;
  sin.sin_port = htons(0x9abc);
  EXPECT_EQ(1, inet_pton(AF_INET, "18.52.86.120", &sin.sin_addr));
  encode(ss, bl);
  EXPECT_EQ(sizeof(expected), bl.length());
  bl.copy(0, 128, (char*)encoded);
  EXPECT_EQ(0, memcmp(expected, encoded, 128));
}

// Test that we can encode a struct ceph_sockaddr_storage and get the same
// binary layout that Linux amd64 produces when casting a sockaddr_in6 to
// sockaddr_storage
TEST(SockaddrStoragePortability, encode_in6) {
  bufferlist bl;
  struct sockaddr_storage ss;
  struct sockaddr_in6 &sin6 = reinterpret_cast<struct sockaddr_in6&>(ss);
  uint8_t encoded[128];
  uint8_t expected[128] = {
    0,                                              //Upper byte of sin6_family
    AF_INET6,                                       //Lower byte of sin6_family
    0x9a, 0xbc,                                     //sin6_port in big endian
    0xcb, 0xed, 0x0f, 0x00,                         //sin6_flowinfo in LE
    0x20, 0x01, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, //sin6_addr[0:8]
    0xde, 0xf0, 0xcb, 0xa9, 0x87, 0x65, 0x43, 0x21, //sin6_addr[8:16]
    0x0e, 0x00, 0x00, 0x00,                         //sin6_scope_id in LE
    0, 0, 0, 0,                                     //__ss_padding[12:16]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[16:32]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[32:64]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[64:80]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[80:96]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[96:112]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[112:128]
  };

  // encode may skip don't care fields.  For testing purposes, we zero them.
  bzero(&ss, sizeof(ss));
#ifdef HAVE_SS_LEN_IN_SOCKADDR_STORAGE
  sin6.sin6_len = sizeof(sin6);
#endif
  sin6.sin6_family = AF_INET6;
  sin6.sin6_port = htons(0x9abc);
  sin6.sin6_scope_id = 0xe;    //Global scope
  sin6.sin6_flowinfo = 0xfedcb;
  EXPECT_EQ(1, inet_pton(AF_INET6, "2001:1234:5678:9abc:def0:cba9:8765:4321",
      &sin6.sin6_addr));

  encode(ss, bl);
  EXPECT_EQ(sizeof(expected), bl.length());
  bl.copy(0, 128, (char*)encoded);
  EXPECT_EQ(0, memcmp(expected, encoded, 128));
}

// Test that we can decode a struct ceph_sockaddr_storage using the binary
// layout of Linux amd64 and get the correct sockaddr_in
TEST(SockaddrStoragePortability, decode_in) {
  bufferlist bl;
  struct sockaddr_storage ss;
  struct sockaddr_in &sin = reinterpret_cast<struct sockaddr_in&>(ss);
  struct in_addr expected_addr;
  uint8_t encoded[128] = {
    0,                                              //Upper byte of sin_family
    AF_INET,                                        //Lower byte of sin_family
    0x9a, 0xbc,                                     //sin_port in big endian
    0x12, 0x34, 0x56, 0x78,                         //sin_addr in big endian
    0, 0, 0, 0,                                     //sin_zero
    0, 0, 0, 0, 0, 0, 0, 0,                         //__ss_padding[8:16]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[16:32]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[32:64]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[64:80]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[80:96]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[96:112]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[112:128]
  };
  bl.append((char*)encoded, 128);

  decode(ss, bl);
#ifdef HAVE_SS_LEN_IN_SOCKADDR_STORAGE
  EXPECT_EQ(sizeof(sin), sin.sin_len);
#endif
  EXPECT_EQ(AF_INET, sin.sin_family);
  EXPECT_EQ(0x9abc, ntohs(sin.sin_port));
  EXPECT_EQ(1, inet_pton(AF_INET, "18.52.86.120", &expected_addr));
  EXPECT_EQ(expected_addr.s_addr, sin.sin_addr.s_addr);
}

// Test that we can decode a struct ceph_sockaddr_storage using the binary
// layout of Linux amd64 and get the correct sockaddr_in6
TEST(SockaddrStoragePortability, decode_in6) {
  bufferlist bl;
  struct sockaddr_storage ss;
  struct sockaddr_in6 &sin6 = reinterpret_cast<struct sockaddr_in6&>(ss);
  struct in6_addr expected_addr;
  uint8_t encoded[128] = {
    0,                                              //Upper byte of sin6_family
    AF_INET6,                                       //Lower byte of sin6_family
    0x9a, 0xbc,                                     //sin6_port in big endian
    0xcb, 0xed, 0x0f, 0x00,                         //sin6_flowinfo in LE
    0x20, 0x01, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, //sin6_addr[0:8]
    0xde, 0xf0, 0xcb, 0xa9, 0x87, 0x65, 0x43, 0x21, //sin6_addr[8:16]
    0x0e, 0x00, 0x00, 0x00,                         //sin6_scope_id in LE
    0, 0, 0, 0,                                     //__ss_padding[12:16]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[16:32]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[32:64]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[64:80]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[80:96]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[96:112]
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //__ss_padding[112:128]
  };
  bl.append((char*)encoded, 128);

  decode(ss, bl);
#ifdef HAVE_SS_LEN_IN_SOCKADDR_STORAGE
  EXPECT_EQ(sizeof(sin6), sin6.sin6_len);
#endif
  EXPECT_EQ(AF_INET6, sin6.sin6_family);
  EXPECT_EQ(0x9abc, ntohs(sin6.sin6_port));
  EXPECT_EQ(0xeu, sin6.sin6_scope_id);
  EXPECT_EQ(0xfedcbu, sin6.sin6_flowinfo);
  EXPECT_EQ(1, inet_pton(AF_INET6, "2001:1234:5678:9abc:def0:cba9:8765:4321", &expected_addr));
  EXPECT_EQ(0, memcmp(&expected_addr, &sin6.sin6_addr, sizeof(expected_addr)));
}
