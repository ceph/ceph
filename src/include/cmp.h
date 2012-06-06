#ifndef __CEPH_CMP_H
#define __CEPH_CMP_H
#include <boost/tuple/tuple.hpp>
#include <boost/tuple/tuple_comparison.hpp>

/*
 * macros to define comparison operators for classes with small numbers of members.
 */

#define WRITE_EQ_OPERATORS_2(type, a, b)				\
  inline bool operator==(const type &l, const type &r) {		\
    return l.a == r.a && l.b == r.b;					\
  }									\
  inline bool operator!=(const type &l, const type &r) {		\
    return l.a != r.a || l.b != r.b;					\
  }

#define WRITE_CMP_OPERATORS_2(type, a, b)			\
  inline bool operator>(const type &l, const type &r) {			\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b > r.b));					\
  }									\
  inline bool operator<(const type &l, const type &r) {			\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b < r.b));					\
  }									\
  inline bool operator>=(const type &l, const type &r) {		\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b >= r.b));					\
  }									\
  inline bool operator<=(const type &l, const type &r) {		\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b <= r.b));					\
  }


#define WRITE_EQ_OPERATORS_3(type, a, b, c)				\
  inline bool operator==(const type &l, const type &r) {		\
    return l.a == r.a && l.b == r.b && l.c == r.c;			\
  }									\
  inline bool operator!=(const type &l, const type &r) {		\
    return l.a != r.a || l.b != r.b || l.c != r.c;			\
  }

#define WRITE_CMP_OPERATORS_3(type, a, b, c)				\
  inline bool operator>(const type &l, const type &r) {			\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b > r.b ||					\
		      (l.b == r.b && (l.c > r.c))));			\
  }									\
  inline bool operator<(const type &l, const type &r) {			\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b < r.b ||					\
		      (l.b == r.b && (l.c < r.c))));			\
  }									\
  inline bool operator>=(const type &l, const type &r) {		\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b > r.b ||					\
		      (l.b == r.b && (l.c >= r.c))));			\
  }									\
  inline bool operator<=(const type &l, const type &r) {		\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b < r.b ||					\
		      (l.b == r.b && (l.c <= r.c))));			\
  }



#define WRITE_EQ_OPERATORS_5(type, a, b, c, d, e)			\
  inline bool operator==(const type &l, const type &r) {		\
    return l.a == r.a && l.b == r.b && l.c == r.c && l.d == r.d && l.e == r.e; \
  }									\
  inline bool operator!=(const type &l, const type &r) {		\
    return l.a != r.a || l.b != r.b || l.c != r.c || l.d != r.d || l.e != r.e; \
  }

#define WRITE_CMP_OPERATORS_5(type, a, b, c, d, e)			\
  inline bool operator>(const type &l, const type &r) {			\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b > r.b ||					\
		      (l.b == r.b && (l.c > r.c ||			\
				      (l.c == r.c && (l.d > r.d ||	\
						      (l.d == r.d && l.e > r.e))))))); \
  }									\
  inline bool operator<(const type &l, const type &r) {			\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b < r.b ||					\
		      (l.b == r.b && (l.c < r.c ||			\
				      (l.c == r.c && (l.d < r.d ||	\
						      (l.d == r.d && (l.e < r.e)))))))); \
  }									\
  inline bool operator>=(const type &l, const type &r) {		\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b > r.b ||					\
		      (l.b == r.b && (l.c > r.c ||			\
				      (l.c == r.c && (l.d > r.d ||	\
						      (l.d == r.d && l.e >= r.e))))))); \
  }									\
  inline bool operator<=(const type &l, const type &r) {		\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b < r.b ||					\
		      (l.b == r.b && (l.c < r.c ||			\
				      (l.c == r.c && (l.d < r.d ||	\
						      (l.d == r.d && l.e <= r.e))))))); \
  }

#define WRITE_EQ_OPERATORS_7(type, a, b, c, d, e, f, g)                                                       \
	inline bool operator==(const type &l, const type &r) {                                                      \
		return (boost::make_tuple(boost::cref(l.a), boost::cref(l.b), boost::cref(l.c), boost::cref(l.d), boost::cref(l.e), boost::cref(l.f), boost::cref(l.g)) == \
						boost::make_tuple(boost::cref(r.a), boost::cref(r.b), boost::cref(r.c), boost::cref(r.d), boost::cref(r.e), boost::cref(r.f), boost::cref(r.g)));  \
	}                                                                                                           \
	inline bool operator!=(const type &l, const type &r) {                                                      \
		return (boost::make_tuple(boost::cref(l.a), boost::cref(l.b), boost::cref(l.c), boost::cref(l.d), boost::cref(l.e), boost::cref(l.f), boost::cref(l.g)) != \
						boost::make_tuple(boost::cref(r.a), boost::cref(r.b), boost::cref(r.c), boost::cref(r.d), boost::cref(r.e), boost::cref(r.f), boost::cref(r.g)));  \
	}
#define WRITE_CMP_OPERATORS_7(type, a, b, c, d, e, f, g)                                                      \
	inline bool operator<=(const type &l, const type &r) {                                                      \
		return (boost::make_tuple(boost::cref(l.a), boost::cref(l.b), boost::cref(l.c), boost::cref(l.d), boost::cref(l.e), boost::cref(l.f), boost::cref(l.g)) <= \
						boost::make_tuple(boost::cref(r.a), boost::cref(r.b), boost::cref(r.c), boost::cref(r.d), boost::cref(r.e), boost::cref(r.f), boost::cref(r.g)));  \
	}                                                                                                           \
	inline bool operator>=(const type &l, const type &r) {                                                      \
		return (boost::make_tuple(boost::cref(l.a), boost::cref(l.b), boost::cref(l.c), boost::cref(l.d), boost::cref(l.e), boost::cref(l.f), boost::cref(l.g)) >= \
						boost::make_tuple(boost::cref(r.a), boost::cref(r.b), boost::cref(r.c), boost::cref(r.d), boost::cref(r.e), boost::cref(r.f), boost::cref(r.g)));  \
	}                                                                                                           \
	inline bool operator>(const type &l, const type &r) {                                                       \
		return (boost::make_tuple(boost::cref(l.a), boost::cref(l.b), boost::cref(l.c), boost::cref(l.d), boost::cref(l.e), boost::cref(l.f), boost::cref(l.g)) >  \
						boost::make_tuple(boost::cref(r.a), boost::cref(r.b), boost::cref(r.c), boost::cref(r.d), boost::cref(r.e), boost::cref(r.f), boost::cref(r.g)));  \
	}                                                                                                           \
	inline bool operator<(const type &l, const type &r) {                                                       \
		return (boost::make_tuple(boost::cref(l.a), boost::cref(l.b), boost::cref(l.c), boost::cref(l.d), boost::cref(l.e), boost::cref(l.f), boost::cref(l.g)) <  \
						boost::make_tuple(boost::cref(r.a), boost::cref(r.b), boost::cref(r.c), boost::cref(r.d), boost::cref(r.e), boost::cref(r.f), boost::cref(r.g)));  \
	}
#endif
