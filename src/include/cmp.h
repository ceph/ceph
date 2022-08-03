#ifndef __CEPH_CMP_H
#define __CEPH_CMP_H

/*
 * macros to define comparison operators for classes with small numbers of members.
 */
#define WRITE_EQ_OPERATORS_7(type, a, b, c, d, e, f, g)			\
  inline bool operator==(const type &l, const type &r) {		\
    return l.a == r.a && l.b == r.b && l.c == r.c && l.d == r.d && l.e == r.e && l.f == r.f && l.g == r.g; \
  }									\
  inline bool operator!=(const type &l, const type &r) {		\
    return l.a != r.a || l.b != r.b || l.c != r.c || l.d != r.d || l.e != r.e || l.f != r.f || l.g != r.g; \
  }
#define WRITE_CMP_OPERATORS_7(type, a, b, c, d, e, f, g)		\
  inline bool operator<=(const type &l, const type &r) {		\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b < r.b ||					\
		      (l.b == r.b && (l.c < r.c ||			\
				      (l.c == r.c && (l.d < r.d ||	\
						      (l.d == r.d && (l.e < r.e || \
								      (l.e == r.e && (l.f < r.f || \
										      (l.f == r.f && l.g <= r.g))))))))))); \
  }									\
  inline bool operator>=(const type &l, const type &r) {		\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b > r.b ||					\
		      (l.b == r.b && (l.c > r.c ||			\
				      (l.c == r.c && (l.d > r.d ||	\
						      (l.d == r.d && (l.e > r.e || \
								      (l.e == r.e && (l.f > r.f || \
										      (l.f == r.f && l.g >= r.g))))))))))); \
  }									\
  inline bool operator>(const type &l, const type &r) {			\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b > r.b ||					\
		      (l.b == r.b && (l.c > r.c ||			\
				      (l.c == r.c && (l.d > r.d ||	\
						      (l.d == r.d && (l.e > r.e || \
								      (l.e == r.e && (l.f > r.f || \
										      (l.f == r.f && l.g > r.g))))))))))); \
  }									\
  inline bool operator<(const type &l, const type &r) {			\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b < r.b ||					\
		      (l.b == r.b && (l.c < r.c ||			\
				      (l.c == r.c && (l.d < r.d ||	\
						      (l.d == r.d && (l.e < r.e || \
								      (l.e == r.e && (l.f < r.f || \
										      (l.f == r.f && l.g < r.g))))))))))); \
  }
#endif
