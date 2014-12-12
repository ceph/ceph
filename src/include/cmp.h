#ifndef __CEPH_CMP_H
#define __CEPH_CMP_H

/*
 * macros to define comparison operators for classes with small numbers of members.
 */

#define WRITE_EQ_OPERATORS_1(type, a)					\
  inline bool operator==(const type &l, const type &r) {		\
    return l.a == r.a;							\
  }									\
  inline bool operator!=(const type &l, const type &r) {		\
    return l.a != r.a;							\
  }

#define WRITE_CMP_OPERATORS_1(type, a)					\
  inline bool operator>(const type &l, const type &r) {			\
    return l.a > r.a;							\
  }									\
  inline bool operator<(const type &l, const type &r) {			\
    return l.a < r.a;							\
  }									\
  inline bool operator>=(const type &l, const type &r) {		\
    return l.a >= r.a;							\
  }									\
  inline bool operator<=(const type &l, const type &r) {		\
    return l.a <= r.a;							\
  }

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

#define WRITE_EQ_OPERATORS_4(type, a, b, c, d)				\
  inline bool operator==(const type &l, const type &r) {		\
    return l.a == r.a && l.b == r.b && l.c == r.c && l.d == r.d;	\
  }									\
  inline bool operator!=(const type &l, const type &r) {		\
    return l.a != r.a || l.b != r.b || l.c != r.c || l.d != r.d;	\
  }

#define WRITE_CMP_OPERATORS_4(type, a, b, c, d)				\
  inline bool operator>(const type &l, const type &r) {			\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b > r.b ||					\
		      (l.b == r.b && (l.c > r.c ||			\
				      (l.c == r.c && (l.d > r.d))))));	\
  }									\
  inline bool operator<(const type &l, const type &r) {			\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b < r.b ||					\
		      (l.b == r.b && (l.c < r.c ||			\
				      (l.c == r.c && (l.d < r.d))))));	\
  }									\
  inline bool operator>=(const type &l, const type &r) {		\
    return l.a > r.a ||							\
      (l.a == r.a && (l.b > r.b ||					\
		      (l.b == r.b && (l.c > r.c ||			\
				      (l.c == r.c && (l.d >= r.d))))));	\
  }									\
  inline bool operator<=(const type &l, const type &r) {		\
    return l.a < r.a ||							\
      (l.a == r.a && (l.b < r.b ||					\
		      (l.b == r.b && (l.c < r.c ||			\
				      (l.c == r.c && (l.d <= r.d)))))); \
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
