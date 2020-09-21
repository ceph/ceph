#if !defined(TRACING_COMMON_H)
#define TRACING_COMMON_H

// Amount of buffer data to dump when using ceph_ctf_sequence or ceph_ctf_sequencep.
// If 0, then *_data field is omitted entirely.
#if !defined(CEPH_TRACE_BUF_TRUNC_LEN)
#define CEPH_TRACE_BUF_TRUNC_LEN 0u
#endif

// TODO: This is GCC-specific.  Replace CEPH_MAX and CEPH_MIN with standard macros, if possible.
#define CEPH_MAX(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

#define CEPH_MIN(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

// type should be an integer type
// val should have type type*
#define ceph_ctf_integerp(type, field, val) \
    ctf_integer(type, field, (val) == NULL ? 0 : (val)) \
    ctf_integer(uint8_t, field##_isnull, (val) == NULL)

// val should have type char*
#define ceph_ctf_string(field, val) \
    ctf_string(field, (val) == NULL ? "" : (val)) \
    ctf_integer(uint8_t, field##_isnull, (val) == NULL)

// val should have type char**
#define ceph_ctf_stringp(field, val) \
    ctf_string(field, ((val) == NULL || *(val) == NULL) ? "" : *(val)) \
    ctf_integer(uint8_t, field##_isnull, (val) == NULL) \
    ctf_integer(uint8_t, field##_data_isnull, (val) == NULL || *(val) == NULL)

// val should have type type*
// lenval should have type lentype
#if CEPH_TRACE_BUF_TRUNC_LEN > 0
#define ceph_ctf_sequence(type, field, val, lentype, lenval) \
    ctf_integer_hex(void*, field, val) \
    ctf_sequence(type, field##_data, (val) == NULL ? "" : (val), lentype, (val) == NULL ? 0 : CEPH_MIN((lenval), CEPH_TRACE_BUF_TRUNC_LEN)) \
    ctf_integer(uint8_t, field##_isnull, (val) == NULL) \
    ctf_integer(lentype, field##_len, lenval)
#else
#define ceph_ctf_sequence(type, field, val, lentype, lenval) \
    ctf_integer_hex(void*, field, val) \
    ctf_integer(uint8_t, field##_isnull, (val) == NULL) \
    ctf_integer(lentype, field##_len, lenval)
#endif

// val should have type type**
// lenval should have type lentype*
#if CEPH_TRACE_BUF_TRUNC_LEN > 0
#define ceph_ctf_sequencep(type, field, val, lentype, lenval) \
    ctf_integer_hex(void*, field, val) \
    ctf_sequence(type, \
                 field##_data, \
                 ((val) == NULL || *(val) == NULL) ? "" : *(val), \
                 lentype, \
                 ((val) == NULL || *(val) == NULL || (lenval) == NULL) ? 0 : CEPH_MIN(*(lenval), CEPH_TRACE_BUF_TRUNC_LEN)) \
    ctf_integer(uint8_t, field##_isnull, (val) == NULL) \
    ctf_integer(uint8_t, field##_data_isnull, ((val) == NULL || *(val) == NULL)) \
    ctf_integer(lentype, field##_len, (lenval) == NULL ? 0 : *(lenval)) \
    ctf_integer(lentype, field##_len_isnull, (lenval) == NULL)
#else
#define ceph_ctf_sequencep(type, field, val, lentype, lenval) \
    ctf_integer_hex(void*, field, val) \
    ctf_integer(uint8_t, field##_isnull, (val) == NULL) \
    ctf_integer(uint8_t, field##_data_isnull, ((val) == NULL || *(val) == NULL)) \
    ctf_integer(lentype, field##_len, (lenval) == NULL ? 0 : *(lenval)) \
    ctf_integer(lentype, field##_len_isnull, (lenval) == NULL)
#endif

// p should be of type struct timeval*
#define ceph_ctf_timevalp(field, p) \
    ctf_integer(long int, field##_sec, (p) == NULL ? 0 : (p)->tv_sec) \
    ctf_integer(long int, field##_usec, (p) == NULL ? 0 : (p)->tv_usec) \
    ctf_integer(uint8_t, field##_isnull, (p) == NULL)

// p should be of type struct timespec*
#define ceph_ctf_timespecp(field, p) \
    ctf_integer(long int, field##_sec, (p) == NULL ? 0 : (p)->tv_sec) \
    ctf_integer(long int, field##_nsec, (p) == NULL ? 0 : (p)->tv_nsec) \
    ctf_integer(uint8_t, field##_isnull, (p) == NULL)

// val should be of type time_t
// Currently assumes that time_t is an integer and no more than 64 bits wide.
// This is verified by the configure script.
#define ceph_ctf_time_t(field, val) \
    ctf_integer(uint64_t, field, (uint64_t)(val))

// val should be of type time_t*
// Currently assumes that time_t is an integer and no more than 64 bits wide.
// This is verified by the configure script.
#define ceph_ctf_time_tp(field, val) \
    ctf_integer(uint64_t, field, (val) == NULL ? 0 : (uint64_t)(*val)) \
    ctf_integer(uint8_t, field##_isnull, (val) == NULL)


#endif /* TRACING_COMMON_H */
