#ifndef __CEPH_DECODE_H
#define __CEPH_DECODE_H

/*
 * in all cases, 
 *   void **p     pointer to position pointer
 *   void *end    pointer to end of buffer (last byte + 1)
 */

/*
 * bounds check input.
 */
#define ceph_decode_need(p, end, n, bad)		\
	do {						\
		if (unlikely(*(p) + (n) > (end))) 	\
			goto bad;			\
	} while (0)

#define ceph_decode_64(p, v)				\
	do {						\
		v = le64_to_cpu(*(__u64*)*(p));		\
		*(p) += sizeof(__u64);			\
	} while (0)
#define ceph_decode_32(p, v)				\
	do {						\
		v = le32_to_cpu(*(__u32*)*(p));		\
		*(p) += sizeof(__u32);			\
	} while (0)
#define ceph_decode_16(p, v)				\
	do {						\
		v = le16_to_cpu(*(__u16*)*(p));		\
		*(p) += sizeof(__u16);			\
	} while (0)

#define ceph_decode_copy(p, pv, n)			\
	do {						\
		memcpy(pv, *(p), n);			\
		*(p) += n;				\
	} while (0)

#define ceph_decode_64_safe(p, end, v, bad)			\
	do {							\
		ceph_decode_need(p, end, sizeof(__u64), bad);	\
		ceph_decode_64(p, v);				\
	} while (0)
#define ceph_decode_32_safe(p, end, v, bad)			\
	do {							\
		ceph_decode_need(p, end, sizeof(__u32), bad);	\
		ceph_decode_32(p, v);				\
	} while (0)
#define ceph_decode_16_safe(p, end, v, bad)			\
	do {							\
		ceph_decode_need(p, end, sizeof(__u16), bad);	\
		ceph_decode_16(p, v);				\
	} while (0)

#define ceph_decode_copy_safe(p, end, pv, n, bad)		\
	do {							\
		ceph_decode_need(p, end, n, bad);		\
		ceph_decode_copy(p, pv, n);			\
	} while (0)

#endif
