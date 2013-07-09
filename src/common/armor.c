
#if defined(__linux__)
#include <linux/errno.h>
#else
#include <sys/errno.h>
#endif

/*
 * base64 encode/decode.
 */

const char *pem_key = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static int encode_bits(int c)
{
	return pem_key[c];
}

static int decode_bits(char c)
{
	if (c >= 'A' && c <= 'Z')
		return c - 'A';
	if (c >= 'a' && c <= 'z')
		return c - 'a' + 26;
	if (c >= '0' && c <= '9')
		return c - '0' + 52;
	if (c == '+' || c == '-')
		return 62;
	if (c == '/' || c == '_')
		return 63;
	if (c == '=')
		return 0; /* just non-negative, please */
	return -EINVAL;	
}

static int set_str_val(char **pdst, const char *end, char c)
{
	if (*pdst < end) {
		char *p = *pdst;
		*p = c;
		(*pdst)++;
	} else
		return -ERANGE;

	return 0;
}

int ceph_armor_line_break(char *dst, const char *dst_end, const char *src, const char *end, int line_width)
{
	int olen = 0;
	int line = 0;

#define SET_DST(c) do { \
	int __ret = set_str_val(&dst, dst_end, c); \
	if (__ret < 0) \
		return __ret; \
} while (0);

	while (src < end) {
		unsigned char a;

		a = *src++;
		SET_DST(encode_bits(a >> 2));
		if (src < end) {
			unsigned char b;
			b = *src++;
			SET_DST(encode_bits(((a & 3) << 4) | (b >> 4)));
			if (src < end) {
				unsigned char c;
				c = *src++;
				SET_DST(encode_bits(((b & 15) << 2) |
								(c >> 6)));
				SET_DST(encode_bits(c & 63));
			} else {
				SET_DST(encode_bits((b & 15) << 2));
				SET_DST('=');
			}
		} else {
			SET_DST(encode_bits(((a & 3) << 4)));
			SET_DST('=');
			SET_DST('=');
		}
		olen += 4;
		line += 4;
		if (line_width && line == line_width) {
			line = 0;
			SET_DST('\n');
			olen++;
		}
	}
	return olen;
}

int ceph_armor(char *dst, const char *dst_end, const char *src, const char *end)
{
	return ceph_armor_line_break(dst, dst_end, src, end, 0);
}

int ceph_unarmor(char *dst, const char *dst_end, const char *src, const char *end)
{
	int olen = 0;

	while (src < end) {
		int a, b, c, d;

		if (src[0] == '\n') {
			src++;
			continue;
		}

		if (src + 4 > end)
			return -EINVAL;
		a = decode_bits(src[0]);
		b = decode_bits(src[1]);
		c = decode_bits(src[2]);
		d = decode_bits(src[3]);
		if (a < 0 || b < 0 || c < 0 || d < 0)
			return -EINVAL;

		SET_DST((a << 2) | (b >> 4));
		if (src[2] == '=')
			return olen + 1;
		SET_DST(((b & 15) << 4) | (c >> 2));
		if (src[3] == '=')
			return olen + 2;
		SET_DST(((c & 3) << 6) | d);
		olen += 3;
		src += 4;
	}
	return olen;
}
