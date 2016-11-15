#include "acconfig.h"
#include "include/int_types.h"
#include "common/crc32c_aarch64.h"

/* Request crc extension capabilities from the assembler */
asm(".arch_extension crc");

#define CRC32CX(crc, value) __asm__("crc32cx %w[c], %w[c], %x[v]":[c]"+r"(crc):[v]"r"(value))
#define CRC32CW(crc, value) __asm__("crc32cw %w[c], %w[c], %w[v]":[c]"+r"(crc):[v]"r"(value))
#define CRC32CH(crc, value) __asm__("crc32ch %w[c], %w[c], %w[v]":[c]"+r"(crc):[v]"r"(value))
#define CRC32CB(crc, value) __asm__("crc32cb %w[c], %w[c], %w[v]":[c]"+r"(crc):[v]"r"(value))

uint32_t ceph_crc32c_aarch64(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	int64_t length = len;

	if (!buffer) {

		while ((length -= sizeof(uint64_t)) >= 0)
			CRC32CX(crc, 0);

		/* The following is more efficient than the straight loop */
		if (length & sizeof(uint32_t))
			CRC32CW(crc, 0);

		if (length & sizeof(uint16_t))
			CRC32CH(crc, 0);

		if (length & sizeof(uint8_t))
			CRC32CB(crc, 0);
	} else {
		while ((length -= sizeof(uint64_t)) >= 0) {
			CRC32CX(crc, *(uint64_t *)buffer);
			buffer += sizeof(uint64_t);
		}

		/* The following is more efficient than the straight loop */
		if (length & sizeof(uint32_t)) {
			CRC32CW(crc, *(uint32_t *)buffer);
			buffer += sizeof(uint32_t);
		}
		if (length & sizeof(uint16_t)) {
			CRC32CH(crc, *(uint16_t *)buffer);
			buffer += sizeof(uint16_t);
		}
		if (length & sizeof(uint8_t))
			CRC32CB(crc, *buffer);
	}
	return crc;
}
