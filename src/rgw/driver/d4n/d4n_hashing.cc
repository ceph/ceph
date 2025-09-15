
#include "d4n_hashing.h"

uint16_t crc16(const char *buf, int len) {
  int counter;
  uint16_t crc = 0;
  for (counter = 0; counter < len; counter++)
	crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
  return crc;
}

unsigned int hash_slot(const char *key, int keylen) {
  int s, e; /* start-end indexes of { and } */

  /* Search the first occurrence of '{'. */
  for (s = 0; s < keylen; s++)
	if (key[s] == '{') break;

  /* No '{' ? Hash the whole key. This is the base case. */
  if (s == keylen) return crc16(key,keylen) & (REDIS_CLUSTER_SLOTS - 1);

  /* '{' found? Check if we have the corresponding '}'. */
  for (e = s+1; e < keylen; e++)
	if (key[e] == '}') break;

  /* No '}' or nothing between {} ? Hash the whole key. */
  if (e == keylen || e == s+1) return crc16(key,keylen) & (REDIS_CLUSTER_SLOTS - 1);

  /* If we are here there is both a { and a } on its right. Hash
   * what is in the middle between { and }. */
  return crc16(key+s+1,e-s-1) & (REDIS_CLUSTER_SLOTS - 1);
}

