#ifndef __DYN_SNPRINTF_H
#define __DYN_SNPRINTF_H

#ifdef __cplusplus
extern "C" {
#endif

int dyn_snprintf(char **pbuf, size_t *pmax_size, int nargs, const char *format, ...);

#ifdef __cplusplus
}
#endif

#endif
