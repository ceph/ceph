#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#define  MAXARGS     32


#define CALL_SNPRINTF(buf, size, format, args) snprintf(buf, size, format, args[0], args[1], args[2], args[3], \
				args[4], args[5], args[6], args[7],	\
				args[8], args[9], args[10], args[11],	\
				args[12], args[13], args[14], args[15],	\
				args[16], args[17], args[18], args[19],	\
				args[20], args[21], args[22], args[23],	\
				args[24], args[25], args[26], args[27],	\
				args[28], args[29], args[30], args[31])

int dyn_snprintf(char **pbuf, size_t *pmax_size, int nargs, const char *format, ...)
{
	int ret;
	va_list vl;
	char *old_buf = *pbuf;
	char *args[MAXARGS];
	char *arg;
	char *tmp_src = NULL;
	int i;

	if (nargs > MAXARGS)
		return -1;

	va_start(vl, format);
	arg = va_arg(vl, char *);
	for (i = 0; i<nargs; i++) {
		if (arg == old_buf) {
			if (!tmp_src) {
				tmp_src = strdup(old_buf);
			}
			arg = tmp_src;
		}
		args[i] = arg;
		arg = va_arg(vl, char *);
	}
	va_end(vl);
	ret = CALL_SNPRINTF(*pbuf, *pmax_size, format, args);

	if (ret >= *pmax_size) {
		*pmax_size = ret * 2;
		*pbuf = (char *)realloc(*pbuf, *pmax_size);
		ret = CALL_SNPRINTF(*pbuf, *pmax_size, format, args);
	}

	return ret;
}

