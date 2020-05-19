#ifndef SPEC_ARCH_PROBE_H
#define SPEC_ARCH_PROBE_H

#ifdef __cplusplus
extern "C" {
#endif

extern int spec_arch_probed;  /* non-zero if we've probed features */

extern int spec_arch_probe(void);

#ifdef __cplusplus
}
#endif

#endif
