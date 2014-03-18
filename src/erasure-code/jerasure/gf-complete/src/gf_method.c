/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_method.c
 *
 * Parses argv to figure out the mult_type and arguments.  Returns the gf.
 */

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "gf_complete.h"
#include "gf_int.h"
#include "gf_method.h"

int create_gf_from_argv(gf_t *gf, int w, int argc, char **argv, int starting)
{
  int mult_type, divide_type, region_type;
  int arg1, arg2;
  uint64_t prim_poly;
  gf_t *base;

  mult_type = GF_MULT_DEFAULT;
  region_type = GF_REGION_DEFAULT;
  divide_type = GF_DIVIDE_DEFAULT;
  prim_poly = 0;
  base = NULL;
  arg1 = 0;
  arg2 = 0;
  while (1) {
    if (argc > starting) {
      if (strcmp(argv[starting], "-m") == 0) {
        starting++;
        if (mult_type != GF_MULT_DEFAULT) {
          if (base != NULL) gf_free(base, 1);
          _gf_errno = GF_E_TWOMULT;
          return 0;
        }
        if (strcmp(argv[starting], "SHIFT") == 0) {
          mult_type = GF_MULT_SHIFT;
          starting++;
        } else if (strcmp(argv[starting], "CARRY_FREE") == 0) {
          mult_type = GF_MULT_CARRY_FREE;
          starting++;
        } else if (strcmp(argv[starting], "GROUP") == 0) {
          mult_type = GF_MULT_GROUP;
          if (argc < starting + 3) {
            _gf_errno = GF_E_GROUPAR;
            return 0;
          }
          if (sscanf(argv[starting+1], "%d", &arg1) == 0 ||
              sscanf(argv[starting+2], "%d", &arg2) == 0) {
            _gf_errno = GF_E_GROUPNU;
            return 0;
          }
          starting += 3;
        } else if (strcmp(argv[starting], "BYTWO_p") == 0) {
          mult_type = GF_MULT_BYTWO_p;
          starting++;
        } else if (strcmp(argv[starting], "BYTWO_b") == 0) {
          mult_type = GF_MULT_BYTWO_b;
          starting++;
        } else if (strcmp(argv[starting], "TABLE") == 0) {
          mult_type = GF_MULT_TABLE;
          starting++;
        } else if (strcmp(argv[starting], "LOG") == 0) {
          mult_type = GF_MULT_LOG_TABLE;
          starting++;
        } else if (strcmp(argv[starting], "LOG_ZERO") == 0) {
          mult_type = GF_MULT_LOG_ZERO;
          starting++;
        } else if (strcmp(argv[starting], "LOG_ZERO_EXT") == 0) {
          mult_type = GF_MULT_LOG_ZERO_EXT;
          starting++;
        } else if (strcmp(argv[starting], "SPLIT") == 0) {
          mult_type = GF_MULT_SPLIT_TABLE;
          if (argc < starting + 3) {
            _gf_errno = GF_E_SPLITAR;
            return 0;
          }
          if (sscanf(argv[starting+1], "%d", &arg1) == 0 ||
              sscanf(argv[starting+2], "%d", &arg2) == 0) {
            _gf_errno = GF_E_SPLITNU;
            return 0;
          }
          starting += 3;
        } else if (strcmp(argv[starting], "COMPOSITE") == 0) {
          mult_type = GF_MULT_COMPOSITE;
          if (argc < starting + 2) { _gf_errno = GF_E_FEWARGS; return 0; }
          if (sscanf(argv[starting+1], "%d", &arg1) == 0) {
            _gf_errno = GF_E_COMP_A2;
            return 0;
          }
          starting += 2;
          base = (gf_t *) malloc(sizeof(gf_t));
          starting = create_gf_from_argv(base, w/arg1, argc, argv, starting);
          if (starting == 0) {
            free(base);
            return 0;
          }
        } else {
          if (base != NULL) gf_free(base, 1);
          _gf_errno = GF_E_UNKNOWN;
          return 0;
        }
      } else if (strcmp(argv[starting], "-r") == 0) {
        starting++;
        if (strcmp(argv[starting], "DOUBLE") == 0) {
          region_type |= GF_REGION_DOUBLE_TABLE;
          starting++;
        } else if (strcmp(argv[starting], "QUAD") == 0) {
          region_type |= GF_REGION_QUAD_TABLE;
          starting++;
        } else if (strcmp(argv[starting], "LAZY") == 0) {
          region_type |= GF_REGION_LAZY;
          starting++;
        } else if (strcmp(argv[starting], "SSE") == 0) {
          region_type |= GF_REGION_SSE;
          starting++;
        } else if (strcmp(argv[starting], "NOSSE") == 0) {
          region_type |= GF_REGION_NOSSE;
          starting++;
        } else if (strcmp(argv[starting], "CAUCHY") == 0) {
          region_type |= GF_REGION_CAUCHY;
          starting++;
        } else if (strcmp(argv[starting], "ALTMAP") == 0) {
          region_type |= GF_REGION_ALTMAP;
          starting++;
        } else {
          if (base != NULL) gf_free(base, 1);
          _gf_errno = GF_E_UNK_REG;
          return 0;
        }
      } else if (strcmp(argv[starting], "-p") == 0) {
        starting++;
        if (sscanf(argv[starting], "%llx", (long long unsigned int *)(&prim_poly)) == 0) {
          if (base != NULL) gf_free(base, 1);
          _gf_errno = GF_E_POLYSPC;
          return 0;
        }
        starting++;
      } else if (strcmp(argv[starting], "-d") == 0) {
        starting++;
        if (divide_type != GF_DIVIDE_DEFAULT) {
          if (base != NULL) gf_free(base, 1);
          _gf_errno = GF_E_TWO_DIV;
          return 0;
        } else if (strcmp(argv[starting], "EUCLID") == 0) {
          divide_type = GF_DIVIDE_EUCLID;
          starting++;
        } else if (strcmp(argv[starting], "MATRIX") == 0) {
          divide_type = GF_DIVIDE_MATRIX;
          starting++;
        } else {
          _gf_errno = GF_E_UNK_DIV;
          return 0;
        }
      } else if (strcmp(argv[starting], "-") == 0) {
         /*
         printf("Scratch size: %d\n", gf_scratch_size(w, 
                                      mult_type, region_type, divide_type, arg1, arg2));
         */
        if (gf_init_hard(gf, w, mult_type, region_type, divide_type, 
                         prim_poly, arg1, arg2, base, NULL) == 0) {
          if (base != NULL) gf_free(base, 1);
          return 0;
        } else
          return starting + 1;
      } else {
        if (base != NULL) gf_free(base, 1);
        _gf_errno = GF_E_UNKFLAG;
        return 0;
      }
    } else {
      if (base != NULL) gf_free(base, 1);
      _gf_errno = GF_E_FEWARGS;
      return 0;
    }
  }
}
