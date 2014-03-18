/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_method.h
 *
 * Parses argv to figure out the flags and arguments.  Creates the gf.
 */

#pragma once

#include "gf_complete.h"

/* Parses argv starting at "starting".  
   
   Returns 0 on failure.
   On success, it returns one past the last argument it read in argv. */

extern int create_gf_from_argv(gf_t *gf, int w, int argc, char **argv, int starting);
