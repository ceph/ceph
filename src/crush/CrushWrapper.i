/* File : CrushWrapper.i */
%module CrushWrapper
%{
#include "CrushWrapper.h"
%}

%include typemaps.i

// This tells SWIG to treat 'int *data' as a special case
%typemap(in) int *items {
  AV *tempav;
  I32 len;
  int i;
  SV **tv;
//  int view;


  //printf("typemap\n");

  if (!SvROK($input))
	croak("$input is not a reference.");
  if (SvTYPE(SvRV($input)) != SVt_PVAV)
	croak("$input is not an array.");

  tempav = (AV*)SvRV($input);
  len = av_len(tempav);
  //printf("typemap len: %i\n",len);
  $1 = (int *) malloc((len+1)*sizeof(int));
  for (i = 0; i <= len; i++) {
	tv = av_fetch(tempav, i, 0);
	$1[i] = (int) SvIV(*tv);
    
	/*
	  view = SvIV(*tv);
	  printf("view: %d",view);
	  printf("\n");
	*/
  }
}

%apply int *items { int *weights };
%apply double *OUTPUT { double *min, double *max, double *avg };

/* Let's just grab the original header file here */
%include "CrushWrapper.h"      

%clear double *min, double *max, double *avg;
