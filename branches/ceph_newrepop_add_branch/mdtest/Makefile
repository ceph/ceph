#/*****************************************************************************\
#*                                                                             *
#*       Copyright (c) 2003, The Regents of the University of California       *
#*     See the file COPYRIGHT for a complete copyright notice and license.     *
#*                                                                             *
#*******************************************************************************
#*
#* CVS info:
#*   $RCSfile$
#*   $Revision$
#*   $Date$
#*   $Author$
#*
#* Purpose:
#*       Make mdtest executable.
#*
#*       make [mdtest]   -- mdtest
#*       make clean      -- remove executable
#*
#\*****************************************************************************/

CC.AIX = mpcc_r -bmaxdata:0x80000000
CC.Linux = mpicc -Wall

# Requires GNU Make
OS=$(shell uname)

CC = $(CC.$(OS))

mdtest: mdtest.c
	$(CC) -g -o mdtest mdtest.c -lm

clean:
	rm -f mdtest mdtest.o
