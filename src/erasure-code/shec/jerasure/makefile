# Makefile
# James S. Plank
# 
# JERASURE - Library for Erasure Coding
# Copright (C) 2007 James S. Plank
# 
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
# 
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
# 
# James S. Plank
# Department of Electrical Engineering and Computer Science
# University of Tennessee
# Knoxville, TN 37996
# plank@cs.utk.edu

# $Revision: 1.0 $
# $Date: 2007/09/25 15:12:20 $

CC = gcc  
CFLAGS = -O3 -I$(HOME)/include

ALL =	galois.o jerasure.o reed_sol.o cauchy.o liberation.o

all: $(ALL)

clean:
	rm -f core *.o $(ALL) a.out

.SUFFIXES: .c .o
.c.o:
	$(CC) $(CFLAGS) -c $*.c

galois.o: galois.h
jerasure.o: jerasure.h galois.h
reed_sol.o: jerasure.h galois.h reed_sol.h
cauchy.o: jerasure.h galois.h cauchy.h
liberation.o: jerasure.h galois.h liberation.h

