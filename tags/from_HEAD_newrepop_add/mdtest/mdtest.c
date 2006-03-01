/*
 * Copyright (C) 2003, The Regents of the University of California.
 *  Produced at the Lawrence Livermore National Laboratory.
 *  Written by Christopher J. Morrone <morrone@llnl.gov>,
 *  Bill Loewe <wel@llnl.gov>, and Tyce McLarty <mclarty@llnl.gov>.
 *  All rights reserved.
 *  UCRL-CODE-155800
 *
 *  Please read the COPYRIGHT file.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License (as published by
 *  the Free Software Foundation) version 2, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  terms and conditions of the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * CVS info:
 *   $RCSfile$
 *   $Revision$
 *   $Date$
 *   $Author$
 */



#include "mpi.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>



#define FILEMODE S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH
#define DIRMODE S_IRUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IXOTH
#define MAX_LEN 1024
#define RELEASE_VERS "1.7.1"

typedef struct
{
    double entry[6];
} table_t;

int rank;
int size;
char testdir[MAX_LEN] = ".";
char hostname[MAX_LEN];
char unique_dir[MAX_LEN];
char mk_name[MAX_LEN];
char stat_name[MAX_LEN];
char rm_name[MAX_LEN];
char unique_mk_dir[MAX_LEN];
char unique_chdir_dir[MAX_LEN];
char unique_stat_dir[MAX_LEN];
char unique_rm_dir[MAX_LEN];
char unique_rm_uni_dir[MAX_LEN];
char * write_buffer = NULL;
int shared_file;
int files_only;
int dirs_only;
int pre_delay;
int unique_dir_per_task;
int time_unique_dir_overhead;
int verbose;
int throttle = 1;
int items;
int collective_creates;
int write_bytes = 0;
int sync_file = 0;
MPI_Comm testcomm;
table_t * summary_table;

/* for making/removing unique directory && stating/deleting subdirectory */
enum {MK_UNI_DIR, STAT_SUB_DIR, RM_SUB_DIR, RM_UNI_DIR};

#ifdef __linux__
#define FAIL(msg) do { \
    fprintf(stdout, "%s: Process %d(%s): FAILED in %s, %s: %s\n",\
            timestamp(), rank, hostname, __func__, \
            msg, strerror(errno)); \
    fflush(stdout);\
    MPI_Abort(MPI_COMM_WORLD, 1); \
} while(0)
#else
#define FAIL(msg) do { \
    fprintf(stdout, "%s: Process %d(%s): FAILED at %d, %s: %s\n",\
            timestamp(), rank, hostname, __LINE__, \
            msg, strerror(errno)); \
    fflush(stdout);\
    MPI_Abort(MPI_COMM_WORLD, 1); \
} while(0)
#endif

char *timestamp() {
    static char datestring[80];
    time_t timestamp;

    fflush(stdout);
    timestamp = time(NULL);
    strftime(datestring, 80, "%m/%d/%Y %T", localtime(&timestamp));

    return datestring;
}

int count_tasks_per_node(void) {
    char       localhost[MAX_LEN],
               hostname[MAX_LEN];
    int        count               = 1,
               i;
    MPI_Status status;

    if (gethostname(localhost, MAX_LEN) != 0) {
        FAIL("gethostname()");
    }
    if (rank == 0) {
        /* MPI_receive all hostnames, and compare to local hostname */
        for (i = 0; i < size-1; i++) {
            MPI_Recv(hostname, MAX_LEN, MPI_CHAR, MPI_ANY_SOURCE,
                     MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if (strcmp(hostname, localhost) == 0) {
                count++;
            }
        }
    } else {
        /* MPI_send hostname to root node */
        MPI_Send(localhost, MAX_LEN, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }
    MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);

    return(count);
}

void delay_secs(int delay) {
    if (rank == 0 && delay > 0) {
        if (verbose) {
            fprintf(stdout, "delaying %d seconds . . .\n", delay);
            fflush(stdout);
        }
        sleep(delay);
    }
    MPI_Barrier(testcomm);
}

void offset_timers(double * t, int tcount) {
    double toffset;
    int i;

    toffset = MPI_Wtime() - t[tcount];
    for (i = 0; i < tcount+1; i++) {
        t[i] += toffset;
    }
}

void unique_dir_access(int opt) {
    if (opt == MK_UNI_DIR) {
        if (!shared_file || !rank) {
            if (mkdir(unique_mk_dir, S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
                FAIL("Unable to create unique directory");
            }
        }
	MPI_Barrier(testcomm);
        if (chdir(unique_chdir_dir) == -1) {
            FAIL("Unable to chdir to unique test directory");
        }
    } else if (opt == STAT_SUB_DIR) {
        if (chdir(unique_stat_dir) == -1) {
            FAIL("Unable to chdir to test directory");
        }
    } else if (opt == RM_SUB_DIR) {
        if (chdir(unique_rm_dir) == -1) {
            FAIL("Unable to chdir to test directory");
        }
    } else if (opt == RM_UNI_DIR) {
        if (chdir(unique_rm_uni_dir) == -1) {
            FAIL("Unable to chdir to test directory");
        }
        if (!shared_file || !rank) {
            if (rmdir(unique_rm_dir) == -1) {
                FAIL("Unable to remove unique test directory");
            }
        }
    }
}

void directory_test(int iteration, int ntasks) {
    int i, j, size;
    struct stat buf;
    char dir[MAX_LEN];
    double t[4] = {0};

    MPI_Barrier(testcomm);
    t[0] = MPI_Wtime();
    if (unique_dir_per_task) {
        unique_dir_access(MK_UNI_DIR);
        if (!time_unique_dir_overhead) {
            offset_timers(t, 0);
        }
    }
    /* "touch" the files */
    if (collective_creates) {
        if (rank == 0) {
            for (j = 0; j < ntasks; j++) {
                for (i = 0; i < items; i++) {
                    sprintf(dir, "mdtest.%d.%d", j, i);
                    if (mkdir(dir, DIRMODE) == -1) {
                        FAIL("unable to create directory");
                    }
                }
            }
        }
    } else {
        for (i = 0; i < items; i++) {
            sprintf(dir, "%s%d", mk_name, i);
            if (mkdir(dir, DIRMODE) == -1) {
                FAIL("unable to create directory");
            }
        }
    }
    MPI_Barrier(testcomm);
    t[1] = MPI_Wtime();
    if (unique_dir_per_task) {
        unique_dir_access(STAT_SUB_DIR);
        if (!time_unique_dir_overhead) {
            offset_timers(t, 1);
        }
    }
    for (i = 0; i < items; i++) {
        sprintf(dir, "%s%d", stat_name, i);
        if (stat(dir, &buf) == -1) {
            FAIL("unable to stat directory");
        }
    }
    MPI_Barrier(testcomm);
    t[2] = MPI_Wtime();
    if (unique_dir_per_task) {
        unique_dir_access(RM_SUB_DIR);
        if (!time_unique_dir_overhead) {
            offset_timers(t, 2);
        }
    }
    if (collective_creates) {
        if (rank == 0) {
            for (j = 0; j < ntasks; j++) {
                for (i = 0; i < items; i++) {
                    sprintf(dir, "mdtest.%d.%d", j, i);
                    if (rmdir(dir) == -1) {
                        FAIL("unable to remove directory");
                    }
                }
            }
        }
    } else {
        for (i = 0; i < items; i++) {
            sprintf(dir, "%s%d", rm_name, i);
            if (rmdir(dir) == -1) {
                FAIL("unable to remove directory");
            }
        }
    }
    MPI_Barrier(testcomm);
    if (unique_dir_per_task) {
        unique_dir_access(RM_UNI_DIR);
    }
    t[3] = MPI_Wtime();
    if (unique_dir_per_task && !time_unique_dir_overhead) {
        offset_timers(t, 3);
    }

    if (rank == 0) {
        MPI_Comm_size(testcomm, &size);
        summary_table[iteration].entry[0] = items*size/(t[1] - t[0]);
        summary_table[iteration].entry[1] = items*size/(t[2] - t[1]);
        summary_table[iteration].entry[2] = items*size/(t[3] - t[2]);
        if (verbose) {
            printf("   Directory creation: %10.3f sec, %10.3f ops/sec\n",
                   t[1] - t[0], summary_table[iteration].entry[0]);
            printf("   Directory stat    : %10.3f sec, %10.3f ops/sec\n",
                   t[2] - t[1], summary_table[iteration].entry[1]);
            printf("   Directory removal : %10.3f sec, %10.3f ops/sec\n",
                   t[3] - t[2], summary_table[iteration].entry[2]);
            fflush(stdout);
        }
    }
}

void file_test(int iteration, int ntasks) {
    int i, j, fd, size;
    struct stat buf;
    char file[MAX_LEN];
    double t[4] = {0};

    MPI_Barrier(testcomm);
    t[0] = MPI_Wtime();
    if (unique_dir_per_task) {
        unique_dir_access(MK_UNI_DIR);
        if (!time_unique_dir_overhead) {
            offset_timers(t, 0);
        }
    }
    /* "touch" the files */
    if (collective_creates) {
        if (rank == 0) {
            for (j = 0; j < ntasks; j++) {
                for (i = 0; i < items; i++) {
                    sprintf(file, "mdtest.%d.%d", j, i);
                    if ((fd = creat(file, FILEMODE)) == -1) {
                        FAIL("unable to create file");
                    }
                    if (close(fd) == -1) {
                        FAIL("unable to close file");
                    }
                }
            }
        }
        MPI_Barrier(testcomm);
    }

    for (i = 0; i < items; i++) {
        sprintf(file, "%s%d", mk_name, i);
        if (collective_creates) {
            if ((fd = open(file, O_RDWR)) == -1) {
                FAIL("unable to open file");
            }
        } else {
            if ((fd = creat(file, FILEMODE)) == -1) {
                FAIL("unable to create file");
            }
        }
        if (write_bytes > 0) {
            if (write(fd, write_buffer, write_bytes) != write_bytes)
                FAIL("unable to write file");
        }
        if (sync_file && fsync(fd) == -1) {
            FAIL("unable to sync file");
        }
        if (close(fd) == -1) {
            FAIL("unable to close file");
        }
    }
    MPI_Barrier(testcomm);
    t[1] = MPI_Wtime();
    if (unique_dir_per_task) {
        unique_dir_access(STAT_SUB_DIR);
        if (!time_unique_dir_overhead) {
            offset_timers(t, 1);
        }
    }
    for (i = 0; i < items; i++) {
        sprintf(file, "%s%d", stat_name, i);
        if (stat(file, &buf) == -1) {
            FAIL("unable to stat file");
        }
    }
    MPI_Barrier(testcomm);
    t[2] = MPI_Wtime();
    if (unique_dir_per_task) {
        unique_dir_access(RM_SUB_DIR);
        if (!time_unique_dir_overhead) {
            offset_timers(t, 2);
        }
    }
    if (collective_creates) {
        if (rank == 0) {
            for (j = 0; j < ntasks; j++) {
                for (i = 0; i < items; i++) {
                    sprintf(file, "mdtest.%d.%d", j, i);
                    if (unlink(file) == -1) {
                        FAIL("unable to unlink file");
                    }
                }
            }
        }
    } else {
        for (i = 0; i < items; i++) {
            sprintf(file, "%s%d", rm_name, i);
            if (!(shared_file && rank != 0)) {
                if (unlink(file) == -1) {
                    FAIL("unable to unlink file");
                }
            }
        }
    }
    MPI_Barrier(testcomm);
    if (unique_dir_per_task) {
        unique_dir_access(RM_UNI_DIR);
    }
    t[3] = MPI_Wtime();
    if (unique_dir_per_task && !time_unique_dir_overhead) {
        offset_timers(t, 3);
    }

    if (rank == 0) {
        MPI_Comm_size(testcomm, &size);
        summary_table[iteration].entry[3] = items*size/(t[1] - t[0]);
        summary_table[iteration].entry[4] = items*size/(t[2] - t[1]);
        summary_table[iteration].entry[5] = items*size/(t[3] - t[2]);
        if (verbose) {
            printf("   File creation     : %10.3f sec, %10.3f ops/sec\n",
                   t[1] - t[0], summary_table[iteration].entry[3]);
            printf("   File stat         : %10.3f sec, %10.3f ops/sec\n",
                   t[2] - t[1], summary_table[iteration].entry[4]);
            printf("   File removal      : %10.3f sec, %10.3f ops/sec\n",
                   t[3] - t[2], summary_table[iteration].entry[5]);
            fflush(stdout);
        }
    }
}

void print_help() {
    char * opts[] = {
"Usage: mdtest [-h] [-f first] [-i iterations] [-l last] [-s stride] [-n #]",
"              [-p seconds] [-d testdir] [-t] [-u] [-v] [-D] [-F] [-N #] [-S]",
"              [-V #]",
"\t-h: prints this help message",
"\t-c: collective creates: task 0 does all creates",
"\t-d: the directory in which the tests will run",
"\t-f: first number of tasks on which the test will run",
"\t-i: number of iterations the test will run",
"\t-l: last number of tasks on which the test will run",
"\t-n: every process will creat/stat/remove # directories and files",
"\t-p: pre-iteration delay (in seconds)",
"\t-s: stride between the number of tasks for each test",
"\t-t: time unique working directory overhead",
"\t-u: unique working directory for each task",
"\t-v: verbosity (each instance of option increments by one)",
"\t-w: bytes to write to each file after it is created",
"\t-D: perform test on directories only (no files)",
"\t-F: perform test on files only (no directories)",
"\t-N: stride # between neighbor tasks for file/dir stat (local=0)",
"\t-S: shared file access (file only, no directories)",
"\t-V: verbosity value",
""
};
    int i, j;

    for (i = 0; strlen(opts[i]) > 0; i++)
        printf("%s\n", opts[i]);
    fflush(stdout);

    MPI_Initialized(&j);
    if (j) {
        MPI_Finalize();
    }
    exit(0);
}

void summarize_results(int iterations) {
    char access[MAX_LEN];
    int i, j;
    int start, stop;
    double min, max, mean, sd, sum = 0, var = 0;

    printf("\nSUMMARY: (of %d iterations)\n", iterations);
    printf("   Operation                  Max        Min       Mean    Std Dev\n");
    printf("   ---------                  ---        ---       ----    -------\n");
    fflush(stdout);
    /* if files only access, skip entries 0-2 (the dir tests) */
    if (files_only && !dirs_only) {
        start = 3;
    } else {
        start = 0;
    }

    /* if directories only access, skip entries 3-5 (the file tests) */
    if (dirs_only && !files_only) {
        stop = 3;
    } else {
        stop = 6;
    }

    /* special case: if no directory or file tests, skip all */
    if (!dirs_only && !files_only) {
        start = stop = 0;
    }

    for (i = start; i < stop; i++) {
        min = max = summary_table[0].entry[i];
        for (j = 0; j < iterations; j++) {
            if (min > summary_table[j].entry[i]) {
                min = summary_table[j].entry[i];
            }
            if (max < summary_table[j].entry[i]) {
                max = summary_table[j].entry[i];
            }
	    sum += summary_table[j].entry[i];
        }
	mean = sum / iterations;
        for (j = 0; j < iterations; j++) {
	    var += pow((mean - summary_table[j].entry[i]), 2);
        }
	var = var / iterations;
	sd = sqrt(var);
        switch (i) {
            case 0: strcpy(access, "Directory creation:"); break;
            case 1: strcpy(access, "Directory stat    :"); break;
            case 2: strcpy(access, "Directory removal :"); break;
            case 3: strcpy(access, "File creation     :"); break;
            case 4: strcpy(access, "File stat         :"); break;
            case 5: strcpy(access, "File removal      :"); break;
           default: strcpy(access, "ERR");                 break;
        }
	printf("   %s ", access);
	printf("%10.3f ", max);
	printf("%10.3f ", min);
	printf("%10.3f ", mean);
	printf("%10.3f\n", sd);
        fflush(stdout);
	sum = var = 0;
    }
}

void valid_tests() {
    /* if dirs_only and files_only were both left unset, set both now */
    if (!dirs_only && !files_only) {
        dirs_only = files_only = 1;
    }

    /* if shared file 'S' access, no directory tests */
    if (shared_file) {
        dirs_only = 0;
    }

    /* check for collective_creates incompatibilities */
    if (unique_dir_per_task && collective_creates && rank == 0) {
        FAIL("-c not compatible with -u");
    }
    if (shared_file && collective_creates && rank == 0) {
        FAIL("-c not compatible with -S");
    }
}

int main(int argc, char **argv) {
    char dfCall[MAX_LEN] = {0}; /* for disk usage call */
    int i, j, c;
    int nodeCount;
    MPI_Group worldgroup, testgroup;
    struct {
        int first;
        int last;
        int stride;
    } range = {0, 0, 1};
    int first = 1;
    int last = 0;
    int stride = 1;
    int nstride = 0; /* neighbor stride */
    int iterations = 1;

    /* Check for -h parameter before MPI_Init so the mdtest binary can be
       called directly, without, for instance, mpirun. */
    for (i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
            print_help();
        }
    }

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    nodeCount = size / count_tasks_per_node();

    if (rank == 0) {
        printf("-- started at %s --\n\n", timestamp());
        printf("mdtest-%s was launched with %d total task(s) on %d nodes\n",
                RELEASE_VERS, size, nodeCount);
        fflush(stdout);
    }

    /* Parse command line options */
    while (1) {
        c = getopt(argc, argv, "cd:Df:Fhi:l:n:N:p:s:StuvV:w:y");
        if (c == -1) {
            break;
        }

        switch (c) {
            case 'c':
                collective_creates = 1;       break;
            case 'd':
                strcpy(testdir, optarg);      break;
            case 'D':
                dirs_only = 1;                break;
            case 'f':
                first = atoi(optarg);         break;
            case 'F':
                files_only = 1;               break;
            case 'h':
                print_help();                 break;
            case 'i':
                iterations = atoi(optarg);    break;
            case 'l':
                last = atoi(optarg);          break;
            case 'n':
                items = atoi(optarg);         break;
            case 'N':
                nstride = atoi(optarg);       break;
            case 'p':
                pre_delay = atoi(optarg);     break;
            case 's':
                stride = atoi(optarg);        break;
            case 'S':
                shared_file = 1;              break;
            case 't':
                time_unique_dir_overhead = 1; break;
            case 'u':
                unique_dir_per_task = 1;      break;
            case 'v':
                verbose += 1;                 break;
            case 'V':
                verbose = atoi(optarg);       break;
            case 'w':
                write_bytes = atoi(optarg);   break;
            case 'y':
                sync_file = 1;                break;
        }
    }

    valid_tests();

    if (rank == 0 && 0) {
        fprintf(stdout, "Command line used:");
        for (i = 0; i < argc; i++) {
            fprintf(stdout, " %s", argv[i]);
        }
	fprintf(stdout, "\n");
        fflush(stdout);

        /* display disk usage */
        sprintf(dfCall, "df %s\n", testdir);
        system(dfCall);
    }

    /* allocate and initialize write buffer with # */
    if (write_bytes > 0) {
        write_buffer = (char *)malloc(write_bytes);
        memset(write_buffer, 0x23, write_bytes);
    }

    if (testdir != NULL) {
		mkdir(testdir, DIRMODE);
        if (chdir(testdir) == -1) {
            FAIL("Unable to chdir to test directory");
        }
    }
    if (gethostname(hostname, MAX_LEN) == -1) {
        perror("gethostname");
        MPI_Abort(MPI_COMM_WORLD, 2);
    }
    if (last == 0) {
        first = size;
        last = size;
    }

    /* default use shared directory */
    strcpy(mk_name, "mdtest.shared.");
    strcpy(stat_name, "mdtest.shared.");
    strcpy(rm_name, "mdtest.shared.");
    sprintf(unique_mk_dir, "%s/shared", testdir);
    sprintf(unique_chdir_dir, "%s/shared", testdir);
    sprintf(unique_stat_dir, "%s/shared", testdir);
    sprintf(unique_rm_dir, "%s/shared", testdir);
    sprintf(unique_rm_uni_dir, "%s", testdir);

    /* setup summary table for recording results */
    summary_table = (table_t *)malloc(iterations * sizeof(table_t));

    MPI_Comm_group(MPI_COMM_WORLD, &worldgroup);
    /* Run the tests */
    for (i = first; i <= last && i <= size; i += stride) {
        range.last = i - 1;
        MPI_Group_range_incl(worldgroup, 1, (void *)&range, &testgroup);
        MPI_Comm_create(MPI_COMM_WORLD, testgroup, &testcomm);
        if (rank == 0) {
            if (files_only && dirs_only) {
                printf("\n%d tasks, %d files/directories\n", i, i * items);
            } else if (files_only) {
                printf("\n%d tasks, %d files\n", i, i * items);
            } else if (dirs_only) {
                printf("\n%d tasks, %d directories\n", i, i * items);
            }
        }
        if (rank == 0 && verbose) {
            printf("\n");
            printf("   Operation               Duration              Rate\n");
            printf("   ---------               --------              ----\n");
        }
        for (j = 0; j < iterations; j++) {
            if (rank == 0 && verbose) {
                printf(" * iteration %d *\n", j+1);
                fflush(stdout);
            }
            if (rank < i) {
                if (!shared_file) {
                    sprintf(mk_name, "mdtest.%d.", (rank+(0*nstride))%i);
                    sprintf(stat_name, "mdtest.%d.", (rank+(1*nstride))%i);
                    sprintf(rm_name, "mdtest.%d.", (rank+(2*nstride))%i);
                    sprintf(unique_mk_dir, "%s/%d", testdir,
                            (rank+(0*nstride))%i);
                    sprintf(unique_chdir_dir, "%s/%d", testdir,
                            (rank+(1*nstride))%i);
                    sprintf(unique_stat_dir, "%s/%d", testdir,
                            (rank+(2*nstride))%i);
                    sprintf(unique_rm_dir, "%s/%d", testdir,
                            (rank+(3*nstride))%i);
                    sprintf(unique_rm_uni_dir, "%s", testdir);
                }
                if (dirs_only && !shared_file) {
                    if (pre_delay) {
                        delay_secs(pre_delay);
                    }
                    directory_test(j, i);
                }
                if (files_only) {
                    if (pre_delay) {
                        delay_secs(pre_delay);
                    }
                    file_test(j, i);
                }
            }
            MPI_Barrier(MPI_COMM_WORLD);
        }
	if (rank == 0) {
            summarize_results(iterations);
        }
        if (i == 1 && stride > 1) {
            i = 0;
        }
    }
    if (rank == 0) {
        printf("\n-- finished at %s --\n", timestamp());
        fflush(stdout);
    }

    MPI_Finalize();
    exit(0);
}

