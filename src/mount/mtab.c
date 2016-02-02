
/*
 * this code lifted from util-linux-ng, licensed GPLv2+,
 *
 *  git://git.kernel.org/pub/scm/utils/util-linux-ng/util-linux-ng.git
 *
 * whoever decided that each special mount program is responsible
 * for updating /etc/mtab should be spanked.
 *
 * <sage@newdream.net>
 */
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <time.h>
#include <mntent.h>
#include <stdarg.h>

#include "mount/canonicalize.c"


/* Updating mtab ----------------------------------------------*/

/* Flag for already existing lock file. */
static int we_created_lockfile = 0;
static int lockfile_fd = -1;

/* Flag to indicate that signals have been set up. */
static int signals_have_been_setup = 0;

/* Ensure that the lock is released if we are interrupted.  */
extern char *strsignal(int sig);	/* not always in <string.h> */

static void
setlkw_timeout (int sig) {
     /* nothing, fcntl will fail anyway */
}

#define _PATH_MOUNTED "/etc/mtab"
#define _PATH_MOUNTED_LOCK "/etc/mtab~"
#define PROC_SUPER_MAGIC      0x9fa0

/* exit status - bits below are ORed */
#define EX_USAGE        1       /* incorrect invocation or permission */
#define EX_SYSERR       2       /* out of memory, cannot fork, ... */
#define EX_SOFTWARE     4       /* internal mount bug or wrong version */
#define EX_USER         8       /* user interrupt */
#define EX_FILEIO      16       /* problems writing, locking, ... mtab/fstab */
#define EX_FAIL        32       /* mount failure */
#define EX_SOMEOK      64       /* some mount succeeded */

int die(int err, const char *fmt, ...) {
        va_list args;

        va_start(args, fmt);
        vfprintf(stderr, fmt, args);
        fprintf(stderr, "\n");
        va_end(args);

        exit(err);
}

static void
handler (int sig) {
	die(EX_USER, "%s", strsignal(sig));
}

/* Remove lock file.  */
void
unlock_mtab (void) {
	if (we_created_lockfile) {
		close(lockfile_fd);
		lockfile_fd = -1;
		unlink (_PATH_MOUNTED_LOCK);
		we_created_lockfile = 0;
	}
}

/* Create the lock file.
   The lock file will be removed if we catch a signal or when we exit. */
/* The old code here used flock on a lock file /etc/mtab~ and deleted
   this lock file afterwards. However, as rgooch remarks, that has a
   race: a second mount may be waiting on the lock and proceed as
   soon as the lock file is deleted by the first mount, and immediately
   afterwards a third mount comes, creates a new /etc/mtab~, applies
   flock to that, and also proceeds, so that the second and third mount
   now both are scribbling in /etc/mtab.
   The new code uses a link() instead of a creat(), where we proceed
   only if it was us that created the lock, and hence we always have
   to delete the lock afterwards. Now the use of flock() is in principle
   superfluous, but avoids an arbitrary sleep(). */

/* Where does the link point to? Obvious choices are mtab and mtab~~.
   HJLu points out that the latter leads to races. Right now we use
   mtab~.<pid> instead. Use 20 as upper bound for the length of %d. */
#define MOUNTLOCK_LINKTARGET		_PATH_MOUNTED_LOCK "%d"
#define MOUNTLOCK_LINKTARGET_LTH	(sizeof(_PATH_MOUNTED_LOCK)+20)

/*
 * The original mount locking code has used sleep(1) between attempts and
 * maximal number of attemps has been 5.
 *
 * There was very small number of attempts and extremely long waiting (1s)
 * that is useless on machines with large number of concurret mount processes.
 *
 * Now we wait few thousand microseconds between attempts and we have global
 * time limit (30s) rather than limit for number of attempts. The advantage
 * is that this method also counts time which we spend in fcntl(F_SETLKW) and
 * number of attempts is not so much restricted.
 *
 * -- kzak@redhat.com [2007-Mar-2007]
 */

/* maximum seconds between first and last attempt */
#define MOUNTLOCK_MAXTIME		30

/* sleep time (in microseconds, max=999999) between attempts */
#define MOUNTLOCK_WAITTIME		5000

void
lock_mtab (void) {
	int i;
	struct timespec waittime;
	struct timeval maxtime;
	char linktargetfile[MOUNTLOCK_LINKTARGET_LTH];

	if (!signals_have_been_setup) {
		int sig = 0;
		struct sigaction sa;

		sa.sa_handler = handler;
		sa.sa_flags = 0;
		sigfillset (&sa.sa_mask);

		while (sigismember (&sa.sa_mask, ++sig) != -1
		       && sig != SIGCHLD) {
			if (sig == SIGALRM)
				sa.sa_handler = setlkw_timeout;
			else
				sa.sa_handler = handler;
			sigaction (sig, &sa, (struct sigaction *) 0);
		}
		signals_have_been_setup = 1;
	}

	snprintf(linktargetfile, sizeof(linktargetfile), MOUNTLOCK_LINKTARGET,
		 getpid ());

	i = open (linktargetfile, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR);
	if (i < 0) {
		int errsv = errno;
		/* linktargetfile does not exist (as a file)
		   and we cannot create it. Read-only filesystem?
		   Too many files open in the system?
		   Filesystem full? */
		die (EX_FILEIO, "can't create lock file %s: %s "
		     "(use -n flag to override)",
		     linktargetfile, strerror (errsv));
	}
	close(i);

	gettimeofday(&maxtime, NULL);
	maxtime.tv_sec += MOUNTLOCK_MAXTIME;

	waittime.tv_sec = 0;
	waittime.tv_nsec = (1000 * MOUNTLOCK_WAITTIME);

	/* Repeat until it was us who made the link */
	while (!we_created_lockfile) {
		struct timeval now;
		struct flock flock;
		int errsv, j;

		j = link(linktargetfile, _PATH_MOUNTED_LOCK);
		errsv = errno;

		if (j == 0)
			we_created_lockfile = 1;

		if (j < 0 && errsv != EEXIST) {
			(void) unlink(linktargetfile);
			die (EX_FILEIO, "can't link lock file %s: %s "
			     "(use -n flag to override)",
			     _PATH_MOUNTED_LOCK, strerror (errsv));
		}

		lockfile_fd = open (_PATH_MOUNTED_LOCK, O_WRONLY);

		if (lockfile_fd < 0) {
			/* Strange... Maybe the file was just deleted? */
			int errsv = errno;
			gettimeofday(&now, NULL);
			if (errno == ENOENT && now.tv_sec < maxtime.tv_sec) {
				we_created_lockfile = 0;
				continue;
			}
			(void) unlink(linktargetfile);
			die (EX_FILEIO, "can't open lock file %s: %s "
			     "(use -n flag to override)",
			     _PATH_MOUNTED_LOCK, strerror (errsv));
		}

		flock.l_type = F_WRLCK;
		flock.l_whence = SEEK_SET;
		flock.l_start = 0;
		flock.l_len = 0;

		if (j == 0) {
			/* We made the link. Now claim the lock. */
			if (fcntl (lockfile_fd, F_SETLK, &flock) == -1) {
				/* proceed, since it was us who created the lockfile anyway */
			}
			(void) unlink(linktargetfile);
		} else {
			/* Someone else made the link. Wait. */
			gettimeofday(&now, NULL);
			if (now.tv_sec < maxtime.tv_sec) {
				alarm(maxtime.tv_sec - now.tv_sec);
				if (fcntl (lockfile_fd, F_SETLKW, &flock) == -1) {
					int errsv = errno;
					(void) unlink(linktargetfile);
					die (EX_FILEIO, "can't lock lock file %s: %s",
					     _PATH_MOUNTED_LOCK, (errno == EINTR) ?
					     "timed out" : strerror (errsv));
				}
				alarm(0);

				nanosleep(&waittime, NULL);
			} else {
				(void) unlink(linktargetfile);
				die (EX_FILEIO, "Cannot create link %s\n"
				     "Perhaps there is a stale lock file?\n",
					 _PATH_MOUNTED_LOCK);
			}
			close(lockfile_fd);
		}
	}
}

static void
update_mtab_entry(const char *spec, const char *node, const char *type,
		  const char *opts, int flags, int freq, int pass) {
	struct statfs buf;
	int err = statfs(_PATH_MOUNTED, &buf);
	if (err) {
		printf("mount: can't statfs %s: %s", _PATH_MOUNTED,
		       strerror (err));
		return;
	}
	/* /etc/mtab is symbol link to /proc/self/mounts? */
	if (buf.f_type == PROC_SUPER_MAGIC)
		return;

	if (!opts)
		opts = "rw";

	struct mntent mnt;
	mnt.mnt_fsname = strdup(spec);
	mnt.mnt_dir = canonicalize_path(node);
	mnt.mnt_type = strdup(type);
	mnt.mnt_opts = strdup(opts);
	mnt.mnt_freq = freq;
	mnt.mnt_passno = pass;

	FILE *fp;
	
	lock_mtab();
	fp = setmntent(_PATH_MOUNTED, "a+");
	if (fp == NULL) {
		int errsv = errno;
		printf("mount: can't open %s: %s", _PATH_MOUNTED,
		       strerror (errsv));
	} else {
		if ((addmntent (fp, &mnt)) == 1) {
			int errsv = errno;
			printf("mount: error writing %s: %s",
			      _PATH_MOUNTED, strerror (errsv));
		}
	}
	endmntent(fp);
	unlock_mtab();

	free(mnt.mnt_fsname);
	free(mnt.mnt_dir);
	free(mnt.mnt_type);
	free(mnt.mnt_opts);
}
