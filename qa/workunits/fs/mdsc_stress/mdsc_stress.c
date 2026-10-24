// SPDX-License-Identifier: GPL-2.0
/*
 * mdsc_stress.c - CephFS MDS request dispatch stress generator.
 *
 * This is the teuthology version: it is driven by
 * qa/tasks/cephfs/test_mdsc_stress.py (double-failover chaos) and
 * also runs standalone as a workunit on any client mount - both kclient
 * and ceph-fuse.  It needs no ceph tooling: it only speaks POSIX
 * filesystem calls against the mount.
 *
 * The program only generates load and detects *client visible* symptoms;
 * the kernel-side oracles (lockdep, KASAN, KCSAN, DEBUG_LIST, refcount)
 * are collected from dmesg by the test on kernel client jobs.  What
 * this program adds on top of a generic metadata stressor is:
 *
 *   - a per-operation watchdog.  A metadata request that never comes
 *     back produces no dmesg output at all until the hung-task timer
 *     fires, so it has to be observed from userspace.
 *
 *   - SIGKILLed victim processes.  Killing a task that is blocked on a
 *     parked request aborts the request while the client may be
 *     draining the very wait list it is parked on, which the request
 *     ownership rules have to survive.
 *
 *   - an operation mix that is heavy on write requests and light on
 *     fsync, so that plenty of unsafe requests are replayed on
 *     reconnect.
 *
 * Usage:
 *	mdsc_stress -d <dir> [-t threads] [-s seconds] [-w watchdog_secs]
 *		    [-k victims] [-K min_ms:max_ms] [-i report_secs] [-C] [-v]
 *
 * Internal mode (spawned by the killer thread, not for direct use):
 *	mdsc_stress --victim <dir> <seed>
 *
 * Signals:
 *	SIGTERM/SIGINT	stop the run and print the summary
 *	SIGUSR1		dump the operations that are currently in flight
 *
 * Exit codes:
 *	0	no hang, no unexpected error
 *	1	an operation was still in flight when the run ended, or a
 *		worker thread could not be joined (suspected lost wakeup)
 *	2	an unexpected errno was returned by an operation
 *	3	usage/setup error
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>

#define MAX_WORKERS	256
#define MAX_VICTIMS	64
#define NAME_SLOTS	64
#define LAT_BUCKETS	32

/* ------------------------------------------------------------------ */
/* operations							      */
/* ------------------------------------------------------------------ */

enum {
	OP_LOOKUP,	/* stat() of a name that cannot be cached	*/
	OP_CREATE,
	OP_UNLINK,
	OP_MKDIR,
	OP_RMDIR,
	OP_RENAME,
	OP_LINK,
	OP_SYMLINK,
	OP_READLINK,
	OP_CHMOD,
	OP_TRUNCATE,
	OP_UTIMES,
	OP_SETXATTR,
	OP_GETXATTR,
	OP_READDIR,
	OP_STAT,
	OP_OPEN,
	OP_FSYNC,
	OP_CROSS,	/* touch another worker's directory		*/
	OP_NR
};

static const char * const op_name[OP_NR] = {
	[OP_LOOKUP]	= "lookup",
	[OP_CREATE]	= "create",
	[OP_UNLINK]	= "unlink",
	[OP_MKDIR]	= "mkdir",
	[OP_RMDIR]	= "rmdir",
	[OP_RENAME]	= "rename",
	[OP_LINK]	= "link",
	[OP_SYMLINK]	= "symlink",
	[OP_READLINK]	= "readlink",
	[OP_CHMOD]	= "chmod",
	[OP_TRUNCATE]	= "truncate",
	[OP_UTIMES]	= "utimes",
	[OP_SETXATTR]	= "setxattr",
	[OP_GETXATTR]	= "getxattr",
	[OP_READDIR]	= "readdir",
	[OP_STAT]	= "stat",
	[OP_OPEN]	= "open",
	[OP_FSYNC]	= "fsync",
	[OP_CROSS]	= "cross",
};

/*
 * Weights.  Write operations dominate so that unsafe requests keep
 * piling up between reconnects; OP_LOOKUP is cheap and always reaches
 * the MDS, which is what makes requests pile up on the wait lists while
 * a rank is not active.
 */
static const int op_weight[OP_NR] = {
	[OP_LOOKUP]	= 15,
	[OP_CREATE]	= 12,
	[OP_UNLINK]	= 10,
	[OP_MKDIR]	= 6,
	[OP_RMDIR]	= 5,
	[OP_RENAME]	= 10,
	[OP_LINK]	= 4,
	[OP_SYMLINK]	= 5,
	[OP_READLINK]	= 3,
	[OP_CHMOD]	= 6,
	[OP_TRUNCATE]	= 5,
	[OP_UTIMES]	= 4,
	[OP_SETXATTR]	= 5,
	[OP_GETXATTR]	= 3,
	[OP_READDIR]	= 4,
	[OP_STAT]	= 4,
	[OP_OPEN]	= 4,
	[OP_FSYNC]	= 2,
	[OP_CROSS]	= 8,
};

static int op_pick[256];
static int op_pick_nr;

static void build_op_table(void)
{
	int op, i;

	for (op = 0; op < OP_NR; op++)
		for (i = 0; i < op_weight[op]; i++)
			op_pick[op_pick_nr++] = op;
}

/* ------------------------------------------------------------------ */
/* error classification						      */
/* ------------------------------------------------------------------ */

enum err_class {
	ERR_OK,
	ERR_BENIGN,	/* races between workers over a shared name space  */
	ERR_CHAOS,	/* expected while the cluster is being disrupted	  */
	ERR_BAD		/* nothing in this test should ever produce this	  */
};

static enum err_class classify(int op, int err)
{
	if (!err)
		return ERR_OK;

	/* per-operation exceptions */
	switch (op) {
	case OP_READLINK:
		if (err == EINVAL)		/* not a symlink */
			return ERR_BENIGN;
		break;
	case OP_LINK:
		if (err == EPERM || err == EMLINK)
			return ERR_BENIGN;
		break;
	case OP_TRUNCATE:
	case OP_OPEN:
	case OP_FSYNC:
		if (err == EISDIR)
			return ERR_BENIGN;
		break;
	default:
		break;
	}

	switch (err) {
	case ENOENT:
	case EEXIST:
	case ENOTEMPTY:
	case ENOTDIR:
	case EISDIR:
	case ENODATA:
	case EBUSY:
	case ELOOP:
		return ERR_BENIGN;

	/*
	 * Session teardown, blocklisting and forced reconnects are part
	 * of the test; they must not be silently ignored, but they are
	 * not failures either.  The runner reports the counts.
	 */
	case EIO:
	case ESTALE:
	case ENOTCONN:
	case ESHUTDOWN:
	case ETIMEDOUT:
	case EINTR:
	case EAGAIN:
	case EACCES:
	case EPERM:
	case ECONNABORTED:
	case ECONNRESET:
	case EHOSTUNREACH:
	case ENOMEM:
		return ERR_CHAOS;

	default:
		return ERR_BAD;
	}
}

/* ------------------------------------------------------------------ */
/* shared state							      */
/* ------------------------------------------------------------------ */

/*
 * One publication slot per worker.  The worker publishes the operation
 * it is about to issue, runs it, then retires the slot.  seq is a plain
 * sequence counter: odd means "in flight".  The watchdog reads it to
 * find operations that never came back, which is the only userspace
 * visible symptom of a lost wakeup on a request with r_timeout == 0.
 */
struct slot {
	_Atomic unsigned long	seq;
	_Atomic uint64_t	start_ns;
	_Atomic int		op;
	char			path[PATH_MAX];
	char			pad[64];
} __attribute__((aligned(64)));

struct worker {
	pthread_t		tid;
	int			idx;
	unsigned int		seed;
	char			dir[PATH_MAX];
	unsigned long		ops[OP_NR];
	unsigned long		errs[OP_NR][4];		/* by err_class */
	unsigned long		errno_hist[256];
	unsigned long		lat[LAT_BUCKETS];
	uint64_t		max_lat_ns;
	int			max_lat_op;
	bool			joined;
};

static struct slot		g_slot[MAX_WORKERS];
static struct worker		g_worker[MAX_WORKERS];
static int			g_nworkers = 16;
static int			g_nvictims = 6;
static int			g_seconds = 60;
static int			g_watchdog = 120;
static int			g_report = 10;
static int			g_kill_min_ms = 200;
static int			g_kill_max_ms = 2000;
static bool			g_keep;
static bool			g_verbose;
static char			g_root[PATH_MAX];

static volatile sig_atomic_t	g_stop;
static volatile sig_atomic_t	g_dump;
static atomic_ulong		g_total_ops;
static atomic_int		g_slow_events;
static atomic_int		g_victim_kills;
static uint64_t			g_start_ns;

static pid_t			g_victim[MAX_VICTIMS];
static pthread_mutex_t		g_victim_lock = PTHREAD_MUTEX_INITIALIZER;
static char			g_self[PATH_MAX];

/* ------------------------------------------------------------------ */
/* helpers							      */
/* ------------------------------------------------------------------ */

static uint64_t now_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

static void msleep(unsigned int ms)
{
	struct timespec ts = {
		.tv_sec  = ms / 1000,
		.tv_nsec = (long)(ms % 1000) * 1000000L,
	};

	while (nanosleep(&ts, &ts) < 0 && errno == EINTR)
		;
}

static int lat_bucket(uint64_t ns)
{
	int b = 0;
	uint64_t us = ns / 1000;

	while (us) {
		b++;
		us >>= 1;
	}
	return b < LAT_BUCKETS ? b : LAT_BUCKETS - 1;
}

static void die(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fprintf(stderr, "mdsc_stress: ");
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fprintf(stderr, "\n");
	exit(3);
}

static void on_stop(int sig)
{
	(void)sig;
	g_stop = 1;
}

static void on_dump(int sig)
{
	(void)sig;
	g_dump = 1;
}

/* ------------------------------------------------------------------ */
/* the operations						      */
/* ------------------------------------------------------------------ */

/*
 * Every operation below issues at least one MDS request when it is not
 * satisfied from the dentry/inode cache.  run_op() returns 0 or -errno.
 */
static int run_op(const char *dir, int op, unsigned int *seed, char *pub,
		  size_t publen)
{
	char p1[PATH_MAX], p2[PATH_MAX], buf[256];
	int a = rand_r(seed) % NAME_SLOTS;
	int b = rand_r(seed) % NAME_SLOTS;
	struct timeval tv[2];
	struct stat st;
	DIR *d;
	int fd, ret;

	switch (op) {
	case OP_LOOKUP:
		/*
		 * A name that has never existed: not in the dentry cache,
		 * so this always reaches the MDS.  It is the cheapest way
		 * to keep requests parked while a rank is not active.
		 */
		snprintf(p1, sizeof(p1), "%s/miss-%u-%u", dir,
			 (unsigned)getpid(), (unsigned)rand_r(seed));
		snprintf(pub, publen, "%s", p1);
		ret = lstat(p1, &st);
		if (ret < 0 && errno == ENOENT)
			ret = 0;	/* the miss is the expected result */
		break;

	case OP_CREATE:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		fd = open(p1, O_CREAT | O_WRONLY, 0644);
		if (fd < 0) {
			ret = -1;
			break;
		}
		ret = write(fd, "x", 1) < 0 ? -1 : 0;
		close(fd);
		break;

	case OP_UNLINK:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		ret = unlink(p1);
		break;

	case OP_MKDIR:
		snprintf(p1, sizeof(p1), "%s/d%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		ret = mkdir(p1, 0755);
		break;

	case OP_RMDIR:
		snprintf(p1, sizeof(p1), "%s/d%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		ret = rmdir(p1);
		break;

	case OP_RENAME:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(p2, sizeof(p2), "%s/f%02d", dir, b);
		snprintf(pub, publen, "%s -> %s", p1, p2);
		ret = (a == b) ? 0 : rename(p1, p2);
		break;

	case OP_LINK:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(p2, sizeof(p2), "%s/h%02d", dir, b);
		snprintf(pub, publen, "%s => %s", p1, p2);
		unlink(p2);
		ret = link(p1, p2);
		break;

	case OP_SYMLINK:
		snprintf(p1, sizeof(p1), "%s/s%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		unlink(p1);
		snprintf(buf, sizeof(buf), "f%02d", b);
		ret = symlink(buf, p1);
		break;

	case OP_READLINK:
		snprintf(p1, sizeof(p1), "%s/s%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		ret = readlink(p1, buf, sizeof(buf)) < 0 ? -1 : 0;
		break;

	case OP_CHMOD:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		ret = chmod(p1, (rand_r(seed) & 1) ? 0644 : 0600);
		break;

	case OP_TRUNCATE:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		ret = truncate(p1, rand_r(seed) % 4096);
		break;

	case OP_UTIMES:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		gettimeofday(&tv[0], NULL);
		tv[1] = tv[0];
		ret = utimes(p1, tv);
		break;

	case OP_SETXATTR:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		snprintf(buf, sizeof(buf), "v%u", (unsigned)rand_r(seed));
		ret = setxattr(p1, "user.mdsc", buf, strlen(buf), 0);
		break;

	case OP_GETXATTR:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		ret = getxattr(p1, "user.mdsc", buf, sizeof(buf)) < 0 ? -1 : 0;
		break;

	case OP_READDIR:
		snprintf(pub, publen, "%s", dir);
		d = opendir(dir);
		if (!d) {
			ret = -1;
			break;
		}
		errno = 0;
		while (readdir(d))
			;
		ret = errno ? -1 : 0;
		closedir(d);
		break;

	case OP_STAT:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		ret = lstat(p1, &st);
		break;

	case OP_OPEN:
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		fd = open(p1, O_RDONLY);
		if (fd < 0) {
			ret = -1;
			break;
		}
		close(fd);
		ret = 0;
		break;

	case OP_FSYNC:
		/*
		 * Kept rare on purpose: fsync() waits for the safe reply
		 * and thereby drains the unsafe requests, while the replay
		 * path is only interesting when there are unsafe requests
		 * to replay.
		 */
		snprintf(p1, sizeof(p1), "%s/f%02d", dir, a);
		snprintf(pub, publen, "%s", p1);
		fd = open(p1, O_WRONLY | O_CREAT, 0644);
		if (fd < 0) {
			ret = -1;
			break;
		}
		if (write(fd, "y", 1) < 0) {
			close(fd);
			ret = -1;
			break;
		}
		ret = fsync(fd);
		close(fd);
		break;

	case OP_CROSS:
		/*
		 * Reach into another worker's directory.  With the per
		 * directory pins that qa/tasks/cephfs/test_mdsc_stress.py
		 * sets up, these paths resolve on a different rank than
		 * the caller's own tree, which is what makes the MDS
		 * forward the request to the authoritative rank.
		 */
		snprintf(p1, sizeof(p1), "%s/w%02d/f%02d", g_root,
			 rand_r(seed) % g_nworkers, a);
		snprintf(pub, publen, "%s", p1);
		if (rand_r(seed) & 1) {
			ret = lstat(p1, &st);
		} else {
			fd = open(p1, O_CREAT | O_WRONLY, 0644);
			if (fd < 0) {
				ret = -1;
				break;
			}
			close(fd);
			ret = 0;
		}
		break;

	default:
		snprintf(pub, publen, "?");
		ret = 0;
		break;
	}

	return ret < 0 ? -errno : 0;
}

/* ------------------------------------------------------------------ */
/* worker							      */
/* ------------------------------------------------------------------ */

static void *worker_fn(void *arg)
{
	struct worker *w = arg;
	struct slot *s = &g_slot[w->idx];
	char pub[PATH_MAX];

	while (!g_stop) {
		int op = op_pick[rand_r(&w->seed) % op_pick_nr];
		unsigned long seq;
		uint64_t t0, dt;
		int err, cls, b;

		pub[0] = '\0';
		snprintf(pub, sizeof(pub), "%s", w->dir);

		/* publish: odd seq means "in flight" */
		seq = atomic_load_explicit(&s->seq, memory_order_relaxed);
		atomic_store_explicit(&s->seq, seq + 1, memory_order_relaxed);
		atomic_store_explicit(&s->op, op, memory_order_relaxed);
		memcpy(s->path, pub, strlen(pub) + 1);
		t0 = now_ns();
		atomic_store_explicit(&s->start_ns, t0, memory_order_release);

		err = -run_op(w->dir, op, &w->seed, s->path, sizeof(s->path));

		dt = now_ns() - t0;
		atomic_store_explicit(&s->start_ns, 0, memory_order_release);
		atomic_store_explicit(&s->seq, seq + 2, memory_order_relaxed);

		w->ops[op]++;
		b = lat_bucket(dt);
		w->lat[b]++;
		if (dt > w->max_lat_ns) {
			w->max_lat_ns = dt;
			w->max_lat_op = op;
		}

		cls = classify(op, err);
		w->errs[op][cls]++;
		if (err > 0 && err < 256)
			w->errno_hist[err]++;
		if (cls == ERR_BAD)
			fprintf(stderr,
				"UNEXPECTED: op=%s path=%s errno=%d (%s)\n",
				op_name[op], s->path, err, strerror(err));

		atomic_fetch_add_explicit(&g_total_ops, 1,
					  memory_order_relaxed);
	}

	return NULL;
}

/* ------------------------------------------------------------------ */
/* watchdog							      */
/* ------------------------------------------------------------------ */

/*
 * Read one publication slot consistently.  The producer retires a slot
 * by clearing start_ns and bumping seq (odd -> even), and can
 * immediately re-publish it (even -> odd, new start_ns).  A reader must
 * therefore treat seq and start_ns as a pair: if a re-read of seq does
 * not match the first read, the start_ns value belonged to a different
 * generation and must not be used to compute an elapsed time (the
 * absurd "585 years" reports in the first run were exactly this race).
 */
enum slot_state {
	SLOT_IDLE,	/* even seq: nothing in flight		    */
	SLOT_BUSY,	/* odd seq, start_ns set, op valid	    */
	SLOT_TORN	/* changed between the two seq reads: retry */
};

static enum slot_state read_slot(struct slot *s, unsigned long *seq,
				 uint64_t *t0, int *op)
{
	unsigned long a, b;
	int i;

	for (i = 0; i < 4; i++) {
		a = atomic_load_explicit(&s->seq, memory_order_relaxed);
		*t0 = atomic_load_explicit(&s->start_ns,
					   memory_order_relaxed);
		b = atomic_load_explicit(&s->seq, memory_order_relaxed);

		if (a != b)
			continue;
		*seq = a;
		if (!(a & 1) || !*t0) {
			*op = -1;
			return SLOT_IDLE;
		}
		*op = atomic_load_explicit(&s->op, memory_order_relaxed);
		return SLOT_BUSY;
	}
	return SLOT_TORN;
}

/*
 * Report an operation that has been in flight for longer than
 * g_watchdog seconds.  Each (worker, seq) pair is reported once so a
 * genuinely hung request does not flood the log.
 */
static void *watchdog_fn(void *arg)
{
	static unsigned long reported[MAX_WORKERS];
	uint64_t thresh = (uint64_t)g_watchdog * 1000000000ull;
	int i;

	(void)arg;

	while (!g_stop) {
		uint64_t now;
		bool dump;

		msleep(500);
		dump = g_dump;
		g_dump = 0;
		now = now_ns();

		for (i = 0; i < g_nworkers; i++) {
			struct slot *s = &g_slot[i];
			unsigned long seq;
			uint64_t t0;
			int op;

			if (read_slot(s, &seq, &t0, &op) != SLOT_BUSY)
				continue;

			/*
			 * Take "now" per slot, after the slot is confirmed
			 * busy: a worker can publish between the scan-wide
			 * timestamp above and this read, putting t0 in the
			 * future relative to it and wrapping the elapsed
			 * computation.  t0 was read inside read_slot() and
			 * the clock is monotonic, so now >= t0 here.
			 */
			now = now_ns();

			if (dump) {
				printf("INFLIGHT: worker=%d op=%s elapsed=%.1fs path=%s\n",
				       i, op_name[op],
				       (double)(now - t0) / 1e9, s->path);
				continue;
			}

			if (now - t0 < thresh || reported[i] == seq)
				continue;

			reported[i] = seq;
			atomic_fetch_add_explicit(&g_slow_events, 1,
						  memory_order_relaxed);
			printf("SLOW: worker=%d op=%s elapsed=%.1fs path=%s\n",
			       i, op_name[op], (double)(now - t0) / 1e9,
			       s->path);
			fflush(stdout);
		}
		if (dump)
			fflush(stdout);
	}

	return NULL;
}

/* ------------------------------------------------------------------ */
/* victims							      */
/* ------------------------------------------------------------------ */

/*
 * A victim is a separate single threaded process running the same
 * operation mix in its own subtree.  The killer thread SIGKILLs it at
 * random and restarts it.  When the victim happens to be blocked on a
 * request that is parked on a wait list, the kill aborts that request
 * while the client may be draining the very same list.
 */
static pid_t spawn_victim(int idx)
{
	char dir[PATH_MAX], seed[32], nw[32];
	pid_t pid;

	snprintf(dir, sizeof(dir), "%s/v%02d", g_root, idx);
	snprintf(seed, sizeof(seed), "%u",
		 (unsigned)(now_ns() ^ ((uint64_t)idx << 16)));
	snprintf(nw, sizeof(nw), "%d", g_nworkers);

	pid = fork();
	if (pid < 0)
		return -1;
	if (pid == 0) {
		/* do not outlive the parent if it is killed outright */
		prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0);
		/*
		 * Re-exec immediately: fork() from a multithreaded parent
		 * only guarantees async-signal-safe calls in the child.
		 */
		execl(g_self, "mdsc_stress", "--victim", dir, seed, nw,
		      (char *)NULL);
		_exit(127);
	}
	return pid;
}

/*
 * Reap a SIGKILLed victim, but never block forever on it: a task that
 * is inside an MDS request only dies once the request is aborted, and
 * if that never happens the run must still be able to report the hang
 * instead of stalling here.
 */
static void reap_victim(pid_t pid, int timeout_ms)
{
	int waited = 0;

	while (waited < timeout_ms) {
		if (waitpid(pid, NULL, WNOHANG) == pid)
			return;
		msleep(50);
		waited += 50;
	}
	fprintf(stderr, "WARNING: victim %d did not die within %dms\n",
		(int)pid, timeout_ms);
}

static void *killer_fn(void *arg)
{
	unsigned int seed = (unsigned int)now_ns();
	int span = g_kill_max_ms - g_kill_min_ms;
	int i;

	(void)arg;

	if (span < 1)
		span = 1;

	while (!g_stop) {
		pid_t pid;
		int idx;

		msleep(g_kill_min_ms + rand_r(&seed) % span);
		if (g_stop)
			break;

		idx = rand_r(&seed) % g_nvictims;

		pthread_mutex_lock(&g_victim_lock);
		pid = g_victim[idx];
		if (pid > 0) {
			kill(pid, SIGKILL);
			reap_victim(pid, 30000);
			g_victim[idx] = 0;
			atomic_fetch_add_explicit(&g_victim_kills, 1,
						  memory_order_relaxed);
		}
		if (!g_stop)
			g_victim[idx] = spawn_victim(idx);
		pthread_mutex_unlock(&g_victim_lock);

		/* reap and restart victims that died on their own */
		pthread_mutex_lock(&g_victim_lock);
		for (i = 0; i < g_nvictims; i++) {
			if (g_victim[i] <= 0)
				continue;
			if (waitpid(g_victim[i], NULL, WNOHANG) ==
			    g_victim[i])
				g_victim[i] = g_stop ? 0 : spawn_victim(i);
		}
		pthread_mutex_unlock(&g_victim_lock);
	}

	pthread_mutex_lock(&g_victim_lock);
	for (i = 0; i < g_nvictims; i++) {
		if (g_victim[i] > 0)
			kill(g_victim[i], SIGKILL);
	}
	for (i = 0; i < g_nvictims; i++) {
		if (g_victim[i] > 0) {
			reap_victim(g_victim[i], 30000);
			g_victim[i] = 0;
		}
	}
	pthread_mutex_unlock(&g_victim_lock);

	return NULL;
}

static int victim_main(const char *dir, unsigned int seed, int nworkers)
{
	char pub[PATH_MAX];
	char *slash;

	/*
	 * OP_CROSS builds paths from g_root, which the option parser
	 * never fills in for a victim: derive it from the victim's own
	 * directory ("<root>/vNN") so that cross-rank traffic stays
	 * inside the test tree.
	 */
	snprintf(g_root, sizeof(g_root), "%s", dir);
	slash = strrchr(g_root, '/');
	if (slash && slash != g_root)
		*slash = '\0';
	if (nworkers > 0 && nworkers <= MAX_WORKERS)
		g_nworkers = nworkers;

	if (mkdir(dir, 0755) < 0 && errno != EEXIST)
		return 1;

	/*
	 * No stop condition: the process exists to be SIGKILLed while it
	 * is blocked inside an MDS request.
	 */
	for (;;) {
		int op = op_pick[rand_r(&seed) % op_pick_nr];

		run_op(dir, op, &seed, pub, sizeof(pub));
	}
	return 0;
}

/* ------------------------------------------------------------------ */
/* setup / teardown / reporting					      */
/* ------------------------------------------------------------------ */

static void mkdir_p(const char *path)
{
	if (mkdir(path, 0755) < 0 && errno != EEXIST)
		die("mkdir %s: %s", path, strerror(errno));
}

static void rm_rf(const char *path)
{
	char cmd[PATH_MAX + 32];

	snprintf(cmd, sizeof(cmd), "rm -rf -- '%s'", path);
	if (system(cmd) != 0)
		fprintf(stderr, "mdsc_stress: cleanup of %s failed\n", path);
}

static void report(int hung, int bad)
{
	unsigned long tot[OP_NR] = { 0 };
	unsigned long cls[4] = { 0 };
	unsigned long errno_hist[256] = { 0 };
	unsigned long lat[LAT_BUCKETS] = { 0 };
	uint64_t max_lat = 0;
	int max_lat_op = 0;
	unsigned long total = 0;
	double secs;
	int i, op;

	for (i = 0; i < g_nworkers; i++) {
		struct worker *w = &g_worker[i];

		for (op = 0; op < OP_NR; op++) {
			int c;

			tot[op] += w->ops[op];
			total += w->ops[op];
			for (c = 0; c < 4; c++)
				cls[c] += w->errs[op][c];
		}
		for (op = 0; op < 256; op++)
			errno_hist[op] += w->errno_hist[op];
		for (op = 0; op < LAT_BUCKETS; op++)
			lat[op] += w->lat[op];
		if (w->max_lat_ns > max_lat) {
			max_lat = w->max_lat_ns;
			max_lat_op = w->max_lat_op;
		}
	}

	secs = (double)(now_ns() - g_start_ns) / 1e9;

	printf("\n===== mdsc_stress summary =====\n");
	printf("duration      : %.1fs\n", secs);
	printf("workers       : %d\n", g_nworkers);
	printf("victims       : %d (killed %d times)\n", g_nvictims,
	       atomic_load(&g_victim_kills));
	printf("operations    : %lu (%.0f op/s)\n", total,
	       secs > 0 ? total / secs : 0.0);
	printf("  ok          : %lu\n", cls[ERR_OK]);
	printf("  benign      : %lu\n", cls[ERR_BENIGN]);
	printf("  chaos       : %lu\n", cls[ERR_CHAOS]);
	printf("  unexpected  : %lu\n", cls[ERR_BAD]);
	printf("slow events   : %d (> %ds in flight)\n",
	       atomic_load(&g_slow_events), g_watchdog);
	printf("max latency   : %.1fs (%s)\n", (double)max_lat / 1e9,
	       op_name[max_lat_op]);

	printf("\nper operation:\n");
	for (op = 0; op < OP_NR; op++) {
		if (!tot[op])
			continue;
		printf("  %-9s %8lu\n", op_name[op], tot[op]);
	}

	printf("\nerrno histogram:\n");
	for (i = 0; i < 256; i++) {
		if (errno_hist[i])
			printf("  %-14s %8lu\n", strerror(i), errno_hist[i]);
	}

	if (g_verbose) {
		printf("\nlatency (log2 us buckets):\n");
		for (i = 0; i < LAT_BUCKETS; i++) {
			if (lat[i])
				printf("  2^%-2d us %10lu\n", i, lat[i]);
		}
	}

	printf("\nRESULT: %s\n",
	       (hung || bad) ? "FAIL" : "PASS");
	if (hung)
		printf("REASON: %d worker(s) still blocked in an MDS request "
		       "(suspected lost wakeup)\n", hung);
	if (bad)
		printf("REASON: %lu unexpected errno(s)\n", cls[ERR_BAD]);
	fflush(stdout);
}

static void usage(void)
{
	fprintf(stderr,
"usage: mdsc_stress -d <dir> [options]\n"
"  -d DIR      test directory on a cephfs mount (required)\n"
"  -t N        worker threads              (default 16)\n"
"  -s SEC      run duration, 0 = until signalled (default 60)\n"
"  -w SEC      watchdog threshold          (default 120)\n"
"  -k N        victim processes, 0 = off   (default 6)\n"
"  -K MIN:MAX  victim kill interval in ms  (default 200:2000)\n"
"  -i SEC      progress interval, 0 = off  (default 10)\n"
"  -C          keep the test tree on exit\n"
"  -v          verbose (latency histogram)\n");
	exit(3);
}

int main(int argc, char **argv)
{
	pthread_t watchdog, killer;
	struct timespec deadline;
	struct sigaction sa;
	int i, c, hung = 0, bad = 0;
	unsigned long last_ops = 0;
	uint64_t last_report;
	ssize_t n;

	build_op_table();

	if (argc >= 4 && !strcmp(argv[1], "--victim"))
		return victim_main(argv[2],
				   (unsigned int)strtoul(argv[3], NULL, 0),
				   argc >= 5 ? atoi(argv[4]) : 0);

	while ((c = getopt(argc, argv, "d:t:s:w:k:K:i:Cvh")) != -1) {
		switch (c) {
		case 'd':
			snprintf(g_root, sizeof(g_root), "%s", optarg);
			break;
		case 't':
			g_nworkers = atoi(optarg);
			break;
		case 's':
			g_seconds = atoi(optarg);
			break;
		case 'w':
			g_watchdog = atoi(optarg);
			break;
		case 'k':
			g_nvictims = atoi(optarg);
			break;
		case 'K':
			if (sscanf(optarg, "%d:%d", &g_kill_min_ms,
				   &g_kill_max_ms) != 2)
				usage();
			break;
		case 'i':
			g_report = atoi(optarg);
			break;
		case 'C':
			g_keep = true;
			break;
		case 'v':
			g_verbose = true;
			break;
		default:
			usage();
		}
	}

	if (!g_root[0])
		usage();
	if (g_nworkers < 1 || g_nworkers > MAX_WORKERS)
		die("threads must be 1..%d", MAX_WORKERS);
	if (g_nvictims < 0 || g_nvictims > MAX_VICTIMS)
		die("victims must be 0..%d", MAX_VICTIMS);

	n = readlink("/proc/self/exe", g_self, sizeof(g_self) - 1);
	if (n < 0)
		die("readlink /proc/self/exe: %s", strerror(errno));
	g_self[n] = '\0';

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = on_stop;
	sigaction(SIGTERM, &sa, NULL);
	sigaction(SIGINT, &sa, NULL);
	sa.sa_handler = on_dump;
	sigaction(SIGUSR1, &sa, NULL);
	signal(SIGPIPE, SIG_IGN);

	mkdir_p(g_root);
	for (i = 0; i < g_nworkers; i++) {
		struct worker *w = &g_worker[i];

		w->idx = i;
		w->seed = (unsigned int)(now_ns() >> 3) + i * 2654435761u;
		snprintf(w->dir, sizeof(w->dir), "%s/w%02d", g_root, i);
		mkdir_p(w->dir);
	}

	printf("mdsc_stress: root=%s workers=%d victims=%d duration=%ds "
	       "watchdog=%ds\n", g_root, g_nworkers, g_nvictims, g_seconds,
	       g_watchdog);
	fflush(stdout);

	g_start_ns = now_ns();
	last_report = g_start_ns;

	for (i = 0; i < g_nvictims; i++)
		g_victim[i] = spawn_victim(i);

	for (i = 0; i < g_nworkers; i++) {
		if (pthread_create(&g_worker[i].tid, NULL, worker_fn,
				   &g_worker[i]))
			die("pthread_create: %s", strerror(errno));
	}
	if (pthread_create(&watchdog, NULL, watchdog_fn, NULL))
		die("pthread_create watchdog: %s", strerror(errno));
	if (g_nvictims && pthread_create(&killer, NULL, killer_fn, NULL))
		die("pthread_create killer: %s", strerror(errno));

	while (!g_stop) {
		uint64_t now;

		msleep(200);
		now = now_ns();

		if (g_seconds > 0 &&
		    now - g_start_ns >= (uint64_t)g_seconds * 1000000000ull)
			g_stop = 1;

		if (g_report > 0 &&
		    now - last_report >= (uint64_t)g_report * 1000000000ull) {
			unsigned long ops = atomic_load(&g_total_ops);

			printf("[%6.0fs] ops=%lu (+%lu, %.0f op/s) slow=%d "
			       "kills=%d\n",
			       (double)(now - g_start_ns) / 1e9, ops,
			       ops - last_ops,
			       (double)(ops - last_ops) /
			       ((double)(now - last_report) / 1e9),
			       atomic_load(&g_slow_events),
			       atomic_load(&g_victim_kills));
			fflush(stdout);
			last_ops = ops;
			last_report = now;
		}
	}

	g_stop = 1;

	if (g_nvictims)
		pthread_join(killer, NULL);
	pthread_join(watchdog, NULL);

	/*
	 * The definitive lost-wakeup oracle: after the run has been told
	 * to stop, every worker must come back.  A worker that is still
	 * inside an MDS request 60s later is either waiting for a reply
	 * that will never arrive or was never re-dispatched.
	 */
	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += 60;

	for (i = 0; i < g_nworkers; i++) {
		unsigned long seq;
		uint64_t t0;
		int op;

		if (pthread_timedjoin_np(g_worker[i].tid, NULL,
					 &deadline) == 0) {
			g_worker[i].joined = true;
			continue;
		}

		hung++;
		if (read_slot(&g_slot[i], &seq, &t0, &op) != SLOT_BUSY)
			printf("HUNG: worker=%d (slot %s)\n", i,
			       seq & 1 ? "mid-publication" : "idle");
		else
			printf("HUNG: worker=%d op=%s elapsed=%.1fs path=%s\n",
			       i, op_name[op],
			       (double)(now_ns() - t0) / 1e9,
			       g_slot[i].path);
		fflush(stdout);
	}

	for (i = 0; i < g_nworkers; i++) {
		int op;

		for (op = 0; op < OP_NR; op++)
			bad += (int)g_worker[i].errs[op][ERR_BAD];
	}

	report(hung, bad);

	if (!g_keep && !hung)
		rm_rf(g_root);

	if (hung)
		return 1;
	if (bad)
		return 2;
	return 0;
}
