/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
#include "spdk/rpc.h"
#include "spdk/nvmf.h"
#include "spdk/likely.h"

#include "spdk_internal/event.h"

#define NVMF_DEFAULT_SUBSYSTEMS		32
#define NVMF_DEFAULT_TIMEOUT_NS		1000000000ULL  /* 1s */
#define ACCEPT_TIMEOUT_US		10000 /* 10ms */

static const char *g_rpc_addr = SPDK_DEFAULT_RPC_ADDR;
static uint32_t g_acceptor_poll_rate = ACCEPT_TIMEOUT_US;

enum nvmf_target_state {
	NVMF_INIT_SUBSYSTEM = 0,
	NVMF_INIT_TARGET,
	NVMF_INIT_POLL_GROUPS,
	NVMF_INIT_START_SUBSYSTEMS,
	NVMF_INIT_START_ACCEPTOR,
	NVMF_RUNNING,
	NVMF_FINI_STOP_SUBSYSTEMS,
	NVMF_FINI_POLL_GROUPS,
	NVMF_FINI_STOP_ACCEPTOR,
	NVMF_FINI_TARGET,
	NVMF_FINI_SUBSYSTEM,
};

struct nvmf_lw_thread {
	TAILQ_ENTRY(nvmf_lw_thread) link; /* Used by reactor run */
	bool resched;
};

struct nvmf_reactor {
	uint32_t core;

	struct spdk_ring		*threads;
	TAILQ_ENTRY(nvmf_reactor)	link;
};

struct nvmf_target_poll_group {
	struct spdk_nvmf_poll_group		*group;
	struct spdk_thread			*thread;

	TAILQ_ENTRY(nvmf_target_poll_group)	link;
};

struct nvmf_target {
	struct spdk_nvmf_tgt	*tgt;

	int max_subsystems;
};

TAILQ_HEAD(, nvmf_reactor) g_reactors = TAILQ_HEAD_INITIALIZER(g_reactors);
TAILQ_HEAD(, nvmf_target_poll_group) g_poll_groups = TAILQ_HEAD_INITIALIZER(g_poll_groups);
static uint32_t g_num_poll_groups = 0;

static struct nvmf_reactor *g_master_reactor = NULL;
static struct nvmf_reactor *g_next_reactor = NULL;
static struct spdk_thread *g_init_thread = NULL;
static struct spdk_thread *g_fini_thread = NULL;
static struct nvmf_target g_nvmf_tgt = {
	.max_subsystems = NVMF_DEFAULT_SUBSYSTEMS,
};
static struct spdk_poller *g_acceptor_poller = NULL;
static struct nvmf_target_poll_group *g_next_pg = NULL;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool g_reactors_exit = false;
static enum nvmf_target_state g_target_state;
static bool g_intr_received = false;
static bool g_dont_poll;
static sem_t g_sem;

static uint32_t g_migrate_pg_period_us = 0;
static struct spdk_poller *g_migrate_pg_poller = NULL;

static void nvmf_target_advance_state(void);
static int nvmf_schedule_spdk_thread(struct spdk_thread *thread);

static void
usage(char *program_name)
{
	printf("%s options", program_name);
	printf("\n");
	printf("\t[-g period of round robin poll group migration (us) (default: 0 (disabled))]\n");
	printf("\t[-h show this usage]\n");
	printf("\t[-i shared memory ID (optional)]\n");
	printf("\t[-m core mask for DPDK]\n");
	printf("\t[-n max subsystems for target(default: 32)]\n");
	printf("\t[-p acceptor poller rate in us for target(default: 10000us)]\n");
	printf("\t[-r RPC listen address (default /var/tmp/spdk.sock)]\n");
	printf("\t[-s memory size in MB for DPDK (default: 0MB)]\n");
	printf("\t[-u disable PCI access]\n");
	printf("\t[-B don't burn CPU, calculate timeout for each reactor and sleep]\n");
}

static int
parse_args(int argc, char **argv, struct spdk_env_opts *opts)
{
	int op;
	long int value;

	while ((op = getopt(argc, argv, "g:i:m:n:p:r:s:u:hB")) != -1) {
		switch (op) {
		case 'g':
			value = spdk_strtol(optarg, 10);
			if (value < 0) {
				fprintf(stderr, "converting a string to integer failed\n");
				return -EINVAL;
			}
			g_migrate_pg_period_us = value;
			break;
		case 'i':
			value = spdk_strtol(optarg, 10);
			if (value < 0) {
				fprintf(stderr, "converting a string to integer failed\n");
				return -EINVAL;
			}
			opts->shm_id = value;
			break;
		case 'm':
			opts->core_mask = optarg;
			break;
		case 'n':
			g_nvmf_tgt.max_subsystems = spdk_strtol(optarg, 10);
			if (g_nvmf_tgt.max_subsystems < 0) {
				fprintf(stderr, "converting a string to integer failed\n");
				return -EINVAL;
			}
			break;
		case 'p':
			value = spdk_strtol(optarg, 10);
			if (value < 0) {
				fprintf(stderr, "converting a string to integer failed\n");
				return -EINVAL;
			}
			g_acceptor_poll_rate = value;
			break;
		case 'r':
			g_rpc_addr = optarg;
			break;
		case 's':
			value = spdk_strtol(optarg, 10);
			if (value < 0) {
				fprintf(stderr, "converting a string to integer failed\n");
				return -EINVAL;
			}
			opts->mem_size = value;
			break;
		case 'u':
			opts->no_pci = true;
			break;
		case 'B':
			g_dont_poll = true;
			break;
		case 'h':
		default:
			usage(argv[0]);
			return 1;
		}
	}

	return 0;
}

static uint64_t
nvmf_calc_timeout(struct spdk_thread *thread, uint64_t timeout)
{
	uint64_t thr_timeout;

	if (spdk_thread_has_active_pollers(thread)) {
		/* Active pollers demand immediate execution */
		return 0;
	}

	thr_timeout = spdk_thread_next_poller_expiration(thread);
	if (!thr_timeout) {
		/* If no timed pollers */
		return timeout;
	}

	return spdk_min(timeout, thr_timeout);
}

static void
nvmf_timeout_to_ts(uint64_t timeout, struct timespec *ts)
{
	uint64_t now;

	clock_gettime(CLOCK_REALTIME, ts);
	now = spdk_get_ticks();
	if (timeout > now) {
		timeout = ((timeout - now) * SPDK_SEC_TO_NSEC) / spdk_get_ticks_hz() +
			  ts->tv_sec * SPDK_SEC_TO_NSEC + ts->tv_nsec;

		ts->tv_sec  = timeout / SPDK_SEC_TO_NSEC;
		ts->tv_nsec = timeout % SPDK_SEC_TO_NSEC;
	}
}

static int
nvmf_reactor_run(void *arg)
{
	struct nvmf_reactor *nvmf_reactor = arg;
	struct nvmf_lw_thread *lw_thread, *tmp;
	struct spdk_thread *thread;

	/* run all the lightweight threads in this nvmf_reactor by FIFO. */
	do {
		TAILQ_HEAD(, nvmf_lw_thread) threads =
			TAILQ_HEAD_INITIALIZER(threads);
		uint64_t timeout = 0;

		if (g_dont_poll) {
			timeout = spdk_get_ticks();
			timeout += (NVMF_DEFAULT_TIMEOUT_NS *
				    spdk_get_ticks_hz()) / SPDK_SEC_TO_NSEC;
		}

		/* First dequeue everything to the shadow queue */
		while (spdk_ring_dequeue(nvmf_reactor->threads,
					 (void **)&lw_thread, 1)) {
			TAILQ_INSERT_TAIL(&threads, lw_thread, link);
		}

		/* Process shadow queue */
		TAILQ_FOREACH_SAFE(lw_thread, &threads, link, tmp) {
			TAILQ_REMOVE(&threads, lw_thread, link);
			thread = spdk_thread_get_from_ctx(lw_thread);

			spdk_thread_poll(thread, 0, 0);
			if (g_dont_poll) {
				timeout = nvmf_calc_timeout(thread, timeout);
			}
			if (spdk_unlikely(spdk_thread_is_exited(thread) &&
					  spdk_thread_is_idle(thread))) {
				spdk_thread_destroy(thread);
			} else if (spdk_unlikely(lw_thread->resched)) {
				lw_thread->resched = false;
				nvmf_schedule_spdk_thread(thread);
			} else {
				spdk_ring_enqueue(nvmf_reactor->threads,
						  (void **)&lw_thread,
						  1, NULL);
			}
		}
		if (g_dont_poll && !g_reactors_exit) {
			struct timespec ts;

			nvmf_timeout_to_ts(timeout, &ts);
			sem_timedwait(&g_sem, &ts);
		}
	} while (!g_reactors_exit);

	/* free all the lightweight threads */
	while (spdk_ring_dequeue(nvmf_reactor->threads, (void **)&lw_thread, 1)) {
		thread = spdk_thread_get_from_ctx(lw_thread);
		spdk_set_thread(thread);

		if (spdk_thread_is_exited(thread)) {
			spdk_thread_destroy(thread);
		} else {
			/* This thread is not exited yet, and may need to communicate with other threads
			 * to be exited. So mark it as exiting, and check again after traversing other threads.
			 */
			spdk_thread_exit(thread);
			spdk_thread_poll(thread, 0, 0);
			spdk_ring_enqueue(nvmf_reactor->threads, (void **)&lw_thread, 1, NULL);
		}
	}

	return 0;
}

static int
nvmf_schedule_spdk_thread(struct spdk_thread *thread)
{
	struct nvmf_reactor *nvmf_reactor;
	struct nvmf_lw_thread *lw_thread;
	struct spdk_cpuset *cpumask;
	uint32_t i;

	/* Lightweight threads may have a requested cpumask.
	 * This is a request only - the scheduler does not have to honor it.
	 * For this scheduler implementation, each reactor is pinned to
	 * a particular core so honoring the request is reasonably easy.
	 */
	cpumask = spdk_thread_get_cpumask(thread);

	lw_thread = spdk_thread_get_ctx(thread);
	assert(lw_thread != NULL);
	memset(lw_thread, 0, sizeof(*lw_thread));

	/* assign lightweight threads to nvmf reactor(core)
	 * Here we use the mutex.The way the actual SPDK event framework
	 * solves this is by using internal rings for messages between reactors
	 */
	pthread_mutex_lock(&g_mutex);
	for (i = 0; i < spdk_env_get_core_count(); i++) {
		if (g_next_reactor == NULL) {
			g_next_reactor = TAILQ_FIRST(&g_reactors);
		}
		nvmf_reactor = g_next_reactor;
		g_next_reactor = TAILQ_NEXT(g_next_reactor, link);

		/* each spdk_thread has the core affinity */
		if (spdk_cpuset_get_cpu(cpumask, nvmf_reactor->core)) {
			spdk_ring_enqueue(nvmf_reactor->threads, (void **)&lw_thread, 1, NULL);
			break;
		}
	}
	pthread_mutex_unlock(&g_mutex);

	if (i == spdk_env_get_core_count()) {
		fprintf(stderr, "failed to schedule spdk thread\n");
		return -1;
	}
	return 0;
}

static void
nvmf_request_spdk_thread_reschedule(struct spdk_thread *thread)
{
	struct nvmf_lw_thread *lw_thread;

	assert(thread == spdk_get_thread());

	lw_thread = spdk_thread_get_ctx(thread);

	assert(lw_thread != NULL);

	lw_thread->resched = true;
}

static int
nvmf_reactor_thread_op(struct spdk_thread *thread, enum spdk_thread_op op)
{
	switch (op) {
	case SPDK_THREAD_OP_NEW:
		return nvmf_schedule_spdk_thread(thread);
	case SPDK_THREAD_OP_RESCHED:
		nvmf_request_spdk_thread_reschedule(thread);
		return 0;
	default:
		return -ENOTSUP;
	}
}

static bool
nvmf_reactor_thread_op_supported(enum spdk_thread_op op)
{
	switch (op) {
	case SPDK_THREAD_OP_NEW:
	case SPDK_THREAD_OP_RESCHED:
		return true;
	default:
		return false;
	}
}

static int
nvmf_init_threads(void)
{
	int rc;
	uint32_t i;
	char thread_name[32];
	struct nvmf_reactor *nvmf_reactor;
	struct spdk_cpuset cpumask;
	uint32_t master_core = spdk_env_get_current_core();

	/* Whenever SPDK creates a new lightweight thread it will call
	 * nvmf_schedule_spdk_thread asking for the application to begin
	 * polling it via spdk_thread_poll(). Each lightweight thread in
	 * SPDK optionally allocates extra memory to be used by the application
	 * framework. The size of the extra memory allocated is the second parameter.
	 */
	spdk_thread_lib_init_ext(nvmf_reactor_thread_op, nvmf_reactor_thread_op_supported,
				 sizeof(struct nvmf_lw_thread));

	/* Spawn one system thread per CPU core. The system thread is called a reactor.
	 * SPDK will spawn lightweight threads that must be mapped to reactors in
	 * nvmf_schedule_spdk_thread. Using a single system thread per CPU core is a
	 * choice unique to this application. SPDK itself does not require this specific
	 * threading model. For example, another viable threading model would be
	 * dynamically scheduling the lightweight threads onto a thread pool using a
	 * work queue.
	 */
	SPDK_ENV_FOREACH_CORE(i) {
		nvmf_reactor = calloc(1, sizeof(struct nvmf_reactor));
		if (!nvmf_reactor) {
			fprintf(stderr, "failed to alloc nvmf reactor\n");
			rc = -ENOMEM;
			goto err_exit;
		}

		nvmf_reactor->core = i;

		nvmf_reactor->threads = spdk_ring_create(SPDK_RING_TYPE_MP_SC, 1024, SPDK_ENV_SOCKET_ID_ANY);
		if (!nvmf_reactor->threads) {
			fprintf(stderr, "failed to alloc ring\n");
			free(nvmf_reactor);
			rc = -ENOMEM;
			goto err_exit;
		}

		TAILQ_INSERT_TAIL(&g_reactors, nvmf_reactor, link);

		if (i == master_core) {
			g_master_reactor = nvmf_reactor;
			g_next_reactor = g_master_reactor;
		} else {
			rc = spdk_env_thread_launch_pinned(i,
							   nvmf_reactor_run,
							   nvmf_reactor);
			if (rc) {
				fprintf(stderr, "failed to pin reactor launch\n");
				goto err_exit;
			}
		}
	}

	/* Spawn a lightweight thread only on the current core to manage this application. */
	spdk_cpuset_zero(&cpumask);
	spdk_cpuset_set_cpu(&cpumask, master_core, true);
	snprintf(thread_name, sizeof(thread_name), "nvmf_master_thread");
	g_init_thread = spdk_thread_create(thread_name, &cpumask);
	if (!g_init_thread) {
		fprintf(stderr, "failed to create spdk thread\n");
		return -1;
	}

	fprintf(stdout, "nvmf threads initlize successfully\n");
	return 0;

err_exit:
	return rc;
}

static void
nvmf_destroy_threads(void)
{
	struct nvmf_reactor *nvmf_reactor, *tmp;

	TAILQ_FOREACH_SAFE(nvmf_reactor, &g_reactors, link, tmp) {
		spdk_ring_free(nvmf_reactor->threads);
		free(nvmf_reactor);
	}

	pthread_mutex_destroy(&g_mutex);
	spdk_thread_lib_fini();
	fprintf(stdout, "nvmf threads destroy successfully\n");
}

static void
nvmf_tgt_destroy_done(void *ctx, int status)
{
	fprintf(stdout, "destroyed the nvmf target service\n");

	g_target_state = NVMF_FINI_SUBSYSTEM;
	nvmf_target_advance_state();
}

static void
nvmf_destroy_nvmf_tgt(void)
{
	if (g_nvmf_tgt.tgt) {
		spdk_nvmf_tgt_destroy(g_nvmf_tgt.tgt, nvmf_tgt_destroy_done, NULL);
	} else {
		g_target_state = NVMF_FINI_SUBSYSTEM;
	}
}

static void
nvmf_create_nvmf_tgt(void)
{
	struct spdk_nvmf_subsystem *subsystem;
	struct spdk_nvmf_target_opts tgt_opts;

	tgt_opts.max_subsystems = g_nvmf_tgt.max_subsystems;
	snprintf(tgt_opts.name, sizeof(tgt_opts.name), "%s", "rbd-nvmf-gw");
	/* Construct the default NVMe-oF target
	 * An NVMe-oF target is a collection of subsystems, namespace, and poll
	 * groups, and defines the scope of the NVMe-oF discovery service.
	 */
	g_nvmf_tgt.tgt = spdk_nvmf_tgt_create(&tgt_opts);
	if (g_nvmf_tgt.tgt == NULL) {
		fprintf(stderr, "spdk_nvmf_tgt_create() failed\n");
		goto error;
	}

	/* Create and add discovery subsystem to the NVMe-oF target.
	 * NVMe-oF defines a discovery mechanism that a host uses to determine
	 * the NVM subsystems that expose namespaces that the host may access.
	 * It provides a host with following capabilities:
	 *	1,The ability to discover a list of NVM subsystems with namespaces
	 *	  that are accessible to the host.
	 *	2,The ability to discover multiple paths to an NVM subsystem.
	 *	3,The ability to discover controllers that are statically configured.
	 */
	subsystem = spdk_nvmf_subsystem_create(g_nvmf_tgt.tgt, SPDK_NVMF_DISCOVERY_NQN,
					       SPDK_NVMF_SUBTYPE_DISCOVERY, 0);
	if (subsystem == NULL) {
		fprintf(stderr, "failed to create discovery nvmf library subsystem\n");
		goto error;
	}

	/* Allow any host to access the discovery subsystem */
	spdk_nvmf_subsystem_set_allow_any_host(subsystem, true);

	fprintf(stdout, "created a nvmf target service\n");

	g_target_state = NVMF_INIT_POLL_GROUPS;
	return;

error:
	g_target_state = NVMF_FINI_TARGET;
}

static void
nvmf_tgt_subsystem_stop_next(struct spdk_nvmf_subsystem *subsystem,
			     void *cb_arg, int status)
{
	subsystem = spdk_nvmf_subsystem_get_next(subsystem);
	if (subsystem) {
		spdk_nvmf_subsystem_stop(subsystem,
					 nvmf_tgt_subsystem_stop_next,
					 cb_arg);
		return;
	}

	fprintf(stdout, "all subsystems of target stopped\n");

	g_target_state = NVMF_FINI_POLL_GROUPS;
	nvmf_target_advance_state();

	/* Switch to CPU burn for fast exit */
	if (g_dont_poll) {
		unsigned int i;

		g_dont_poll = false;
		SPDK_ENV_FOREACH_CORE(i) {
			/* Signal for all threads */
			sem_post(&g_sem);
		}
	}
}

static void
nvmf_tgt_stop_subsystems(struct nvmf_target *nvmf_tgt)
{
	struct spdk_nvmf_subsystem *subsystem;

	subsystem = spdk_nvmf_subsystem_get_first(nvmf_tgt->tgt);
	if (spdk_likely(subsystem)) {
		spdk_nvmf_subsystem_stop(subsystem,
					 nvmf_tgt_subsystem_stop_next,
					 NULL);
	} else {
		g_target_state = NVMF_FINI_POLL_GROUPS;
	}
}

static int
nvmf_tgt_acceptor_poll(void *arg)
{
	struct nvmf_target *nvmf_tgt = arg;

	spdk_nvmf_tgt_accept(nvmf_tgt->tgt);

	return -1;
}

static void
nvmf_tgt_subsystem_start_next(struct spdk_nvmf_subsystem *subsystem,
			      void *cb_arg, int status)
{
	subsystem = spdk_nvmf_subsystem_get_next(subsystem);
	if (subsystem) {
		spdk_nvmf_subsystem_start(subsystem, nvmf_tgt_subsystem_start_next,
					  cb_arg);
		return;
	}

	fprintf(stdout, "all subsystems of target started\n");

	g_target_state = NVMF_INIT_START_ACCEPTOR;
	nvmf_target_advance_state();
}

static void
nvmf_tgt_start_subsystems(struct nvmf_target *nvmf_tgt)
{
	struct spdk_nvmf_subsystem *subsystem;

	/* Subsystem is the NVM subsystem which is a combine of namespaces
	 * except the discovery subsystem which is used for discovery service.
	 * It also controls the hosts that means the subsystem determines whether
	 * the host can access this subsystem.
	 */
	subsystem = spdk_nvmf_subsystem_get_first(nvmf_tgt->tgt);
	if (spdk_likely(subsystem)) {
		/* In SPDK there are three states in subsystem: Inactive, Active, Paused.
		 * Start subsystem means make it from inactive to active that means
		 * subsystem start to work or it can be accessed.
		 */
		spdk_nvmf_subsystem_start(subsystem,
					  nvmf_tgt_subsystem_start_next,
					  NULL);
	} else {
		g_target_state = NVMF_INIT_START_ACCEPTOR;
	}
}

static void
nvmf_tgt_create_poll_groups_done(void *ctx)
{
	struct nvmf_target_poll_group *pg = ctx;

	if (!g_next_pg) {
		g_next_pg = pg;
	}

	TAILQ_INSERT_TAIL(&g_poll_groups, pg, link);

	assert(g_num_poll_groups < spdk_env_get_core_count());

	if (++g_num_poll_groups == spdk_env_get_core_count()) {
		fprintf(stdout, "create targets's poll groups done\n");

		g_target_state = NVMF_INIT_START_SUBSYSTEMS;
		nvmf_target_advance_state();
	}
}

static void
nvmf_tgt_create_poll_group(void *ctx)
{
	struct nvmf_target_poll_group *pg;

	pg = calloc(1, sizeof(struct nvmf_target_poll_group));
	if (!pg) {
		fprintf(stderr, "failed to allocate poll group\n");
		assert(false);
		return;
	}

	pg->thread = spdk_get_thread();
	pg->group = spdk_nvmf_poll_group_create(g_nvmf_tgt.tgt);
	if (!pg->group) {
		fprintf(stderr, "failed to create poll group of the target\n");
		free(pg);
		assert(false);
		return;
	}

	spdk_thread_send_msg(g_init_thread, nvmf_tgt_create_poll_groups_done, pg);
}

/* Create a lightweight thread per poll group instead of assuming a pool of lightweight
 * threads already exist at start up time. A poll group is a collection of unrelated NVMe-oF
 * connections. Each poll group is only accessed from the associated lightweight thread.
 */
static void
nvmf_poll_groups_create(void)
{
	struct spdk_cpuset tmp_cpumask = {};
	uint32_t i;
	char thread_name[32];
	struct spdk_thread *thread;

	assert(g_init_thread != NULL);

	SPDK_ENV_FOREACH_CORE(i) {
		spdk_cpuset_zero(&tmp_cpumask);
		spdk_cpuset_set_cpu(&tmp_cpumask, i, true);
		snprintf(thread_name, sizeof(thread_name), "nvmf_tgt_poll_group_%u", i);

		thread = spdk_thread_create(thread_name, &tmp_cpumask);
		assert(thread != NULL);

		spdk_thread_send_msg(thread, nvmf_tgt_create_poll_group, NULL);
	}
}

static void
_nvmf_tgt_destroy_poll_groups_done(void *ctx)
{
	assert(g_num_poll_groups > 0);

	if (--g_num_poll_groups == 0) {
		fprintf(stdout, "destroy targets's poll groups done\n");

		g_target_state = NVMF_FINI_STOP_ACCEPTOR;
		nvmf_target_advance_state();
	}
}

static void
nvmf_tgt_destroy_poll_groups_done(void *cb_arg, int status)
{
	struct nvmf_target_poll_group *pg = cb_arg;

	free(pg);

	spdk_thread_send_msg(g_fini_thread, _nvmf_tgt_destroy_poll_groups_done, NULL);

	spdk_thread_exit(spdk_get_thread());
}

static void
nvmf_tgt_destroy_poll_group(void *ctx)
{
	struct nvmf_target_poll_group *pg = ctx;

	spdk_nvmf_poll_group_destroy(pg->group, nvmf_tgt_destroy_poll_groups_done, pg);
}

static void
nvmf_poll_groups_destroy(void)
{
	struct nvmf_target_poll_group *pg, *tmp;

	g_fini_thread = spdk_get_thread();
	assert(g_fini_thread != NULL);

	TAILQ_FOREACH_SAFE(pg, &g_poll_groups, link, tmp) {
		TAILQ_REMOVE(&g_poll_groups, pg, link);
		spdk_thread_send_msg(pg->thread, nvmf_tgt_destroy_poll_group, pg);
	}
}

static void
nvmf_subsystem_fini_done(void *cb_arg)
{
	fprintf(stdout, "bdev subsystem finish successfully\n");
	spdk_rpc_finish();
	g_reactors_exit = true;
}

static void
nvmf_subsystem_init_done(int rc, void *cb_arg)
{
	fprintf(stdout, "bdev subsystem init successfully\n");
	spdk_rpc_initialize(g_rpc_addr);
	spdk_rpc_set_state(SPDK_RPC_RUNTIME);

	g_target_state = NVMF_INIT_TARGET;
	nvmf_target_advance_state();
}

static void
migrate_poll_group_by_rr(void *ctx)
{
	uint32_t current_core, next_core;
	struct spdk_cpuset cpumask = {};

	current_core = spdk_env_get_current_core();
	next_core = spdk_env_get_next_core(current_core);
	if (next_core == UINT32_MAX) {
		next_core = spdk_env_get_first_core();
	}

	spdk_cpuset_set_cpu(&cpumask, next_core, true);

	spdk_thread_set_cpumask(&cpumask);
}

static int
migrate_poll_groups_by_rr(void *ctx)
{
	struct nvmf_target_poll_group *pg;

	TAILQ_FOREACH(pg, &g_poll_groups, link) {
		spdk_thread_send_msg(pg->thread, migrate_poll_group_by_rr, NULL);
	}

	return 1;
}

static void
nvmf_target_advance_state(void)
{
	enum nvmf_target_state prev_state;

	do {
		prev_state = g_target_state;

		switch (g_target_state) {
		case NVMF_INIT_SUBSYSTEM:
			/* initlize the bdev layer */
			spdk_subsystem_init(nvmf_subsystem_init_done, NULL);
			return;
		case NVMF_INIT_TARGET:
			nvmf_create_nvmf_tgt();
			break;
		case NVMF_INIT_POLL_GROUPS:
			nvmf_poll_groups_create();
			break;
		case NVMF_INIT_START_SUBSYSTEMS:
			nvmf_tgt_start_subsystems(&g_nvmf_tgt);
			break;
		case NVMF_INIT_START_ACCEPTOR:
			g_acceptor_poller = SPDK_POLLER_REGISTER(nvmf_tgt_acceptor_poll, &g_nvmf_tgt,
					    g_acceptor_poll_rate);
			fprintf(stdout, "Acceptor running\n");
			g_target_state = NVMF_RUNNING;
			break;
		case NVMF_RUNNING:
			fprintf(stdout, "nvmf target is running\n");
			if (g_migrate_pg_period_us != 0) {
				g_migrate_pg_poller = SPDK_POLLER_REGISTER(migrate_poll_groups_by_rr, NULL,
						      g_migrate_pg_period_us);
			}
			break;
		case NVMF_FINI_STOP_SUBSYSTEMS:
			spdk_poller_unregister(&g_migrate_pg_poller);
			nvmf_tgt_stop_subsystems(&g_nvmf_tgt);
			break;
		case NVMF_FINI_POLL_GROUPS:
			nvmf_poll_groups_destroy();
			break;
		case NVMF_FINI_STOP_ACCEPTOR:
			spdk_poller_unregister(&g_acceptor_poller);
			g_target_state = NVMF_FINI_TARGET;
			break;
		case NVMF_FINI_TARGET:
			nvmf_destroy_nvmf_tgt();
			break;
		case NVMF_FINI_SUBSYSTEM:
			spdk_subsystem_fini(nvmf_subsystem_fini_done, NULL);
			break;
		}
	} while (g_target_state != prev_state);
}

static void
nvmf_target_app_start(void *arg)
{
	g_target_state = NVMF_INIT_SUBSYSTEM;
	nvmf_target_advance_state();
}

static void
_nvmf_shutdown_cb(void *ctx)
{
	/* Still in initialization state, defer shutdown operation */
	if (g_target_state < NVMF_RUNNING) {
		spdk_thread_send_msg(spdk_get_thread(), _nvmf_shutdown_cb, NULL);
		return;
	} else if (g_target_state > NVMF_RUNNING) {
		/* Already in Shutdown status, ignore the signal */
		return;
	}

	g_target_state = NVMF_FINI_STOP_SUBSYSTEMS;
	nvmf_target_advance_state();
}

static void
nvmf_shutdown_cb(int signo)
{
	if (!g_intr_received) {
		g_intr_received = true;
		spdk_thread_send_msg(g_init_thread, _nvmf_shutdown_cb, NULL);
	}
}

static int
nvmf_setup_signal_handlers(void)
{
	struct sigaction	sigact;
	sigset_t		sigmask;
	int			signals[] = {SIGINT, SIGTERM};
	int			num_signals = sizeof(signals) / sizeof(int);
	int			rc, i;

	rc = sigemptyset(&sigmask);
	if (rc) {
		fprintf(stderr, "errno:%d--failed to empty signal set\n", errno);
		return rc;
	}
	memset(&sigact, 0, sizeof(sigact));
	rc = sigemptyset(&sigact.sa_mask);
	if (rc) {
		fprintf(stderr, "errno:%d--failed to empty signal set\n", errno);
		return rc;
	}

	/* Install the same handler for SIGINT and SIGTERM */
	sigact.sa_handler = nvmf_shutdown_cb;

	for (i = 0; i < num_signals; i++) {
		rc = sigaction(signals[i], &sigact, NULL);
		if (rc < 0) {
			fprintf(stderr, "errno:%d--sigaction() failed\n", errno);
			return rc;
		}
		rc = sigaddset(&sigmask, signals[i]);
		if (rc) {
			fprintf(stderr, "errno:%d--failed to add set\n", errno);
			return rc;
		}
	}

	pthread_sigmask(SIG_UNBLOCK, &sigmask, NULL);

	return 0;
}

int main(int argc, char **argv)
{
	int rc;
	struct spdk_env_opts opts;

	spdk_env_opts_init(&opts);
	opts.name = "rbd-nvmf-gw";

	rc = parse_args(argc, argv, &opts);
	if (rc != 0) {
		return rc;
	}

	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "unable to initialize SPDK env\n");
		return -EINVAL;
	}

	/* Initialize the threads */
	rc = nvmf_init_threads();
	assert(rc == 0);

	/* Send a message to the thread assigned to the master reactor
	 * that continues initialization. This is how we bootstrap the
	 * program so that all code from here on is running on an SPDK thread.
	 */
	assert(g_init_thread != NULL);

	rc = nvmf_setup_signal_handlers();
	assert(rc == 0);

	rc = sem_init(&g_sem, 0, 0);
	assert(rc == 0);

	spdk_thread_send_msg(g_init_thread, nvmf_target_app_start, NULL);

	nvmf_reactor_run(g_master_reactor);

	spdk_env_thread_wait_all();
	nvmf_destroy_threads();
	return rc;
}
