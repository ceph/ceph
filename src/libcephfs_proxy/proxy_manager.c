
#include <signal.h>

#include "proxy_manager.h"
#include "proxy_helpers.h"
#include "proxy_list.h"
#include "proxy_log.h"

static void proxy_manager_signal_handler(int32_t signum, siginfo_t *info,
					 void *ctx)
{
}

static void proxy_worker_register(proxy_worker_t *worker)
{
	proxy_manager_t *manager;

	manager = worker->manager;

	proxy_mutex_lock(&manager->mutex);

	list_add_tail(&worker->list, &manager->workers);

	proxy_mutex_unlock(&manager->mutex);
}

static void proxy_worker_deregister(proxy_worker_t *worker)
{
	proxy_manager_t *manager;

	manager = worker->manager;

	proxy_mutex_lock(&manager->mutex);

	list_del_init(&worker->list);
	if (list_empty(&manager->workers)) {
		proxy_condition_signal(&manager->condition);
	}

	proxy_mutex_unlock(&manager->mutex);
}

static void proxy_worker_finished(proxy_worker_t *worker)
{
	proxy_manager_t *manager;

	manager = worker->manager;

	proxy_mutex_lock(&manager->mutex);

	if (list_empty(&manager->finished)) {
		proxy_condition_signal(&manager->condition);
	}

	list_move_tail(&worker->list, &manager->finished);

	proxy_mutex_unlock(&manager->mutex);
}

static void *proxy_worker_start(void *arg)
{
	proxy_worker_t *worker;

	worker = arg;

	worker->start(worker);

	proxy_worker_finished(worker);

	return NULL;
}

static void *proxy_manager_start(void *arg)
{
	proxy_manager_t *manager;
	proxy_worker_t *worker;

	manager = arg;

	proxy_mutex_lock(&manager->mutex);

	while (true) {
		while (!list_empty(&manager->finished)) {
			worker = list_first_entry(&manager->finished,
						  proxy_worker_t, list);
			list_del_init(&worker->list);

			proxy_mutex_unlock(&manager->mutex);

			proxy_thread_join(worker->tid);

			if (worker->destroy != NULL) {
				worker->destroy(worker);
			}

			proxy_mutex_lock(&manager->mutex);
		}

		if (manager->stop && list_empty(&manager->workers)) {
			break;
		}

		proxy_condition_wait(&manager->condition, &manager->mutex);
	}

	manager->done = true;
	proxy_condition_signal(&manager->condition);

	proxy_mutex_unlock(&manager->mutex);

	return NULL;
}

static int32_t proxy_manager_init(proxy_manager_t *manager)
{
	int32_t err;

	list_init(&manager->workers);
	list_init(&manager->finished);

	manager->stop = false;
	manager->done = false;

	manager->main_tid = pthread_self();

	err = proxy_mutex_init(&manager->mutex);
	if (err < 0) {
		return err;
	}

	err = proxy_condition_init(&manager->condition);
	if (err < 0) {
		pthread_mutex_destroy(&manager->mutex);
	}

	return err;
}

static void proxy_manager_destroy(proxy_manager_t *manager)
{
	pthread_cond_destroy(&manager->condition);
	pthread_mutex_destroy(&manager->mutex);
}

static int32_t proxy_manager_setup_signals(struct sigaction *old)
{
	struct sigaction action;

	/* The CONT signal will be used to wake threads blocked in I/O. */
	memset(&action, 0, sizeof(action));
	action.sa_flags = SA_SIGINFO;
	action.sa_sigaction = proxy_manager_signal_handler;

	return proxy_signal_set(SIGCONT, &action, old);
}

static void proxy_manager_restore_signals(struct sigaction *action)
{
	proxy_signal_set(SIGCONT, action, NULL);
}

static void proxy_manager_terminate(proxy_manager_t *manager)
{
	proxy_worker_t *worker;

	proxy_mutex_lock(&manager->mutex);

	list_for_each_entry(worker, &manager->workers, list) {
		worker->stop = true;
		proxy_thread_kill(worker->tid, SIGCONT);
	}

	while (!manager->done) {
		proxy_condition_wait(&manager->condition, &manager->mutex);
	}

	proxy_mutex_unlock(&manager->mutex);

	proxy_thread_join(manager->tid);
}

int32_t proxy_manager_run(proxy_manager_t *manager, proxy_manager_start_t start)
{
	struct sigaction old_action;
	int32_t err;

	err = proxy_manager_init(manager);
	if (err < 0) {
		return err;
	}

	err = proxy_manager_setup_signals(&old_action);
	if (err < 0) {
		goto done_destroy;
	}

	err = proxy_thread_create(&manager->tid, proxy_manager_start, manager);
	if (err < 0) {
		goto done_signal;
	}

	err = start(manager);

	proxy_manager_terminate(manager);

done_signal:
	proxy_manager_restore_signals(&old_action);

done_destroy:
	proxy_manager_destroy(manager);

	return err;
}

void proxy_manager_shutdown(proxy_manager_t *manager)
{
	proxy_mutex_lock(&manager->mutex);

	manager->stop = true;
	proxy_condition_signal(&manager->condition);

	proxy_mutex_unlock(&manager->mutex);

	/* Wake the thread if it was blocked in an I/O operation. */
	proxy_thread_kill(manager->main_tid, SIGCONT);
}

int32_t proxy_manager_launch(proxy_manager_t *manager, proxy_worker_t *worker,
			     proxy_worker_start_t start,
			     proxy_worker_destroy_t destroy)
{
	int32_t err;

	worker->manager = manager;
	worker->start = start;
	worker->destroy = destroy;
	worker->stop = false;

	proxy_worker_register(worker);

	err = proxy_thread_create(&worker->tid, proxy_worker_start, worker);
	if (err < 0) {
		proxy_worker_deregister(worker);
	}

	return err;
}
