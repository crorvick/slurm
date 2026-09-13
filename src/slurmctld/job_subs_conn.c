/*****************************************************************************\
 *  job_subs_conn.c - subscriber connection handling for job subscriptions
 *****************************************************************************
 *  Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include <pthread.h>

#include "src/common/list.h"
#include "src/common/macros.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/threadpool.h"
#include "src/common/xmalloc.h"

#include "src/interfaces/conn.h"

#include "src/slurmctld/job_subs.h"
#include "src/slurmctld/job_subs_conn.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/slurmctld.h"

/*
 * A stalled client that stops draining its socket accumulates events
 * here. Past this depth we declare it dead and close the connection:
 * per the design, a reconnecting client gets a fresh snapshot, so the
 * server never needs to preserve an unbounded backlog.
 */
#define MAX_QUEUED_EVENTS 100000

typedef struct {
	uint16_t msg_type;
	job_subs_event_msg_t *event;
} pending_event_t;

typedef struct {
	uint32_t query_id;
	conn_t *conn;		/* owned; closed by the delivery thread */
	uid_t uid;		/* subscriber; outbound sends restrict to it */
	uint16_t protocol_version;
	list_t *out;		/* pending_event_t queue */
	bool stop;		/* fini or replacement: exit quietly */
	bool failed;		/* send error or stall: mark disconnected */
} subs_conn_t;

/*
 * One mutex/cond pair covers the connection table and every queue.
 * Subscriber counts are expected to be small (a sidecar daemon, a
 * handful of tools), so contention is not a concern and the single
 * lock keeps enqueue-vs-teardown reasoning simple.
 */
static list_t *subs_conns = NULL;
static int writer_count = 0;
static pthread_mutex_t conn_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t conn_cond = PTHREAD_COND_INITIALIZER;

static void _free_pending(void *x)
{
	pending_event_t *pending = x;

	slurm_free_job_subs_event_msg(pending->event);
	xfree(pending);
}

static void _free_conn(subs_conn_t *sc)
{
	FREE_NULL_CONN(sc->conn);
	FREE_NULL_LIST(sc->out);
	xfree(sc);
}

static int _match_query(void *x, void *key)
{
	subs_conn_t *sc = x;

	return (sc->query_id == *(uint32_t *) key);
}

static int _match_ptr(void *x, void *key)
{
	return (x == key);
}

/*
 * Deliver one connection's queue until told to stop or the client goes
 * away. Sends happen without any lock held, so a slow client stalls
 * only its own thread, never the controller.
 */
static void *_writer(void *arg)
{
	subs_conn_t *sc = arg;
	slurmctld_lock_t job_write_lock = { .job = WRITE_LOCK };
	uint32_t query_id = sc->query_id;
	bool failed;

	slurm_mutex_lock(&conn_mutex);
	while (!sc->stop) {
		pending_event_t *pending = list_pop(sc->out);
		slurm_msg_t out;

		if (!pending) {
			slurm_cond_wait(&conn_cond, &conn_mutex);
			continue;
		}
		slurm_mutex_unlock(&conn_mutex);

		slurm_msg_t_init(&out);
		out.msg_type = pending->msg_type;
		out.protocol_version = sc->protocol_version;
		out.data = pending->event;
		slurm_msg_set_r_uid(&out, sc->uid);

		if (slurm_send_node_msg(sc->conn, &out) < 0) {
			debug("%s: query %u subscriber went away: %m",
			      __func__, query_id);
			sc->failed = true;
			sc->stop = true;
		}
		_free_pending(pending);

		slurm_mutex_lock(&conn_mutex);
	}
	failed = sc->failed;
	if (subs_conns)
		list_remove_first(subs_conns, _match_ptr, sc);
	_free_conn(sc);
	writer_count--;
	slurm_cond_broadcast(&conn_cond);	/* wake fini, if waiting */
	slurm_mutex_unlock(&conn_mutex);

	/*
	 * Start the reattach clock only for a genuine loss, not for a
	 * replacement or shutdown. A racing reattach can still make this
	 * marking stale; the pruning sweep cross-checks for a live
	 * connection before expiring a query.
	 */
	if (failed) {
		lock_slurmctld(job_write_lock);
		job_subs_query_disconnected(query_id);
		unlock_slurmctld(job_write_lock);
	}

	return NULL;
}

/*
 * The delivery hook job_subs.c calls from the flush pass, while the job
 * write lock is held: enqueue only, never send. Events for a query with
 * no live connection are dropped; the snapshot burst on reattach makes
 * the client whole.
 */
static void _route_event(uint16_t msg_type, job_subs_event_msg_t *event)
{
	subs_conn_t *sc;
	pending_event_t *pending;

	slurm_mutex_lock(&conn_mutex);

	sc = list_find_first(subs_conns, _match_query, &event->query_id);
	if (!sc || sc->stop) {
		slurm_mutex_unlock(&conn_mutex);
		slurm_free_job_subs_event_msg(event);
		return;
	}

	if (list_count(sc->out) >= MAX_QUEUED_EVENTS) {
		error("%s: query %u subscriber stalled with %d events queued, dropping the connection",
		      __func__, sc->query_id, MAX_QUEUED_EVENTS);
		sc->failed = true;
		sc->stop = true;
		slurm_cond_broadcast(&conn_cond);
		slurm_mutex_unlock(&conn_mutex);
		slurm_free_job_subs_event_msg(event);
		return;
	}

	pending = xmalloc(sizeof(*pending));
	pending->msg_type = msg_type;
	pending->event = event;
	list_append(sc->out, pending);
	slurm_cond_broadcast(&conn_cond);

	slurm_mutex_unlock(&conn_mutex);
}

extern void job_subs_conn_init(void)
{
	slurm_mutex_lock(&conn_mutex);
	if (!subs_conns)
		subs_conns = list_create(NULL);
	slurm_mutex_unlock(&conn_mutex);

	job_subs_set_send_fn(_route_event);
}

extern void job_subs_conn_fini(void)
{
	job_subs_set_send_fn(NULL);

	slurm_mutex_lock(&conn_mutex);
	if (subs_conns) {
		subs_conn_t *sc;
		list_itr_t *itr = list_iterator_create(subs_conns);

		while ((sc = list_next(itr)))
			sc->stop = true;
		list_iterator_destroy(itr);
		slurm_cond_broadcast(&conn_cond);

		while (writer_count > 0)
			slurm_cond_wait(&conn_cond, &conn_mutex);

		FREE_NULL_LIST(subs_conns);
	}
	slurm_mutex_unlock(&conn_mutex);
}

extern int job_subs_conn_attach(slurm_msg_t *msg, uint32_t query_id)
{
	job_subscribe_response_msg_t resp = { .query_id = query_id };
	subs_conn_t *sc, *old;
	int rc;

	xassert(msg->conn);
	xassert(!msg->conmgr_con);
	xassert(!msg->pcon);

	/*
	 * A reattach replaces any connection still serving this query.
	 * Pull the old entry out of the table right away so no further
	 * events (in particular the coming snapshot burst) can land on
	 * the dying connection; its writer thread still owns the struct
	 * and frees it on exit.
	 */
	slurm_mutex_lock(&conn_mutex);
	if (subs_conns &&
	    (old = list_remove_first(subs_conns, _match_query, &query_id))) {
		old->stop = true;
		slurm_cond_broadcast(&conn_cond);
	}
	slurm_mutex_unlock(&conn_mutex);

	if ((rc = send_msg_response(msg, RESPONSE_JOB_SUBSCRIBE, &resp))) {
		FREE_NULL_CONN(msg->conn);
		FREE_NULL_MSG(msg);
		return rc;
	}

	sc = xmalloc(sizeof(*sc));
	sc->query_id = query_id;
	sc->conn = msg->conn;
	msg->conn = NULL;
	sc->uid = msg->auth_uid;
	sc->protocol_version = msg->protocol_version;
	sc->out = list_create(_free_pending);

	slurm_mutex_lock(&conn_mutex);
	list_append(subs_conns, sc);
	writer_count++;
	slurm_mutex_unlock(&conn_mutex);

	slurm_thread_create_detached("jobsubs_writer", _writer, sc);

	FREE_NULL_MSG(msg);

	return SLURM_SUCCESS;
}

extern int job_subs_conn_count(void)
{
	int count;

	slurm_mutex_lock(&conn_mutex);
	count = subs_conns ? list_count(subs_conns) : 0;
	slurm_mutex_unlock(&conn_mutex);

	return count;
}

extern bool job_subs_conn_active(uint32_t query_id)
{
	subs_conn_t *sc;

	slurm_mutex_lock(&conn_mutex);
	sc = subs_conns ? list_find_first(subs_conns, _match_query,
					  &query_id) : NULL;
	slurm_mutex_unlock(&conn_mutex);

	return sc && !sc->stop;
}

extern void job_subs_conn_prune(void)
{
	slurmctld_lock_t job_write_lock = {
		.job = WRITE_LOCK,
	};
	time_t cutoff = time(NULL) - slurm_conf.subscription_timeout;
	int pruned;

	lock_slurmctld(job_write_lock);
	pruned = job_subs_prune(cutoff, job_subs_conn_active);
	unlock_slurmctld(job_write_lock);

	if (pruned)
		debug("%s: pruned %d expired subscription queries",
		      __func__, pruned);
}
