/*****************************************************************************\
 *  job_subs.c - job status subscription tracking for slurmctld
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

#include "src/common/list.h"
#include "src/common/macros.h"
#include "src/common/xassert.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include <stdlib.h>

#include "src/slurmctld/job_subs.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/slurmctld.h"

/*
 * Jobs with at least one dirty bit set since the last flush. A job is on
 * this list iff its tracking state has a non-zero dirty mask, so the flush
 * pass never scans job_list. Entries are bare job_record_t pointers; the
 * list owns nothing.
 */
static list_t *modified_jobs = NULL;

/* Registered queries; expected to stay small enough for a plain list. */
static list_t *queries = NULL;
static uint32_t next_query_id = 1;

/*
 * Reverse index: for each trackable attribute, the queries whose filter
 * mask includes that bit. The flush pass re-runs predicates only for
 * queries that care about an attribute that actually changed, instead of
 * every registered query. A flat array is right here: the filterable
 * attribute set is small, bounded and fixed at compile time.
 */
static list_t *filter_index[JOB_SUBS_ATTR_COUNT];

static void _query_free(void *x)
{
	job_subs_query_t *query = x;

	xfree(query->job_ids);
	xfree(query);
}

extern void job_subs_init(void)
{
	xassert(!modified_jobs);
	modified_jobs = list_create(NULL);
	queries = list_create(_query_free);
	for (int i = 0; i < JOB_SUBS_ATTR_COUNT; i++)
		filter_index[i] = list_create(NULL);
}

extern void job_subs_fini(void)
{
	FREE_NULL_LIST(modified_jobs);
	FREE_NULL_LIST(queries);
	for (int i = 0; i < JOB_SUBS_ATTR_COUNT; i++)
		FREE_NULL_LIST(filter_index[i]);
}

extern job_subs_track_t *job_subs_track(job_record_t *job_ptr)
{
	return job_ptr->subs;
}

static job_subs_track_t *_track_create(job_record_t *job_ptr)
{
	if (!job_ptr->subs)
		job_ptr->subs = xmalloc(sizeof(job_subs_track_t));

	return job_ptr->subs;
}

extern void job_subs_attr_dirty(job_record_t *job_ptr, job_subs_attr_t attr)
{
	job_subs_track_t *track;

	xassert(verify_lock(JOB_LOCK, WRITE_LOCK));
	xassert(attr < JOB_SUBS_ATTR_COUNT);

	if (!modified_jobs)	/* subscriptions not initialized */
		return;

	track = _track_create(job_ptr);

	if (!track->dirty)
		list_append(modified_jobs, job_ptr);

	track->dirty |= JOB_SUBS_BIT(attr);
}

extern void job_subs_set_priority(job_record_t *job_ptr, uint32_t priority)
{
	if (job_ptr->priority == priority)
		return;

	job_record_init_priority(job_ptr, priority);
	job_subs_attr_dirty(job_ptr, JOB_SUBS_ATTR_PRIORITY);
}

extern void job_subs_set_start_time(job_record_t *job_ptr, time_t start_time)
{
	if (job_ptr->start_time == start_time)
		return;

	job_record_init_start_time(job_ptr, start_time);
	job_subs_attr_dirty(job_ptr, JOB_SUBS_ATTR_START_TIME);
}

extern void job_subs_set_end_time(job_record_t *job_ptr, time_t end_time)
{
	if (job_ptr->end_time == end_time)
		return;

	job_record_init_end_time(job_ptr, end_time);
	job_subs_attr_dirty(job_ptr, JOB_SUBS_ATTR_END_TIME);
}

extern void job_subs_set_exit_code(job_record_t *job_ptr, uint32_t exit_code)
{
	if (job_ptr->exit_code == exit_code)
		return;

	job_record_init_exit_code(job_ptr, exit_code);
	job_subs_attr_dirty(job_ptr, JOB_SUBS_ATTR_EXIT_CODE);
}

/*
 * Find the insertion point for query_id in the sorted membership list.
 * Membership lists are expected to stay short, so a linear scan is fine.
 */
static int _member_index(job_subs_track_t *track, uint32_t query_id)
{
	int i;

	for (i = 0; i < track->memb_cnt; i++) {
		if (track->memb[i] >= query_id)
			break;
	}

	return i;
}

extern bool job_subs_member_add(job_record_t *job_ptr, uint32_t query_id)
{
	job_subs_track_t *track = _track_create(job_ptr);
	int i = _member_index(track, query_id);

	if ((i < track->memb_cnt) && (track->memb[i] == query_id))
		return false;

	if (track->memb_cnt == track->memb_size) {
		track->memb_size = MAX(track->memb_size * 2, 4);
		xrecalloc(track->memb, track->memb_size,
			  sizeof(*track->memb));
	}

	memmove(&track->memb[i + 1], &track->memb[i],
		(track->memb_cnt - i) * sizeof(*track->memb));
	track->memb[i] = query_id;
	track->memb_cnt++;

	return true;
}

extern bool job_subs_member_del(job_record_t *job_ptr, uint32_t query_id)
{
	job_subs_track_t *track = job_ptr->subs;
	int i;

	if (!track)
		return false;

	i = _member_index(track, query_id);
	if ((i >= track->memb_cnt) || (track->memb[i] != query_id))
		return false;

	track->memb_cnt--;
	memmove(&track->memb[i], &track->memb[i + 1],
		(track->memb_cnt - i) * sizeof(*track->memb));

	return true;
}

extern bool job_subs_member_test(job_record_t *job_ptr, uint32_t query_id)
{
	job_subs_track_t *track = job_ptr->subs;
	int i;

	if (!track)
		return false;

	i = _member_index(track, query_id);

	return (i < track->memb_cnt) && (track->memb[i] == query_id);
}

static int _match_job(void *x, void *key)
{
	return (x == key);
}

static int _cmp_job_id(const void *a, const void *b)
{
	uint32_t ia = *(const uint32_t *) a, ib = *(const uint32_t *) b;

	return (ia > ib) - (ia < ib);
}

extern uint32_t job_subs_query_create(const uint32_t *job_ids, uint32_t cnt,
				      job_subs_mask_t emit_mask, uid_t uid)
{
	job_subs_query_t *query = xmalloc(sizeof(*query));

	xassert(queries);

	/*
	 * Monotonic id assignment; NO_VAL is the "no query yet" sentinel
	 * on the wire so skip over it if the counter ever gets there.
	 */
	if (next_query_id == NO_VAL)
		next_query_id = 1;
	query->query_id = next_query_id++;

	if (cnt) {
		query->job_ids = xcalloc(cnt, sizeof(*query->job_ids));
		memcpy(query->job_ids, job_ids,
		       cnt * sizeof(*query->job_ids));
		qsort(query->job_ids, cnt, sizeof(*query->job_ids),
		      _cmp_job_id);
		query->job_ids_cnt = cnt;
		query->filter_mask = JOB_SUBS_BIT(JOB_SUBS_ATTR_JOB_ID);
	}
	query->emit_mask = emit_mask;
	query->uid = uid;

	list_append(queries, query);

	for (int i = 0; i < JOB_SUBS_ATTR_COUNT; i++) {
		if (query->filter_mask & JOB_SUBS_BIT(i))
			list_append(filter_index[i], query);
	}

	return query->query_id;
}

static int _match_query_id(void *x, void *key)
{
	job_subs_query_t *query = x;

	return (query->query_id == *(uint32_t *) key);
}

extern job_subs_query_t *job_subs_query_find(uint32_t query_id)
{
	if (!queries)
		return NULL;

	return list_find_first(queries, _match_query_id, &query_id);
}

static int _drop_membership(void *x, void *arg)
{
	job_subs_member_del(x, *(uint32_t *) arg);

	return 0;
}

extern bool job_subs_query_delete(uint32_t query_id)
{
	job_subs_query_t *query;

	if (!queries)
		return false;

	query = list_remove_first(queries, _match_query_id, &query_id);
	if (!query)
		return false;

	for (int i = 0; i < JOB_SUBS_ATTR_COUNT; i++) {
		if (query->filter_mask & JOB_SUBS_BIT(i))
			list_delete_first(filter_index[i], _match_job, query);
	}

	if (job_list)
		list_for_each(job_list, _drop_membership, &query_id);

	_query_free(query);

	return true;
}

extern int job_subs_query_count(void)
{
	return queries ? list_count(queries) : 0;
}

extern bool job_subs_query_match(job_subs_query_t *query,
				 job_record_t *job_ptr)
{
	if (query->firehose)
		return true;

	if (!query->job_ids_cnt)
		return false;

	return bsearch(&job_ptr->job_id, query->job_ids, query->job_ids_cnt,
		       sizeof(*query->job_ids), _cmp_job_id) != NULL;
}

static job_subs_send_fn_t send_fn = NULL;

extern void job_subs_set_send_fn(job_subs_send_fn_t fn)
{
	send_fn = fn;
}

/*
 * Build and dispatch one notification carrying the job's current values
 * for the attributes in mask. The sink owns the message.
 */
static void _send_event(job_subs_query_t *query, uint16_t msg_type,
			job_record_t *job_ptr, job_subs_mask_t mask)
{
	job_subs_event_msg_t *event = xmalloc(sizeof(*event));

	event->query_id = query->query_id;
	event->job_id = job_ptr->job_id;
	event->attr_mask = mask;
	if (mask & JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE))
		event->job_state = job_ptr->job_state;
	if (mask & JOB_SUBS_BIT(JOB_SUBS_ATTR_PRIORITY))
		event->priority = job_ptr->priority;
	if (mask & JOB_SUBS_BIT(JOB_SUBS_ATTR_PARTITION))
		event->partition = xstrdup(job_ptr->partition);
	if (mask & JOB_SUBS_BIT(JOB_SUBS_ATTR_NODES))
		event->nodes = xstrdup(job_ptr->nodes);
	if (mask & JOB_SUBS_BIT(JOB_SUBS_ATTR_START_TIME))
		event->start_time = job_ptr->start_time;
	if (mask & JOB_SUBS_BIT(JOB_SUBS_ATTR_END_TIME))
		event->end_time = job_ptr->end_time;
	if (mask & JOB_SUBS_BIT(JOB_SUBS_ATTR_EXIT_CODE))
		event->exit_code = job_ptr->exit_code;

	if (send_fn)
		send_fn(msg_type, event);
	else
		slurm_free_job_subs_event_msg(event);
}

typedef struct {
	job_record_t *job_ptr;
	/* queries whose predicate truth may have flipped this cycle */
	job_subs_query_t **cand;
	int cand_cnt;
	int cand_size;
} flush_ctx_t;

static int _collect_candidate(void *x, void *arg)
{
	job_subs_query_t *query = x;
	flush_ctx_t *ctx = arg;

	/* a query can sit under several dirty filter bits: visit it once */
	for (int i = 0; i < ctx->cand_cnt; i++) {
		if (ctx->cand[i] == query)
			return 0;
	}

	if (ctx->cand_cnt == ctx->cand_size) {
		ctx->cand_size = MAX(ctx->cand_size * 2, 8);
		xrecalloc(ctx->cand, ctx->cand_size, sizeof(*ctx->cand));
	}
	ctx->cand[ctx->cand_cnt++] = query;

	return 0;
}

/*
 * Evaluate one modified job against the registered queries:
 *
 *   1. For queries whose filter references a dirty attribute, re-run the
 *      predicate. Joining the filter earns a SNAPSHOT with full values
 *      for the query's emit set; leaving it earns a DELETE.
 *   2. Every remaining member whose emit mask intersects the dirty bits
 *      gets an UPDATE with just the changed values. A query that just
 *      received a SNAPSHOT is skipped: the snapshot already carries the
 *      current values.
 */
static void _flush_job(job_record_t *job_ptr, job_subs_mask_t dirty)
{
	flush_ctx_t ctx = { .job_ptr = job_ptr };
	job_subs_track_t *track;

	for (int i = 0; i < JOB_SUBS_ATTR_COUNT; i++) {
		if (dirty & JOB_SUBS_BIT(i))
			list_for_each(filter_index[i], _collect_candidate,
				      &ctx);
	}

	for (int i = 0; i < ctx.cand_cnt; i++) {
		job_subs_query_t *query = ctx.cand[i];
		bool is_member = job_subs_member_test(job_ptr,
						      query->query_id);
		bool matches = job_subs_query_match(query, job_ptr);

		if (!is_member && matches) {
			job_subs_member_add(job_ptr, query->query_id);
			_send_event(query, MESSAGE_JOB_SNAPSHOT, job_ptr,
				    query->emit_mask);
		} else if (is_member && !matches) {
			job_subs_member_del(job_ptr, query->query_id);
			_send_event(query, MESSAGE_JOB_DELETE, job_ptr, 0);
			ctx.cand[i] = NULL;	/* no longer a member */
		} else if (!matches) {
			ctx.cand[i] = NULL;	/* never was a member */
		} else {
			ctx.cand[i] = NULL;	/* member, no snapshot */
		}
	}

	/* ctx.cand now holds exactly the queries that got a SNAPSHOT */

	track = job_ptr->subs;
	for (int i = 0; track && (i < track->memb_cnt); i++) {
		job_subs_query_t *query;
		job_subs_mask_t mask;
		bool snapshotted = false;

		for (int j = 0; j < ctx.cand_cnt; j++) {
			if (ctx.cand[j] &&
			    (ctx.cand[j]->query_id == track->memb[i])) {
				snapshotted = true;
				break;
			}
		}
		if (snapshotted)
			continue;

		if (!(query = job_subs_query_find(track->memb[i])))
			continue;

		if ((mask = dirty & query->emit_mask))
			_send_event(query, MESSAGE_JOB_UPDATE, job_ptr, mask);
	}

	xfree(ctx.cand);
}

static job_subs_flush_fn_t flush_fn = NULL;

extern void job_subs_set_flush_fn(job_subs_flush_fn_t fn)
{
	flush_fn = fn;
}

extern void job_subs_flush(void)
{
	job_record_t *job_ptr;

	if (!modified_jobs || list_is_empty(modified_jobs))
		return;

	xassert(verify_lock(JOB_LOCK, WRITE_LOCK));

	while ((job_ptr = list_pop(modified_jobs))) {
		job_subs_track_t *track = job_ptr->subs;

		xassert(track && track->dirty);

		_flush_job(job_ptr, track->dirty);

		if (flush_fn)
			flush_fn(job_ptr, track->dirty);

		track->dirty = 0;
	}
}

extern void job_subs_job_created(job_record_t *job_ptr)
{
	if (!modified_jobs)	/* subscriptions not initialized */
		return;

	for (int i = 0; i < JOB_SUBS_ATTR_COUNT; i++) {
		if (JOB_SUBS_FILTERABLE & JOB_SUBS_BIT(i))
			job_subs_attr_dirty(job_ptr, i);
	}
}

extern void job_subs_job_purged(job_record_t *job_ptr)
{
	job_subs_track_t *track = job_ptr->subs;

	if (!track)
		return;

	for (int i = 0; i < track->memb_cnt; i++) {
		job_subs_query_t *query = job_subs_query_find(track->memb[i]);
		job_subs_mask_t mask;

		if (!query)
			continue;

		if ((mask = track->dirty & query->emit_mask))
			_send_event(query, MESSAGE_JOB_UPDATE, job_ptr, mask);
		_send_event(query, MESSAGE_JOB_DELETE, job_ptr, 0);
	}

	job_subs_detach(job_ptr);
}

extern void job_subs_detach(job_record_t *job_ptr)
{
	job_subs_track_t *track = job_ptr->subs;

	if (!track)
		return;

	if (track->dirty && modified_jobs)
		list_delete_first(modified_jobs, _match_job, job_ptr);

	xfree(track->memb);
	xfree(job_ptr->subs);
}

extern int job_subs_modified_count(void)
{
	return modified_jobs ? list_count(modified_jobs) : 0;
}
