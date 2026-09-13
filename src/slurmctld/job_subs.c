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

#include "src/slurmctld/job_subs.h"
#include "src/slurmctld/locks.h"

/*
 * Jobs with at least one dirty bit set since the last flush. A job is on
 * this list iff its tracking state has a non-zero dirty mask, so the flush
 * pass never scans job_list. Entries are bare job_record_t pointers; the
 * list owns nothing.
 */
static list_t *modified_jobs = NULL;

extern void job_subs_init(void)
{
	xassert(!modified_jobs);
	modified_jobs = list_create(NULL);
}

extern void job_subs_fini(void)
{
	FREE_NULL_LIST(modified_jobs);
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

		if (flush_fn)
			flush_fn(job_ptr, track->dirty);

		track->dirty = 0;
	}
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
