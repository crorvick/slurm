/*****************************************************************************\
 *  job_subs.h - job status subscription tracking for slurmctld
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

#ifndef _SLURMCTLD_JOB_SUBS_H
#define _SLURMCTLD_JOB_SUBS_H

#include <inttypes.h>

#include "src/common/job_record.h"

/*
 * The trackable-attribute enum (job_subs_attr_t) and its mask macros live
 * in src/common/job_record.h beside the fields they describe, since the
 * protocol pack code needs them too.
 */

/*
 * Per-job subscription tracking state, hung off job_record_t->subs and
 * owned by this module: allocated on first use, freed by job_subs_detach()
 * when the job is purged.
 */
typedef struct job_subs_track {
	job_subs_mask_t dirty;	/* attributes changed since the last flush */
	uint32_t *memb;		/* query ids this job matches, ascending */
	uint16_t memb_cnt;	/* entries used in memb[] */
	uint16_t memb_size;	/* entries allocated in memb[] */
} job_subs_track_t;

/* Set up module state (the modified-jobs list). */
extern void job_subs_init(void);

/* Tear down module state. Any per-job tracking must already be detached. */
extern void job_subs_fini(void);

/*
 * Mark one attribute changed on a job, allocating tracking state on first
 * use and queueing the job for the next flush pass. Call only when the
 * value actually changed; the setters own that comparison.
 */
extern void job_subs_attr_dirty(job_record_t *job_ptr, job_subs_attr_t attr);

/* The job's tracking state, or NULL if nothing has been recorded yet. */
extern job_subs_track_t *job_subs_track(job_record_t *job_ptr);

/*
 * Tracked attribute setters. Each compares against the current value and
 * marks the attribute dirty only on a real change, so a dirty bit always
 * means the value differs from what the last flush saw. slurmctld code
 * must change tracked job_record_t fields through these; job_state has
 * its own setters in job_state.c which feed the same dirty bits.
 */
extern void job_subs_set_priority(job_record_t *job_ptr, uint32_t priority);
extern void job_subs_set_start_time(job_record_t *job_ptr, time_t start_time);
extern void job_subs_set_end_time(job_record_t *job_ptr, time_t end_time);
extern void job_subs_set_exit_code(job_record_t *job_ptr, uint32_t exit_code);

/*
 * Membership list operations. The list holds the ids of every query the
 * job currently matches, kept sorted so tests and removals stay simple.
 * job_subs_member_add() and job_subs_member_del() return true if the list
 * changed.
 */
extern bool job_subs_member_add(job_record_t *job_ptr, uint32_t query_id);
extern bool job_subs_member_del(job_record_t *job_ptr, uint32_t query_id);
extern bool job_subs_member_test(job_record_t *job_ptr, uint32_t query_id);

/*
 * Drop a job's tracking state: remove it from the modified-jobs list and
 * free the tracking struct. Call before the job_record_t is freed.
 */
extern void job_subs_detach(job_record_t *job_ptr);

/*
 * A job just entered job_list. Treat it as "all filter bits dirty at
 * t=0": the next flush runs every filter-indexed query's predicate over
 * it and snapshots the matches, through exactly the same path a filter
 * attribute change would take.
 */
extern void job_subs_job_created(job_record_t *job_ptr);

/*
 * The job is leaving job_list for good. Send members a final update for
 * any dirty emit attributes that have not flushed yet, then the delete,
 * then drop the tracking state. Call with the job record still intact.
 */
extern void job_subs_job_purged(job_record_t *job_ptr);

/*
 * A registered query: a filter predicate over jobs plus the set of
 * attributes whose changes stream to the subscriber. The predicate is
 * job-id membership today, but the filter mask records which attribute
 * bits require re-running it, so mutable filter attributes (partition,
 * account, ...) can be added without reshaping the flush pass.
 */
typedef struct {
	uint32_t query_id;	/* server-assigned, returned on subscribe */
	uint32_t *job_ids;	/* filter: sorted job ids to match */
	uint32_t job_ids_cnt;	/* count of job_ids[] */
	bool firehose;		/* match every job, ignore the filter */
	job_subs_mask_t filter_mask; /* dirty bits forcing predicate re-run */
	job_subs_mask_t emit_mask;   /* dirty bits producing an update */
	uid_t uid;		/* subscriber, for visibility scoping */
	time_t disconnect_time;	/* 0 while attached; set on disconnect so
				 * stale queries can be pruned */
} job_subs_query_t;

/*
 * Register a new query over the given job ids (copied) and emit set.
 * Returns the assigned query id. Membership binding and the snapshot
 * burst are driven by the caller via job_subs_query_bind().
 */
extern uint32_t job_subs_query_create(const uint32_t *job_ids, uint32_t cnt,
				      job_subs_mask_t emit_mask, uid_t uid);

/* The registered query, or NULL. */
extern job_subs_query_t *job_subs_query_find(uint32_t query_id);

/*
 * Unregister a query and remove it from every job's membership list.
 * Returns false if the id is unknown.
 */
extern bool job_subs_query_delete(uint32_t query_id);

/* Number of registered queries. */
extern int job_subs_query_count(void);

/* True if the job satisfies the query's filter predicate. */
extern bool job_subs_query_match(job_subs_query_t *query,
				 job_record_t *job_ptr);

/*
 * Sink for outgoing subscription messages. The flush pass builds a
 * job_subs_event_msg_t per notification and hands it here together with
 * its message type (MESSAGE_JOB_SNAPSHOT/UPDATE/DELETE); the sink owns
 * the message. The network layer installs the real sender; tests install
 * a capturing sink. With no sink installed, events are dropped.
 */
typedef void (*job_subs_send_fn_t)(uint16_t msg_type,
				   job_subs_event_msg_t *event);
extern void job_subs_set_send_fn(job_subs_send_fn_t fn);

/*
 * Diagnostic hook for the flush pass, called once per modified job with
 * the job's accumulated dirty mask after query evaluation, while the job
 * write lock is still held. Tests install a capturing consumer.
 */
typedef void (*job_subs_flush_fn_t)(job_record_t *job_ptr,
				    job_subs_mask_t dirty);
extern void job_subs_set_flush_fn(job_subs_flush_fn_t fn);

/*
 * Flush the modified-jobs list: hand each queued job to the consumer,
 * then clear its dirty bits. unlock_slurmctld() calls this at the job
 * write lock boundary, the point where a batch of mutations (an RPC
 * handler, a scheduling cycle, ...) is known to be complete and
 * consistent.
 */
extern void job_subs_flush(void);

/* Number of jobs waiting for the next flush pass. */
extern int job_subs_modified_count(void);

#endif /* _SLURMCTLD_JOB_SUBS_H */
