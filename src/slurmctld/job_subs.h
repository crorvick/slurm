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
 * The attributes a job status subscription can track. Every attribute may
 * appear in a query's emit set; only those in JOB_SUBS_FILTERABLE may be
 * referenced by a filter predicate.
 *
 * The order is part of the wire protocol once the subscription RPCs exist:
 * add new attributes at the end, before JOB_SUBS_ATTR_COUNT.
 */
typedef enum {
	JOB_SUBS_ATTR_JOB_ID = 0,
	JOB_SUBS_ATTR_STATE,
	JOB_SUBS_ATTR_PRIORITY,
	JOB_SUBS_ATTR_PARTITION,
	JOB_SUBS_ATTR_NODES,
	JOB_SUBS_ATTR_START_TIME,
	JOB_SUBS_ATTR_END_TIME,
	JOB_SUBS_ATTR_EXIT_CODE,
	JOB_SUBS_ATTR_COUNT
} job_subs_attr_t;

typedef uint64_t job_subs_mask_t;

#define JOB_SUBS_BIT(attr) ((job_subs_mask_t) 1 << (attr))
#define JOB_SUBS_ALL (JOB_SUBS_BIT(JOB_SUBS_ATTR_COUNT) - 1)

/*
 * Attributes a filter predicate may reference. Job id is immutable so
 * membership never needs re-evaluation today, but the flush pass must not
 * assume this: future filterable attributes (partition, account, ...) are
 * mutable.
 */
#define JOB_SUBS_FILTERABLE JOB_SUBS_BIT(JOB_SUBS_ATTR_JOB_ID)

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

/* Number of jobs waiting for the next flush pass. */
extern int job_subs_modified_count(void);

#endif /* _SLURMCTLD_JOB_SUBS_H */
