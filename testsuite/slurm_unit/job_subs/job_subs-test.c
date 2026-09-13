/*****************************************************************************\
 *  job_subs-test.c - tests for job status subscription tracking
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

#include <stdio.h>
#include <stdlib.h>

#include <check.h>

#include "src/common/job_record.h"
#include "src/common/log.h"

#include "src/slurmctld/job_subs.h"
#include "src/slurmctld/locks.h"

static slurmctld_lock_t job_write_lock = { .job = WRITE_LOCK };

/* referenced by job_subs_query_delete(); the tests populate it */
list_t *job_list = NULL;

START_TEST(test_dirty_bits)
{
	job_record_t *job1, *job2;
	job_subs_track_t *track;

	job_subs_init();

	job1 = job_record_create();
	job2 = job_record_create();

	lock_slurmctld(job_write_lock);

	/* tracking state is allocated lazily */
	ck_assert(job_subs_track(job1) == NULL);
	ck_assert_int_eq(job_subs_modified_count(), 0);

	job_subs_attr_dirty(job1, JOB_SUBS_ATTR_STATE);

	track = job_subs_track(job1);
	ck_assert(track != NULL);
	ck_assert(track->dirty == JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE));
	ck_assert_int_eq(job_subs_modified_count(), 1);

	/* re-marking the same attribute must not queue the job twice */
	job_subs_attr_dirty(job1, JOB_SUBS_ATTR_STATE);
	ck_assert_int_eq(job_subs_modified_count(), 1);

	/* a second attribute accumulates in the same mask */
	job_subs_attr_dirty(job1, JOB_SUBS_ATTR_PRIORITY);
	ck_assert(track->dirty == (JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE) |
				   JOB_SUBS_BIT(JOB_SUBS_ATTR_PRIORITY)));
	ck_assert_int_eq(job_subs_modified_count(), 1);

	/* a second job queues independently */
	job_subs_attr_dirty(job2, JOB_SUBS_ATTR_END_TIME);
	ck_assert_int_eq(job_subs_modified_count(), 2);

	/* releasing the job write lock flushes and clears everything */
	unlock_slurmctld(job_write_lock);
	ck_assert_int_eq(job_subs_modified_count(), 0);
	ck_assert(job_subs_track(job1)->dirty == 0);
	ck_assert(job_subs_track(job2)->dirty == 0);

	job_subs_detach(job1);
	ck_assert(job_subs_track(job1) == NULL);
	job_subs_detach(job2);

	job_subs_fini();
}
END_TEST

START_TEST(test_detach_dequeues)
{
	job_record_t *job1, *job2;

	job_subs_init();
	job1 = job_record_create();
	job2 = job_record_create();

	lock_slurmctld(job_write_lock);
	job_subs_attr_dirty(job1, JOB_SUBS_ATTR_STATE);
	job_subs_attr_dirty(job2, JOB_SUBS_ATTR_STATE);
	ck_assert_int_eq(job_subs_modified_count(), 2);

	/* detach with dirty bits pending drops the queue entry too */
	job_subs_detach(job1);
	ck_assert_int_eq(job_subs_modified_count(), 1);
	job_subs_detach(job2);
	ck_assert_int_eq(job_subs_modified_count(), 0);
	unlock_slurmctld(job_write_lock);

	job_subs_fini();
}
END_TEST

START_TEST(test_membership)
{
	job_record_t *job_ptr;
	job_subs_track_t *track;

	job_subs_init();
	job_ptr = job_record_create();

	ck_assert(!job_subs_member_test(job_ptr, 1));

	/* out-of-order adds land sorted */
	ck_assert(job_subs_member_add(job_ptr, 5));
	ck_assert(job_subs_member_add(job_ptr, 1));
	ck_assert(job_subs_member_add(job_ptr, 3));
	track = job_subs_track(job_ptr);
	ck_assert_int_eq(track->memb_cnt, 3);
	ck_assert_int_eq(track->memb[0], 1);
	ck_assert_int_eq(track->memb[1], 3);
	ck_assert_int_eq(track->memb[2], 5);

	/* duplicate add is a no-op */
	ck_assert(!job_subs_member_add(job_ptr, 3));
	ck_assert_int_eq(track->memb_cnt, 3);

	ck_assert(job_subs_member_test(job_ptr, 1));
	ck_assert(job_subs_member_test(job_ptr, 3));
	ck_assert(job_subs_member_test(job_ptr, 5));
	ck_assert(!job_subs_member_test(job_ptr, 2));

	/* removing an absent id reports no change */
	ck_assert(!job_subs_member_del(job_ptr, 2));
	ck_assert(job_subs_member_del(job_ptr, 3));
	ck_assert(!job_subs_member_test(job_ptr, 3));
	ck_assert_int_eq(track->memb_cnt, 2);

	/* push past the initial allocation to exercise growth */
	for (uint32_t id = 10; id < 30; id++)
		ck_assert(job_subs_member_add(job_ptr, id));
	ck_assert_int_eq(track->memb_cnt, 22);
	ck_assert(job_subs_member_test(job_ptr, 29));
	ck_assert(job_subs_member_test(job_ptr, 1));

	/* membership operations never queue a flush */
	ck_assert_int_eq(job_subs_modified_count(), 0);

	job_subs_detach(job_ptr);
	job_subs_fini();
}
END_TEST

START_TEST(test_setters_detect_change)
{
	job_record_t *job_ptr;
	job_subs_track_t *track;

	job_subs_init();
	job_ptr = job_record_create();

	lock_slurmctld(job_write_lock);

	/* a genuine change dirties exactly that attribute */
	job_subs_set_priority(job_ptr, 100);
	track = job_subs_track(job_ptr);
	ck_assert(track != NULL);
	ck_assert(track->dirty == JOB_SUBS_BIT(JOB_SUBS_ATTR_PRIORITY));

	job_subs_set_start_time(job_ptr, 1000);
	job_subs_set_end_time(job_ptr, 2000);
	job_subs_set_exit_code(job_ptr, 0);	/* already 0: no change */
	ck_assert(track->dirty == (JOB_SUBS_BIT(JOB_SUBS_ATTR_PRIORITY) |
				   JOB_SUBS_BIT(JOB_SUBS_ATTR_START_TIME) |
				   JOB_SUBS_BIT(JOB_SUBS_ATTR_END_TIME)));
	ck_assert_int_eq(job_ptr->priority, 100);
	ck_assert_int_eq(job_ptr->start_time, 1000);
	ck_assert_int_eq(job_ptr->end_time, 2000);

	unlock_slurmctld(job_write_lock);

	/* writing the same values again must not dirty anything */
	lock_slurmctld(job_write_lock);
	job_subs_set_priority(job_ptr, 100);
	job_subs_set_start_time(job_ptr, 1000);
	job_subs_set_end_time(job_ptr, 2000);
	ck_assert(track->dirty == 0);
	ck_assert_int_eq(job_subs_modified_count(), 0);
	unlock_slurmctld(job_write_lock);

	job_subs_detach(job_ptr);
	job_subs_fini();
}
END_TEST

/* capture consumer for the flush tests */
#define CAP_MAX 8
static struct {
	job_record_t *job_ptr;
	job_subs_mask_t dirty;
} cap[CAP_MAX];
static int cap_cnt;

static void _capture(job_record_t *job_ptr, job_subs_mask_t dirty)
{
	ck_assert(cap_cnt < CAP_MAX);
	cap[cap_cnt].job_ptr = job_ptr;
	cap[cap_cnt].dirty = dirty;
	cap_cnt++;
}

START_TEST(test_flush_consumer)
{
	job_record_t *job1, *job2;

	job_subs_init();
	job_subs_set_flush_fn(_capture);
	job1 = job_record_create();
	job2 = job_record_create();

	/*
	 * A mutation batch: the consumer must see each modified job once,
	 * with its accumulated dirty mask, at the unlock boundary.
	 */
	lock_slurmctld(job_write_lock);
	job_subs_set_priority(job1, 7);
	job_subs_set_start_time(job1, 1000);
	job_subs_set_end_time(job2, 2000);
	ck_assert_int_eq(cap_cnt, 0);	/* nothing until the flush */
	unlock_slurmctld(job_write_lock);

	ck_assert_int_eq(cap_cnt, 2);
	ck_assert(cap[0].job_ptr == job1);
	ck_assert(cap[0].dirty == (JOB_SUBS_BIT(JOB_SUBS_ATTR_PRIORITY) |
				   JOB_SUBS_BIT(JOB_SUBS_ATTR_START_TIME)));
	ck_assert(cap[1].job_ptr == job2);
	ck_assert(cap[1].dirty == JOB_SUBS_BIT(JOB_SUBS_ATTR_END_TIME));

	/* an empty batch flushes nothing */
	lock_slurmctld(job_write_lock);
	unlock_slurmctld(job_write_lock);
	ck_assert_int_eq(cap_cnt, 2);

	/* a no-op write flushes nothing either */
	lock_slurmctld(job_write_lock);
	job_subs_set_priority(job1, 7);
	unlock_slurmctld(job_write_lock);
	ck_assert_int_eq(cap_cnt, 2);

	job_subs_set_flush_fn(NULL);
	job_subs_detach(job1);
	job_subs_detach(job2);
	job_subs_fini();
}
END_TEST

START_TEST(test_query_registry)
{
	uint32_t ids[] = { 30, 10, 20 };	/* deliberately unsorted */
	uint32_t qid1, qid2;
	job_subs_query_t *query;
	job_record_t *job_ptr;

	job_subs_init();
	job_ptr = job_record_create();

	qid1 = job_subs_query_create(ids, 3,
				     JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE), 1001);
	qid2 = job_subs_query_create(NULL, 0, JOB_SUBS_ALL, 1002);
	ck_assert(qid1 != qid2);
	ck_assert_int_eq(job_subs_query_count(), 2);

	query = job_subs_query_find(qid1);
	ck_assert(query != NULL);
	ck_assert_int_eq(query->query_id, qid1);
	ck_assert(query->uid == 1001);

	/* the filter ids were copied and sorted */
	ck_assert_int_eq(query->job_ids_cnt, 3);
	ck_assert_int_eq(query->job_ids[0], 10);
	ck_assert_int_eq(query->job_ids[1], 20);
	ck_assert_int_eq(query->job_ids[2], 30);
	ck_assert(query->filter_mask == JOB_SUBS_BIT(JOB_SUBS_ATTR_JOB_ID));

	/* predicate: job id membership */
	job_ptr->job_id = 20;
	ck_assert(job_subs_query_match(query, job_ptr));
	job_ptr->job_id = 21;
	ck_assert(!job_subs_query_match(query, job_ptr));

	/* an empty filter matches nothing... */
	query = job_subs_query_find(qid2);
	ck_assert(query->filter_mask == 0);
	ck_assert(!job_subs_query_match(query, job_ptr));

	/* ...unless it is the firehose, which matches everything */
	query->firehose = true;
	ck_assert(job_subs_query_match(query, job_ptr));

	ck_assert(job_subs_query_find(9999) == NULL);
	ck_assert(!job_subs_query_delete(9999));

	job_subs_fini();
}
END_TEST

START_TEST(test_query_delete_drops_memberships)
{
	uint32_t ids[] = { 5 };
	uint32_t qid;
	job_record_t *job_ptr;

	job_subs_init();
	job_list = list_create(NULL);
	job_ptr = job_record_create();
	job_ptr->job_id = 5;
	list_append(job_list, job_ptr);

	qid = job_subs_query_create(ids, 1, JOB_SUBS_ALL, 0);
	ck_assert(job_subs_member_add(job_ptr, qid));
	ck_assert(job_subs_member_test(job_ptr, qid));

	ck_assert(job_subs_query_delete(qid));
	ck_assert(!job_subs_member_test(job_ptr, qid));
	ck_assert(job_subs_query_find(qid) == NULL);
	ck_assert_int_eq(job_subs_query_count(), 0);

	job_subs_detach(job_ptr);
	FREE_NULL_LIST(job_list);
	job_subs_fini();
}
END_TEST

/* capture sink for the event-sequence tests */
#define EV_MAX 16
static struct {
	uint16_t msg_type;
	job_subs_event_msg_t *event;
} evs[EV_MAX];
static int ev_cnt;

static void _capture_event(uint16_t msg_type, job_subs_event_msg_t *event)
{
	ck_assert(ev_cnt < EV_MAX);
	evs[ev_cnt].msg_type = msg_type;
	evs[ev_cnt].event = event;
	ev_cnt++;
}

static void _drain_events(void)
{
	for (int i = 0; i < ev_cnt; i++)
		slurm_free_job_subs_event_msg(evs[i].event);
	ev_cnt = 0;
}

/*
 * Drive a mutation sequence against two queries and assert the exact
 * message stream each produces. Marking JOB_ID dirty stands in for the
 * initial predicate evaluation a new job gets (all filter bits dirty at
 * t=0); a mutable filter attribute would take the same path.
 */
START_TEST(test_event_sequence)
{
	uint32_t ids1[] = { 5 }, ids2[] = { 7 };
	uint32_t qid1, qid2;
	job_record_t *job5, *job7;

	job_subs_init();
	job_subs_set_send_fn(_capture_event);

	qid1 = job_subs_query_create(ids1, 1,
				     JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE) |
				     JOB_SUBS_BIT(JOB_SUBS_ATTR_END_TIME),
				     0);
	qid2 = job_subs_query_create(ids2, 1, JOB_SUBS_ALL, 0);

	job5 = job_record_create();
	job5->job_id = 5;
	job_record_init_priority(job5, 50);
	job_record_init_end_time(job5, 500);

	/* job 5 "arrives": snapshot to query 1 only, with emit values */
	lock_slurmctld(job_write_lock);
	job_subs_attr_dirty(job5, JOB_SUBS_ATTR_JOB_ID);
	unlock_slurmctld(job_write_lock);

	ck_assert_int_eq(ev_cnt, 1);
	ck_assert_int_eq(evs[0].msg_type, MESSAGE_JOB_SNAPSHOT);
	ck_assert_int_eq(evs[0].event->query_id, qid1);
	ck_assert_int_eq(evs[0].event->job_id, 5);
	ck_assert(evs[0].event->attr_mask ==
		  (JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE) |
		   JOB_SUBS_BIT(JOB_SUBS_ATTR_END_TIME)));
	ck_assert(evs[0].event->end_time == 500);
	_drain_events();

	/*
	 * A subscribed attribute and an unsubscribed one change in the
	 * same batch: one update, carrying only the subscribed value.
	 */
	lock_slurmctld(job_write_lock);
	job_subs_set_end_time(job5, 600);
	job_subs_set_priority(job5, 60);
	unlock_slurmctld(job_write_lock);

	ck_assert_int_eq(ev_cnt, 1);
	ck_assert_int_eq(evs[0].msg_type, MESSAGE_JOB_UPDATE);
	ck_assert_int_eq(evs[0].event->query_id, qid1);
	ck_assert(evs[0].event->attr_mask ==
		  JOB_SUBS_BIT(JOB_SUBS_ATTR_END_TIME));
	ck_assert(evs[0].event->end_time == 600);
	_drain_events();

	/* only unsubscribed attributes change: silence */
	lock_slurmctld(job_write_lock);
	job_subs_set_priority(job5, 70);
	unlock_slurmctld(job_write_lock);
	ck_assert_int_eq(ev_cnt, 0);

	/*
	 * A job that enters the filter and changes an emit attribute in
	 * the same batch gets the snapshot only: it already carries the
	 * current values.
	 */
	job7 = job_record_create();
	job7->job_id = 7;
	lock_slurmctld(job_write_lock);
	job_subs_attr_dirty(job7, JOB_SUBS_ATTR_JOB_ID);
	job_subs_set_end_time(job7, 700);
	unlock_slurmctld(job_write_lock);

	ck_assert_int_eq(ev_cnt, 1);
	ck_assert_int_eq(evs[0].msg_type, MESSAGE_JOB_SNAPSHOT);
	ck_assert_int_eq(evs[0].event->query_id, qid2);
	ck_assert(evs[0].event->attr_mask == JOB_SUBS_ALL);
	ck_assert(evs[0].event->end_time == 700);
	_drain_events();

	/* leaving the filter produces a delete */
	job7->job_id = 8;
	lock_slurmctld(job_write_lock);
	job_subs_attr_dirty(job7, JOB_SUBS_ATTR_JOB_ID);
	unlock_slurmctld(job_write_lock);

	ck_assert_int_eq(ev_cnt, 1);
	ck_assert_int_eq(evs[0].msg_type, MESSAGE_JOB_DELETE);
	ck_assert_int_eq(evs[0].event->query_id, qid2);
	ck_assert(evs[0].event->attr_mask == 0);
	ck_assert(!job_subs_member_test(job7, qid2));
	_drain_events();

	/* and its emit changes no longer notify anyone */
	lock_slurmctld(job_write_lock);
	job_subs_set_end_time(job7, 800);
	unlock_slurmctld(job_write_lock);
	ck_assert_int_eq(ev_cnt, 0);

	job_subs_set_send_fn(NULL);
	job_subs_detach(job5);
	job_subs_detach(job7);
	job_subs_fini();
}
END_TEST

START_TEST(test_dirty_before_init)
{
	job_record_t *job_ptr = job_record_create();

	/* harmless before job_subs_init(): no tracking state appears */
	lock_slurmctld(job_write_lock);
	job_subs_attr_dirty(job_ptr, JOB_SUBS_ATTR_STATE);
	unlock_slurmctld(job_write_lock);
	ck_assert(job_subs_track(job_ptr) == NULL);
	ck_assert_int_eq(job_subs_modified_count(), 0);

	job_subs_detach(job_ptr);
}
END_TEST

int main(void)
{
	int number_failed;
	Suite *s = suite_create("job_subs");
	TCase *tc = tcase_create("job_subs");
	SRunner *sr;

	log_options_t log_opts = LOG_OPTS_INITIALIZER;
	log_init("job_subs-test", log_opts, 0, NULL);

	tcase_add_test(tc, test_dirty_bits);
	tcase_add_test(tc, test_detach_dequeues);
	tcase_add_test(tc, test_membership);
	tcase_add_test(tc, test_setters_detect_change);
	tcase_add_test(tc, test_flush_consumer);
	tcase_add_test(tc, test_query_registry);
	tcase_add_test(tc, test_query_delete_drops_memberships);
	tcase_add_test(tc, test_event_sequence);
	tcase_add_test(tc, test_dirty_before_init);
	suite_add_tcase(s, tc);

	sr = srunner_create(s);
	srunner_run_all(sr, CK_ENV);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);

	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
