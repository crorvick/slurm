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

static void _dirty(job_record_t *job_ptr, job_subs_attr_t attr)
{
	lock_slurmctld(job_write_lock);
	job_subs_attr_dirty(job_ptr, attr);
	unlock_slurmctld(job_write_lock);
}

START_TEST(test_dirty_bits)
{
	job_record_t *job1, *job2;
	job_subs_track_t *track;

	job_subs_init();

	job1 = job_record_create();
	job2 = job_record_create();

	/* tracking state is allocated lazily */
	ck_assert(job_subs_track(job1) == NULL);
	ck_assert_int_eq(job_subs_modified_count(), 0);

	_dirty(job1, JOB_SUBS_ATTR_STATE);

	track = job_subs_track(job1);
	ck_assert(track != NULL);
	ck_assert(track->dirty == JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE));
	ck_assert_int_eq(job_subs_modified_count(), 1);

	/* re-marking the same attribute must not queue the job twice */
	_dirty(job1, JOB_SUBS_ATTR_STATE);
	ck_assert_int_eq(job_subs_modified_count(), 1);

	/* a second attribute accumulates in the same mask */
	_dirty(job1, JOB_SUBS_ATTR_PRIORITY);
	ck_assert(track->dirty == (JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE) |
				   JOB_SUBS_BIT(JOB_SUBS_ATTR_PRIORITY)));
	ck_assert_int_eq(job_subs_modified_count(), 1);

	/* a second job queues independently */
	_dirty(job2, JOB_SUBS_ATTR_END_TIME);
	ck_assert_int_eq(job_subs_modified_count(), 2);

	/* detach drops the queue entry along with the tracking state */
	job_subs_detach(job1);
	ck_assert(job_subs_track(job1) == NULL);
	ck_assert_int_eq(job_subs_modified_count(), 1);
	job_subs_detach(job2);
	ck_assert_int_eq(job_subs_modified_count(), 0);

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

START_TEST(test_dirty_before_init)
{
	job_record_t *job_ptr = job_record_create();

	/* harmless before job_subs_init(): no tracking state appears */
	_dirty(job_ptr, JOB_SUBS_ATTR_STATE);
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
	tcase_add_test(tc, test_membership);
	tcase_add_test(tc, test_dirty_before_init);
	suite_add_tcase(s, tc);

	sr = srunner_create(s);
	srunner_run_all(sr, CK_ENV);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);

	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
