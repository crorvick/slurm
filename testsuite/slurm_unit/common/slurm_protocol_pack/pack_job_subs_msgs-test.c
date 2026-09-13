/*****************************************************************************\
 *  pack_job_subs_msgs-test.c - round-trip the job subscription messages
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

#include <check.h>
#include <stdio.h>
#include <stdlib.h>

#include "src/common/job_record.h"
#include "src/common/slurm_protocol_pack.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

START_TEST(subscribe_round_trip)
{
	int rc;
	buf_t *buf = init_buf(1024);
	slurm_msg_t msg = {{0}};
	uint32_t job_ids[] = { 42, 1000, 4294967294u };
	job_subscribe_msg_t req = {
		.query_id = NO_VAL,
		.emit_mask = JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE) |
			     JOB_SUBS_BIT(JOB_SUBS_ATTR_END_TIME),
		.job_ids = job_ids,
		.job_ids_cnt = 3,
	};
	job_subscribe_msg_t *out;

	msg.msg_type = REQUEST_JOB_SUBSCRIBE;
	msg.protocol_version = SLURM_PROTOCOL_VERSION;
	msg.data = &req;

	rc = pack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	set_buf_offset(buf, 0);
	msg.data = NULL;
	rc = unpack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	out = msg.data;
	ck_assert(out != NULL);
	ck_assert(out->query_id == NO_VAL);
	ck_assert_int_eq(out->flags, 0);
	ck_assert(out->emit_mask == req.emit_mask);
	ck_assert_int_eq(out->job_ids_cnt, 3);
	ck_assert(out->job_ids[0] == 42);
	ck_assert(out->job_ids[1] == 1000);
	ck_assert(out->job_ids[2] == 4294967294u);

	free_buf(buf);
	slurm_free_msg_data(msg.msg_type, msg.data);
}
END_TEST

/*
 * A firehose request carries neither an id filter nor an emit set; the
 * server supplies both. Only the flag has to survive.
 */
START_TEST(subscribe_firehose_round_trip)
{
	int rc;
	buf_t *buf = init_buf(1024);
	slurm_msg_t msg = {{0}};
	job_subscribe_msg_t req = {
		.query_id = NO_VAL,
		.flags = JOB_SUBS_FLAG_FIREHOSE,
	};
	job_subscribe_msg_t *out;

	msg.msg_type = REQUEST_JOB_SUBSCRIBE;
	msg.protocol_version = SLURM_PROTOCOL_VERSION;
	msg.data = &req;

	rc = pack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	set_buf_offset(buf, 0);
	msg.data = NULL;
	rc = unpack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	out = msg.data;
	ck_assert(out != NULL);
	ck_assert_int_eq(out->flags, JOB_SUBS_FLAG_FIREHOSE);
	ck_assert_int_eq(out->job_ids_cnt, 0);
	ck_assert(out->emit_mask == 0);

	free_buf(buf);
	slurm_free_msg_data(msg.msg_type, msg.data);
}
END_TEST

START_TEST(subscribe_response_round_trip)
{
	int rc;
	buf_t *buf = init_buf(1024);
	slurm_msg_t msg = {{0}};
	job_subscribe_response_msg_t resp = { .query_id = 17 };
	job_subscribe_response_msg_t *out;

	msg.msg_type = RESPONSE_JOB_SUBSCRIBE;
	msg.protocol_version = SLURM_PROTOCOL_VERSION;
	msg.data = &resp;

	rc = pack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	set_buf_offset(buf, 0);
	msg.data = NULL;
	rc = unpack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	out = msg.data;
	ck_assert(out != NULL);
	ck_assert_int_eq(out->query_id, 17);

	free_buf(buf);
	slurm_free_msg_data(msg.msg_type, msg.data);
}
END_TEST

/*
 * A snapshot carries a full set of attribute values; assert every field
 * survives, including the strings.
 */
START_TEST(event_full_round_trip)
{
	int rc;
	buf_t *buf = init_buf(1024);
	slurm_msg_t msg = {{0}};
	job_subs_event_msg_t ev = {
		.query_id = 3,
		.job_id = 1234,
		.attr_mask = JOB_SUBS_ALL,
		.job_state = JOB_RUNNING,
		.priority = 4021,
		.partition = "gpu",
		.nodes = "node[001-004]",
		.start_time = 1757620000,
		.end_time = 1757623600,
		.exit_code = 0,
	};
	job_subs_event_msg_t *out;

	msg.msg_type = MESSAGE_JOB_SNAPSHOT;
	msg.protocol_version = SLURM_PROTOCOL_VERSION;
	msg.data = &ev;

	rc = pack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	set_buf_offset(buf, 0);
	msg.data = NULL;
	rc = unpack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	out = msg.data;
	ck_assert(out != NULL);
	ck_assert_int_eq(out->query_id, 3);
	ck_assert_int_eq(out->job_id, 1234);
	ck_assert(out->attr_mask == JOB_SUBS_ALL);
	ck_assert_int_eq(out->job_state, JOB_RUNNING);
	ck_assert_int_eq(out->priority, 4021);
	ck_assert_str_eq(out->partition, "gpu");
	ck_assert_str_eq(out->nodes, "node[001-004]");
	ck_assert(out->start_time == 1757620000);
	ck_assert(out->end_time == 1757623600);
	ck_assert_int_eq(out->exit_code, 0);

	free_buf(buf);
	slurm_free_msg_data(msg.msg_type, msg.data);
}
END_TEST

/*
 * An update carries only the changed attributes: fields outside the mask
 * must not be packed at all, and must come back zeroed.
 */
START_TEST(event_sparse_round_trip)
{
	int rc;
	buf_t *buf = init_buf(1024);
	slurm_msg_t msg = {{0}};
	job_subs_event_msg_t ev = {
		.query_id = 8,
		.job_id = 77,
		.attr_mask = JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE),
		.job_state = JOB_COMPLETE,
		/* poison the unmasked fields: they must not travel */
		.priority = 999,
		.partition = "should-not-travel",
		.exit_code = 77,
	};
	job_subs_event_msg_t *out;

	msg.msg_type = MESSAGE_JOB_UPDATE;
	msg.protocol_version = SLURM_PROTOCOL_VERSION;
	msg.data = &ev;

	rc = pack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	set_buf_offset(buf, 0);
	msg.data = NULL;
	rc = unpack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	out = msg.data;
	ck_assert(out != NULL);
	ck_assert(out->attr_mask == JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE));
	ck_assert_int_eq(out->job_state, JOB_COMPLETE);
	ck_assert_int_eq(out->priority, 0);
	ck_assert(out->partition == NULL);
	ck_assert(out->nodes == NULL);
	ck_assert_int_eq(out->exit_code, 0);

	free_buf(buf);
	slurm_free_msg_data(msg.msg_type, msg.data);
}
END_TEST

/* A delete is just (query_id, job_id) with an empty mask. */
START_TEST(event_delete_round_trip)
{
	int rc;
	buf_t *buf = init_buf(1024);
	slurm_msg_t msg = {{0}};
	job_subs_event_msg_t ev = {
		.query_id = 5,
		.job_id = 4321,
		.attr_mask = 0,
	};
	job_subs_event_msg_t *out;

	msg.msg_type = MESSAGE_JOB_DELETE;
	msg.protocol_version = SLURM_PROTOCOL_VERSION;
	msg.data = &ev;

	rc = pack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	set_buf_offset(buf, 0);
	msg.data = NULL;
	rc = unpack_msg(&msg, buf);
	ck_assert_int_eq(rc, SLURM_SUCCESS);

	out = msg.data;
	ck_assert(out != NULL);
	ck_assert_int_eq(out->query_id, 5);
	ck_assert_int_eq(out->job_id, 4321);
	ck_assert(out->attr_mask == 0);

	free_buf(buf);
	slurm_free_msg_data(msg.msg_type, msg.data);
}
END_TEST

Suite *suite(void)
{
	Suite *s = suite_create("Pack job subscription messages");
	TCase *tc_core = tcase_create("round-trips");

	tcase_add_test(tc_core, subscribe_round_trip);
	tcase_add_test(tc_core, subscribe_firehose_round_trip);
	tcase_add_test(tc_core, subscribe_response_round_trip);
	tcase_add_test(tc_core, event_full_round_trip);
	tcase_add_test(tc_core, event_sparse_round_trip);
	tcase_add_test(tc_core, event_delete_round_trip);
	suite_add_tcase(s, tc_core);

	return s;
}

int main(void)
{
	int number_failed;
	SRunner *sr = srunner_create(NULL);

	srunner_add_suite(sr, suite());
	srunner_run_all(sr, CK_VERBOSE);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);

	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
