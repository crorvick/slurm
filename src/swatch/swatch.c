/*****************************************************************************\
 *  swatch.c - watch job status changes streamed from slurmctld
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

#include "config.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/data.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/parse_time.h"
#include "src/common/read_config.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/xmalloc.h"
#include "src/common/xsignal.h"
#include "src/common/xstring.h"

#include "src/interfaces/conn.h"
#include "src/interfaces/serializer.h"

#include "src/swatch/opt.h"

/*
 * The subscription is long-lived by nature, so the only ordinary way out
 * is a signal. The handler just records the request; the receive loop is
 * blocked in a read and notices once that read returns.
 */
static volatile sig_atomic_t stop_requested;

static conn_t *conn;
static uint32_t query_id = NO_VAL;
static uint32_t events_seen;
static bool header_printed;

static void _on_signal(int sig)
{
	stop_requested = 1;
}

/*
 * Which attributes to give columns to.
 *
 * A reattach (--query-id) does not restate the emit set, since it belongs
 * to the subscription rather than to this invocation. The first snapshot
 * carries the whole of it, so learn the columns from that.
 */
static job_subs_mask_t learned_mask;

static job_subs_mask_t _display_mask(void)
{
	return opt.emit_mask ? opt.emit_mask : learned_mask;
}

/* Index of the rightmost displayed attribute, which is not padded. */
static int _last_displayed(void)
{
	int last = -1;

	for (int i = 0; swatch_attrs[i].name; i++) {
		if (_display_mask() & JOB_SUBS_BIT(swatch_attrs[i].attr))
			last = i;
	}

	return last;
}

static void _print_header(void)
{
	int last = _last_displayed();

	if (opt.no_header || opt.json || header_printed)
		return;

	printf("%-10s %-8s", "JOBID", "EVENT");
	for (int i = 0; swatch_attrs[i].name; i++) {
		if (!(_display_mask() & JOB_SUBS_BIT(swatch_attrs[i].attr)))
			continue;
		if (i == last)
			printf(" %s", swatch_attrs[i].heading);
		else
			printf(" %-*s", swatch_attrs[i].width,
			       swatch_attrs[i].heading);
	}
	printf("\n");
	fflush(stdout);

	header_printed = true;
}

static const char *_event_name(uint16_t msg_type)
{
	switch (msg_type) {
	case MESSAGE_JOB_SNAPSHOT:
		return "snapshot";
	case MESSAGE_JOB_UPDATE:
		return "update";
	case MESSAGE_JOB_DELETE:
		return "delete";
	default:
		return "unknown";
	}
}

/*
 * Render one attribute's value into buf.
 *
 * An update only carries the attributes that changed, so a column the
 * event says nothing about prints as "-" rather than as a stale or zero
 * value that would read as news.
 */
static void _format_attr(const job_subs_event_msg_t *event,
			 job_subs_attr_t attr, char *buf, size_t size)
{
	if (!(event->attr_mask & JOB_SUBS_BIT(attr))) {
		snprintf(buf, size, "-");
		return;
	}

	switch (attr) {
	case JOB_SUBS_ATTR_STATE:
		snprintf(buf, size, "%s", job_state_string(event->job_state));
		break;
	case JOB_SUBS_ATTR_PRIORITY:
		snprintf(buf, size, "%u", event->priority);
		break;
	case JOB_SUBS_ATTR_PARTITION:
		snprintf(buf, size, "%s",
			 event->partition ? event->partition : "(null)");
		break;
	case JOB_SUBS_ATTR_NODES:
		snprintf(buf, size, "%s",
			 (event->nodes && event->nodes[0]) ? event->nodes :
			 "(none)");
		break;
	case JOB_SUBS_ATTR_START_TIME:
	case JOB_SUBS_ATTR_END_TIME: {
		time_t when = (attr == JOB_SUBS_ATTR_START_TIME) ?
			      event->start_time : event->end_time;

		if (!when)
			snprintf(buf, size, "Unknown");
		else
			slurm_make_time_str(&when, buf, size);
		break;
	}
	case JOB_SUBS_ATTR_EXIT_CODE:
		snprintf(buf, size, "%u", event->exit_code);
		break;
	default:
		snprintf(buf, size, "-");
		break;
	}
}

static void _print_event_tabular(uint16_t msg_type,
				 const job_subs_event_msg_t *event)
{
	int last;

	if (!opt.emit_mask && (msg_type == MESSAGE_JOB_SNAPSHOT))
		learned_mask |= event->attr_mask;

	_print_header();

	last = _last_displayed();

	printf("%-10u %-8s", event->job_id, _event_name(msg_type));
	for (int i = 0; swatch_attrs[i].name; i++) {
		char buf[128];

		if (!(_display_mask() & JOB_SUBS_BIT(swatch_attrs[i].attr)))
			continue;

		_format_attr(event, swatch_attrs[i].attr, buf, sizeof(buf));
		if (i == last)
			printf(" %s", buf);
		else
			printf(" %-*s", swatch_attrs[i].width, buf);
	}
	printf("\n");
	fflush(stdout);
}

static void _print_event_json(uint16_t msg_type,
			      const job_subs_event_msg_t *event)
{
	data_t *d = data_set_dict(data_new());
	char *out = NULL;

	data_set_string(data_key_set(d, "event"), _event_name(msg_type));
	data_set_int(data_key_set(d, "job_id"), event->job_id);

	for (int i = 0; swatch_attrs[i].name; i++) {
		job_subs_attr_t attr = swatch_attrs[i].attr;

		/* omit rather than null: the event said nothing about it */
		if (!(event->attr_mask & JOB_SUBS_BIT(attr)))
			continue;

		switch (attr) {
		case JOB_SUBS_ATTR_STATE:
			data_set_string(data_key_set(d, swatch_attrs[i].name),
					job_state_string(event->job_state));
			break;
		case JOB_SUBS_ATTR_PRIORITY:
			data_set_int(data_key_set(d, swatch_attrs[i].name),
				     event->priority);
			break;
		case JOB_SUBS_ATTR_PARTITION:
			data_set_string(data_key_set(d, swatch_attrs[i].name),
					event->partition);
			break;
		case JOB_SUBS_ATTR_NODES:
			data_set_string(data_key_set(d, swatch_attrs[i].name),
					event->nodes);
			break;
		case JOB_SUBS_ATTR_START_TIME:
			data_set_int(data_key_set(d, swatch_attrs[i].name),
				     event->start_time);
			break;
		case JOB_SUBS_ATTR_END_TIME:
			data_set_int(data_key_set(d, swatch_attrs[i].name),
				     event->end_time);
			break;
		case JOB_SUBS_ATTR_EXIT_CODE:
			data_set_int(data_key_set(d, swatch_attrs[i].name),
				     event->exit_code);
			break;
		default:
			break;
		}
	}

	if (serialize_g_data_to_string(&out, NULL, d, MIME_TYPE_JSON,
				       SER_FLAGS_COMPACT)) {
		error("unable to serialize event for job %u", event->job_id);
	} else {
		printf("%s\n", out);
		fflush(stdout);
	}

	xfree(out);
	FREE_NULL_DATA(d);
}

/*
 * Open the subscription and read the assigned query id.
 *
 * RET SLURM_SUCCESS, or an error already reported to the user.
 */
static int _subscribe(void)
{
	slurm_msg_t req, resp;
	job_subscribe_msg_t sub = {
		.query_id = opt.query_id,
		.flags = opt.firehose ? JOB_SUBS_FLAG_FIREHOSE : 0,
		.emit_mask = opt.emit_mask,
		.job_ids = opt.job_ids,
		.job_ids_cnt = opt.job_ids_cnt,
	};
	int rc = SLURM_SUCCESS;

	if (!(conn = slurm_open_controller(0, NULL))) {
		error("unable to contact slurmctld: %m");
		return SLURM_ERROR;
	}

	slurm_msg_t_init(&req);
	req.msg_type = REQUEST_JOB_SUBSCRIBE;
	req.protocol_version = SLURM_PROTOCOL_VERSION;
	req.data = &sub;
	slurm_msg_set_r_uid(&req, slurm_conf.slurm_user_id);

	if (slurm_send_node_msg(conn, &req) < 0) {
		error("unable to send subscribe request: %m");
		return SLURM_ERROR;
	}

	slurm_msg_t_init(&resp);
	if (slurm_receive_msg(conn, &resp, 0)) {
		error("no response to subscribe request: %m");
		return SLURM_ERROR;
	}

	if (resp.msg_type == RESPONSE_JOB_SUBSCRIBE) {
		job_subscribe_response_msg_t *r = resp.data;

		query_id = r->query_id;
		verbose("subscribed with query id %u", query_id);
	} else if (resp.msg_type == RESPONSE_SLURM_RC) {
		return_code_msg_t *r = resp.data;

		error("subscribe failed: %s", slurm_strerror(r->return_code));
		rc = r->return_code;
	} else {
		error("unexpected reply to subscribe request: %s",
		      rpc_num2string(resp.msg_type));
		rc = SLURM_ERROR;
	}

	slurm_free_msg_members(&resp);

	return rc;
}

/*
 * Read and print events until the controller hangs up, --count is
 * reached, or a signal asks us to stop.
 *
 * RET 0 on an orderly exit, 2 on a connection error.
 */
static int _watch(void)
{
	while (!stop_requested) {
		slurm_msg_t msg;

		slurm_msg_t_init(&msg);
		if (slurm_receive_msg(conn, &msg, 0)) {
			if (stop_requested)
				break;
			/*
			 * A subscription is idle whenever nothing is
			 * happening to the jobs it watches, which is the
			 * normal case; only a genuine failure of the
			 * connection ends the stream.
			 */
			if (errno == SLURM_PROTOCOL_SOCKET_IMPL_TIMEOUT)
				continue;
			error("connection to slurmctld lost: %m");
			return 2;
		}

		switch (msg.msg_type) {
		case MESSAGE_JOB_SNAPSHOT:
		case MESSAGE_JOB_UPDATE:
		case MESSAGE_JOB_DELETE:
			if (opt.json)
				_print_event_json(msg.msg_type, msg.data);
			else
				_print_event_tabular(msg.msg_type, msg.data);
			events_seen++;
			break;
		default:
			verbose("ignoring unexpected message %s",
				rpc_num2string(msg.msg_type));
			break;
		}

		slurm_free_msg_members(&msg);

		if (opt.count && (events_seen >= opt.count))
			break;
	}

	return 0;
}

int main(int argc, char **argv)
{
	log_options_t log_opts = LOG_OPTS_STDERR_ONLY;
	int rc;

	log_init("swatch", log_opts, SYSLOG_FACILITY_DAEMON, NULL);
	slurm_init(NULL);

	parse_command_line(argc, argv);

	if (opt.verbose || opt.quiet) {
		log_opts.stderr_level += opt.verbose;
		log_opts.stderr_level -= opt.quiet;
		log_alter(log_opts, SYSLOG_FACILITY_DAEMON, NULL);
	}

	/*
	 * Check for JSON support up front rather than discovering it
	 * per-event, which would leave the stream silently empty.
	 */
	if (opt.json) {
		if (serializer_g_init() ||
		    !resolve_mime_type(MIME_TYPE_JSON, NULL)) {
			error("--json requires a JSON serializer plugin, which this build does not have");
			exit(2);
		}
	}

	xsignal(SIGINT, _on_signal);
	xsignal(SIGTERM, _on_signal);

	if ((rc = _subscribe())) {
		rc = 2;
		goto done;
	}

	/*
	 * The query outlives this process by SubscriptionTimeout, so tell
	 * the user the id they would need to resume it.
	 */
	verbose("resume this subscription with: swatch --query-id=%u",
		query_id);

	rc = _watch();

done:
	FREE_NULL_CONN(conn);

	if (opt.json)
		serializer_g_fini();
	free_opt();

	return rc;
}
