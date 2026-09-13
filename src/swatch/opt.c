/*****************************************************************************\
 *  opt.c - options processing for swatch
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

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#include "slurm/slurm.h"

#include "src/common/log.h"
#include "src/common/proc_args.h"
#include "src/common/read_config.h"
#include "src/common/ref.h"
#include "src/common/slurm_opt.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "src/swatch/opt.h"

/* getopt_long codes for long-only options */
#define OPT_LONG_HELP 0x100
#define OPT_LONG_USAGE 0x101
#define OPT_LONG_AUTOCOMP 0x102
#define OPT_LONG_ATTRS 0x103
#define OPT_LONG_FIREHOSE 0x104
#define OPT_LONG_JSON 0x105
#define OPT_LONG_NOHEADER 0x106
#define OPT_LONG_QUERY_ID 0x107
#define OPT_LONG_COUNT 0x108

/*
 * The attributes a subscription can carry, in the order they are printed.
 * The names double as --attrs keywords, column headings and JSON keys, so
 * a newly tracked attribute becomes selectable and printable by adding one
 * row here.
 */
const swatch_attr_t swatch_attrs[] = {
	{ "state", JOB_SUBS_ATTR_STATE, "STATE", 11 },
	{ "priority", JOB_SUBS_ATTR_PRIORITY, "PRIORITY", 10 },
	{ "partition", JOB_SUBS_ATTR_PARTITION, "PARTITION", 12 },
	{ "nodes", JOB_SUBS_ATTR_NODES, "NODES", 20 },
	{ "start_time", JOB_SUBS_ATTR_START_TIME, "START_TIME", 20 },
	{ "end_time", JOB_SUBS_ATTR_END_TIME, "END_TIME", 20 },
	{ "exit_code", JOB_SUBS_ATTR_EXIT_CODE, "EXIT_CODE", 10 },
	{ NULL, 0, NULL, 0 }
};

swatch_opt_t opt = {
	.query_id = NO_VAL,
};

decl_static_data(help_txt);
decl_static_data(usage_txt);

static void _help(void)
{
	char *txt;
	static_ref_to_cstring(txt, help_txt);
	printf("%s", txt);
	xfree(txt);
}

static void _usage(void)
{
	char *txt;
	static_ref_to_cstring(txt, usage_txt);
	printf("%s", txt);
	xfree(txt);
}

/*
 * Build an emit mask from a comma separated list of attribute names, or
 * the keyword "all".
 *
 * IN list - argument to --attrs
 * RET the mask, or 0 if a name was not recognized (with an error printed)
 */
static job_subs_mask_t _parse_attrs(const char *list)
{
	job_subs_mask_t mask = 0;
	char *dup, *tok, *save_ptr = NULL;

	if (!xstrcasecmp(list, "all"))
		return JOB_SUBS_ALL & ~JOB_SUBS_BIT(JOB_SUBS_ATTR_JOB_ID);

	dup = xstrdup(list);
	for (tok = strtok_r(dup, ",", &save_ptr); tok;
	     tok = strtok_r(NULL, ",", &save_ptr)) {
		int i;

		for (i = 0; swatch_attrs[i].name; i++) {
			if (!xstrcasecmp(tok, swatch_attrs[i].name)) {
				mask |= JOB_SUBS_BIT(swatch_attrs[i].attr);
				break;
			}
		}
		if (!swatch_attrs[i].name) {
			error("--attrs: unknown attribute '%s'", tok);
			mask = 0;
			break;
		}
	}
	xfree(dup);

	return mask;
}

/* Append one job id, or a comma separated list of them, to opt.job_ids. */
static void _add_job_ids_or_die(const char *src)
{
	char *dup = xstrdup(src), *tok, *save_ptr = NULL;

	for (tok = strtok_r(dup, ",", &save_ptr); tok;
	     tok = strtok_r(NULL, ",", &save_ptr)) {
		uint32_t job_id;

		if (parse_uint32((char *) tok, &job_id) || !job_id) {
			error("invalid job id '%s'", tok);
			xfree(dup);
			exit(2);
		}
		xrecalloc(opt.job_ids, opt.job_ids_cnt + 1,
			  sizeof(*opt.job_ids));
		opt.job_ids[opt.job_ids_cnt++] = job_id;
	}
	xfree(dup);
}

extern void parse_command_line(int argc, char **argv)
{
	int opt_char = 0, option_index = 0;
	static struct option long_options[] = {
		{ "attrs", required_argument, 0, OPT_LONG_ATTRS },
		{ "autocomplete", required_argument, 0, OPT_LONG_AUTOCOMP },
		{ "count", required_argument, 0, OPT_LONG_COUNT },
		{ "firehose", no_argument, 0, OPT_LONG_FIREHOSE },
		{ "help", no_argument, 0, OPT_LONG_HELP },
		{ "json", no_argument, 0, OPT_LONG_JSON },
		{ "noheader", no_argument, 0, OPT_LONG_NOHEADER },
		{ "query-id", required_argument, 0, OPT_LONG_QUERY_ID },
		{ "quiet", no_argument, 0, 'Q' },
		{ "usage", no_argument, 0, OPT_LONG_USAGE },
		{ "verbose", no_argument, 0, 'v' },
		{ "version", no_argument, 0, 'V' },
		{ NULL, 0, 0, 0 }
	};

	optind = 0;
	while ((opt_char = getopt_long(argc, argv, "hQvV", long_options,
				       &option_index)) != -1) {
		switch (opt_char) {
		case 'h':
		case OPT_LONG_HELP:
			_help();
			exit(0);
		case 'Q':
			opt.quiet = true;
			break;
		case 'v':
			opt.verbose++;
			break;
		case 'V':
			print_slurm_version();
			exit(0);
		case OPT_LONG_USAGE:
			_usage();
			exit(0);
		case OPT_LONG_AUTOCOMP:
			suggest_completion(long_options, optarg);
			exit(0);
		case OPT_LONG_ATTRS:
			if (!(opt.emit_mask = _parse_attrs(optarg)))
				exit(2);
			break;
		case OPT_LONG_FIREHOSE:
			opt.firehose = true;
			break;
		case OPT_LONG_JSON:
			opt.json = true;
			break;
		case OPT_LONG_NOHEADER:
			opt.no_header = true;
			break;
		case OPT_LONG_COUNT:
			if (!optarg || parse_uint32(optarg, &opt.count) ||
			    !opt.count) {
				error("--count: invalid value '%s' (must be a positive integer)",
				      optarg ? optarg : "");
				exit(2);
			}
			break;
		case OPT_LONG_QUERY_ID:
			if (!optarg || parse_uint32(optarg, &opt.query_id) ||
			    (opt.query_id == NO_VAL)) {
				error("--query-id: invalid value '%s'",
				      optarg ? optarg : "");
				exit(2);
			}
			break;
		default:
			info("Try \"swatch --help\" for more information");
			exit(2);
		}
	}

	if (opt.quiet && opt.verbose) {
		error("--verbose (-v) and --quiet (-Q) are mutually exclusive");
		exit(2);
	}

	for (int i = optind; i < argc; i++)
		_add_job_ids_or_die(argv[i]);

	/*
	 * A reattach names an existing subscription, whose filter and emit
	 * set already live in the controller; accepting either here would
	 * silently ignore it.
	 */
	if (opt.query_id != NO_VAL) {
		if (opt.job_ids_cnt || opt.firehose || opt.emit_mask) {
			error("--query-id resumes an existing subscription; job ids, --firehose and --attrs cannot be given with it");
			exit(2);
		}
	} else if (opt.firehose) {
		if (opt.job_ids_cnt) {
			error("--firehose watches every job; job ids cannot be given with it");
			exit(2);
		}
		if (opt.emit_mask) {
			error("--firehose always reports every attribute; --attrs cannot be given with it");
			exit(2);
		}
		opt.emit_mask = JOB_SUBS_ALL &
				~JOB_SUBS_BIT(JOB_SUBS_ATTR_JOB_ID);
	} else {
		if (!opt.job_ids_cnt) {
			char *env = getenv("SLURM_JOB_ID");

			if (env && *env)
				_add_job_ids_or_die(env);
		}
		if (!opt.job_ids_cnt) {
			error("no job ids given and SLURM_JOB_ID is not set");
			info("Try \"swatch --help\" for more information");
			exit(2);
		}
		if (!opt.emit_mask)
			opt.emit_mask =
				JOB_SUBS_BIT(JOB_SUBS_ATTR_STATE) |
				JOB_SUBS_BIT(JOB_SUBS_ATTR_PRIORITY) |
				JOB_SUBS_BIT(JOB_SUBS_ATTR_NODES);
	}
}

extern void free_opt(void)
{
	xfree(opt.job_ids);
}
