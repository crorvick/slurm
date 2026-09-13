/*****************************************************************************\
 *  opt.h - definitions for swatch option processing
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

#ifndef _SWATCH_OPT_H
#define _SWATCH_OPT_H

#include <stdbool.h>
#include <stdint.h>

#include "src/common/job_record.h"

/*
 * One trackable attribute, as the user names it and as it is printed.
 * Adding a row makes a newly tracked attribute both selectable through
 * --attrs and printable in either output format.
 */
typedef struct {
	const char *name;	/* --attrs keyword and JSON key */
	job_subs_attr_t attr;
	const char *heading;	/* column heading */
	int width;		/* column width */
} swatch_attr_t;

/* Printing order; terminated by a NULL name. */
extern const swatch_attr_t swatch_attrs[];

typedef struct {
	uint32_t *job_ids;	/* job ids to watch */
	uint32_t job_ids_cnt;	/* count of job_ids[] */
	bool firehose;		/* --firehose: every job, privileged */
	uint32_t query_id;	/* --query-id to reattach, else NO_VAL */
	job_subs_mask_t emit_mask; /* attributes selected by --attrs */
	bool json;		/* --json: one JSON object per line */
	bool no_header;		/* --noheader */
	uint32_t count;		/* --count: exit after this many events */
	bool quiet;
	int verbose;
} swatch_opt_t;

extern swatch_opt_t opt;

/* Parse the command line into opt, exiting on error. */
extern void parse_command_line(int argc, char **argv);

/* Release what parse_command_line() allocated. */
extern void free_opt(void);

#endif /* _SWATCH_OPT_H */
