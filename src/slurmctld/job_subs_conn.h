/*****************************************************************************\
 *  job_subs_conn.h - subscriber connection handling for job subscriptions
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

#ifndef _SLURMCTLD_JOB_SUBS_CONN_H
#define _SLURMCTLD_JOB_SUBS_CONN_H

#include "src/common/slurm_protocol_defs.h"

/*
 * Set up the subscriber connection table and install the delivery hook
 * that routes flush-pass events onto per-connection outbound queues.
 */
extern void job_subs_conn_init(void);

/* Stop every delivery thread and drop the connection table. */
extern void job_subs_conn_fini(void);

/*
 * Adopt the (extracted) connection of a REQUEST_JOB_SUBSCRIBE message for
 * the given query: send RESPONSE_JOB_SUBSCRIBE on it, then keep it open
 * with a dedicated delivery thread draining the query's outbound queue.
 * A previous connection serving the same query is shut down first.
 *
 * Takes ownership of msg (and its connection) in every case. Returns
 * SLURM_SUCCESS once the connection is registered.
 */
extern int job_subs_conn_attach(slurm_msg_t *msg, uint32_t query_id);

/* Number of live subscriber connections. */
extern int job_subs_conn_count(void);

#endif /* _SLURMCTLD_JOB_SUBS_CONN_H */
