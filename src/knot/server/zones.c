/*  Copyright (C) 2011 CZ.NIC, z.s.p.o. <knot-dns@labs.nic.cz>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <config.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>

#include "common/descriptor.h"
#include "common/lists.h"
#include "common/log.h"
#include "knot/conf/conf.h"
#include "knot/other/debug.h"
#include "knot/server/server.h"
#include "knot/server/xfr-handler.h"
#include "knot/server/zone-load.h"
#include "knot/server/zones.h"
#include "knot/zone/zone-dump.h"
#include "libknot/dname.h"
#include "libknot/dnssec/random.h"
#include "knot/dnssec/zone-events.h"
#include "knot/dnssec/zone-sign.h"
#include "knot/nameserver/chaos.h"
#include "libknot/rdata.h"
#include "libknot/tsig-op.h"
#include "knot/updates/changesets.h"
#include "knot/updates/ddns.h"
#include "knot/updates/xfr-in.h"
#include "libknot/packet/wire.h"
#include "knot/zone/zone-contents.h"
#include "knot/zone/zone-diff.h"
#include "knot/zone/zone.h"
#include "knot/zone/zonedb.h"

/* Forward declarations. */
static int zones_dump_zone_text(knot_zone_contents_t *zone,  const char *zf);

/*!
 * \brief Apply jitter to time interval.
 *
 * Amount of jitter is specified by ZONES_JITTER_PCT.
 *
 * \param interval base value.
 * \return interval value minus rand(0, ZONES_JITTER_PCT) %
 */
static uint32_t zones_jitter(uint32_t interval)
{
	return (interval * (100 - (knot_random_uint32_t() % ZONES_JITTER_PCT))) / 100;
}

/*!
 * \brief Return SOA timer value.
 *
 * \param zone Pointer to zone.
 * \param rr_func RDATA specificator.
 * \return Timer in miliseconds.
 */
static uint32_t zones_soa_timer(zone_t *zone, uint32_t (*rr_func)(const knot_rrset_t*))
{
	if (!zone) {
		dbg_zones_verb("zones: zones_soa_timer() called "
		               "with NULL zone\n");
	}

	uint32_t ret = 0;

	/* Retrieve SOA RDATA. */
	const knot_rrset_t *soa_rrs = 0;

	rcu_read_lock();

	knot_zone_contents_t * zc = zone->contents;
	if (!zc) {
		rcu_read_unlock();
		return 0;
	}

	soa_rrs = knot_node_rrset(zc->apex, KNOT_RRTYPE_SOA);
	assert(soa_rrs != NULL);
	ret = rr_func(soa_rrs);

	rcu_read_unlock();

	/* Convert to miliseconds. */
	return ret * 1000;
}

/*!
 * \brief Return SOA REFRESH timer value.
 *
 * \param zone Pointer to zone.
 * \return REFRESH timer in miliseconds.
 */
static uint32_t zones_soa_refresh(zone_t *zone)
{
	return zones_soa_timer(zone, knot_rdata_soa_refresh);
}

/*!
 * \brief Return SOA RETRY timer value.
 *
 * \param zone Pointer to zone.
 * \return RETRY timer in miliseconds.
 */
static uint32_t zones_soa_retry(zone_t *zone)
{
	return zones_soa_timer(zone, knot_rdata_soa_retry);
}

/*!
 * \brief Return SOA EXPIRE timer value.
 *
 * \param zone Pointer to zone.
 * \return EXPIRE timer in miliseconds.
 */
static uint32_t zones_soa_expire(zone_t *zone)
{
	return zones_soa_timer(zone, knot_rdata_soa_expire);
}

/*!
 * \brief XFR/IN expire event handler.
 */
int zones_expire_ev(event_t *e)
{
	assert(e);

	dbg_zones("zones: EXPIRE timer event\n");
	if (e->data == NULL) {
		return KNOT_EINVAL;
	}

	rcu_read_lock();
	zone_t *zone = (zone_t *)e->data;

	/* Check if zone is not discarded. */
	if (zone->flags & ZONE_DISCARDED) {
		rcu_read_unlock();
		return KNOT_EOK;
	}

	zone_retain(zone); /* Keep a reference. */
	rcu_read_unlock();

	/* Mark the zone as expired. This will remove the zone contents. */
	knot_zone_contents_t *contents = knot_zonedb_expire_zone(
			zone->server->nameserver->zone_db, zone->name);

	/* Early finish this event to prevent lockup during cancellation. */
	dbg_zones("zones: zone expired, removing from database\n");
	evsched_event_finished(e->parent);

	/* Publish expired zone, must be after evsched_event_finished.
	 * This is because some other thread may hold rcu_read_lock and
	 * wait for event cancellation. */
	synchronize_rcu();

	/* Log event. */
	log_zone_info("Zone '%s' expired.\n", zone->conf->name);

	/* Cancel REFRESH timer. */
	if (zone->xfr_in.timer) {
		evsched_cancel(e->parent, zone->xfr_in.timer);
	}

	knot_zone_contents_deep_free(&contents);

	/* Release holding reference. */
	zone_release(zone);
	return KNOT_EOK;
}

/*!
 * \brief Zone REFRESH or RETRY event.
 */
int zones_refresh_ev(event_t *e)
{
	assert(e);

	dbg_zones("zones: REFRESH or RETRY timer event\n");
	rcu_read_lock();
	zone_t *zone = (zone_t *)e->data;
	if (zone == NULL) {
		rcu_read_unlock();
		return KNOT_EINVAL;
	}

	if (zone->flags & ZONE_DISCARDED) {
		rcu_read_unlock();
		return KNOT_EOK;
	}

	/* Create XFR request. */
	knot_ns_xfr_t *rq = xfr_task_create(zone, XFR_TYPE_SOA, XFR_FLAG_TCP);
	rcu_read_unlock(); /* rq now holds a reference to zone */
	if (!rq) {
		return KNOT_EINVAL;
	}
	xfr_task_setaddr(rq, &zone->xfr_in.master, &zone->xfr_in.via);
	if (zone->xfr_in.tsig_key.name) {
		rq->tsig_key = &zone->xfr_in.tsig_key;
	}

	/* Check for contents. */
	int ret = KNOT_EOK;
	if (!zone->contents) {

		/* Bootstrap over TCP. */
		rq->type = XFR_TYPE_AIN;
		rq->flags = XFR_FLAG_TCP;
		evsched_event_finished(e->parent);

		/* Check transfer state. */
		pthread_mutex_lock(&zone->lock);
		if (zone->xfr_in.state == XFR_PENDING) {
			pthread_mutex_unlock(&zone->lock);
			xfr_task_free(rq);
			return KNOT_EOK;
		} else {
			zone->xfr_in.state = XFR_PENDING;
		}

		/* Issue request. */
		ret = xfr_enqueue(zone->server->xfr, rq);
		if (ret != KNOT_EOK) {
			xfr_task_free(rq);
			zone->xfr_in.state = XFR_SCHED; /* Revert state. */
		}
		pthread_mutex_unlock(&zone->lock);
		return ret;

	}

	/* Schedule EXPIRE timer on first attempt. */
	if (!zone->xfr_in.expire) {
		uint32_t expire_tmr = zones_jitter(zones_soa_expire(zone));
		// Allow for timeouts.  Otherwise zones with very short
		// expiry may expire before the timeout is reached.
		expire_tmr += 2 * (conf()->max_conn_idle * 1000);
		zone->xfr_in.expire = evsched_schedule_cb(
					      e->parent,
					      zones_expire_ev,
					      zone, expire_tmr);
		dbg_zones("zones: EXPIRE of '%s' after %u seconds\n",
		          zone->conf->name, expire_tmr / 1000);
	}

	/* Reschedule as RETRY timer. */
	uint32_t retry_tmr = zones_jitter(zones_soa_retry(zone));
	evsched_schedule(e->parent, e, retry_tmr);
	dbg_zones("zones: RETRY of '%s' after %u seconds\n",
	          zone->conf->name, retry_tmr / 1000);

	/* Issue request. */
	evsched_event_finished(e->parent);
	ret = xfr_enqueue(zone->server->xfr, rq);
	if (ret != KNOT_EOK) {
		xfr_task_free(rq);
	}

	return ret;
}

/*! \brief Function for marking nodes as synced and updated. */
static int zones_ixfrdb_sync_apply(journal_t *j, journal_node_t *n)
{
	assert(j);
	assert(n);

	/* Check for dirty bit (not synced to permanent storage). */
	if (n->flags & JOURNAL_DIRTY) {

		/* Remove dirty bit. */
		n->flags = n->flags & ~JOURNAL_DIRTY;

		/* Sync. */
		journal_update(j, n);
	}

	return KNOT_EOK;
}

static bool zones_changesets_empty(const knot_changesets_t *chs)
{
	if (chs == NULL) {
		return true;
	}

	if (EMPTY_LIST(chs->sets)) {
		return true;
	}

	return knot_changeset_is_empty(HEAD(chs->sets));
}

static int zones_store_chgsets_try_store(zone_t *zone,
                                         knot_changesets_t *chgsets,
                                         journal_t **transaction)
{
	assert(zone);
	assert(chgsets);
	assert(transaction);

	*transaction = zones_store_changesets_begin(zone);
	if (*transaction == NULL) {
		dbg_zones("Could not start journal operation.\n");
		return KNOT_ERROR;
	}

	int ret = zones_store_changesets(zone, chgsets, *transaction);

	/* In any case, rollback the transaction. */
	if (ret != KNOT_EOK) {
		zones_store_changesets_rollback(*transaction);
		*transaction = NULL;
		dbg_zones("Could not store in the journal. Reason: %s.\n",
		          knot_strerror(ret));
		return ret;
	}

	return KNOT_EOK;
}

static int zones_zonefile_sync_from_ev(zone_t *zone)
{
	assert(zone);

	/* Only on zones with valid contents (non empty). */
	int ret = KNOT_EOK;
	if (zone->contents && journal_is_used(zone->ixfr_db)) {
		/* Synchronize journal. */
		ret = journal_retain(zone->ixfr_db);
		if (ret == KNOT_EOK) {
			ret = zones_zonefile_sync(zone, zone->ixfr_db);
			journal_release(zone->ixfr_db);
		}

		rcu_read_lock();
		if (ret == KNOT_EOK) {
			log_zone_info("Applied differences of '%s' to zonefile.\n",
			              zone->conf->name);
		} else if (ret != KNOT_ERANGE) {
			log_zone_warning("Failed to apply differences of '%s' "
			                 "to zonefile (%s).\n", zone->conf->name,
			                 knot_strerror(ret));
		}
		rcu_read_unlock();
	}

	return ret;
}

/*!
 * \brief Sync chagnes in zone to zonefile.
 */
int zones_flush_ev(event_t *e)
{
	assert(e);
	dbg_zones("zones: IXFR database SYNC timer event\n");

	/* Fetch zone. */
	zone_t *zone = (zone_t *)e->data;
	if (!zone) {
		return KNOT_EINVAL;
	}

	int ret = zones_zonefile_sync_from_ev(zone);

	/* Reschedule. */
	rcu_read_lock();
	int next_timeout = zone->conf->dbsync_timeout * 1000;
	if (next_timeout > 0) {
		dbg_zones("%s: next zonefile sync of '%s' in %d seconds\n",
		          __func__, zone->conf->name, next_timeout / 1000);
		evsched_schedule(e->parent, e, next_timeout);
	}
	rcu_read_unlock();
	return ret;
}

static int zones_store_changesets_begin_and_store(zone_t *zone,
                                                  knot_changesets_t *chgsets,
                                                  journal_t **transaction)
{
	assert(zone != NULL);
	assert(chgsets != NULL);

	if (zones_changesets_empty(chgsets)) {
		return KNOT_EINVAL;
	}

	int ret = zones_store_chgsets_try_store(zone, chgsets, transaction);

	/* If the journal was full (KNOT_EBUSY), we must flush it by hand and
	 * try to save the changesets once again. If this fails, the changesets
	 * are larger than max journal size, so return error.
	 */
	if (ret == KNOT_EBUSY) {
		log_zone_notice("Journal for '%s' is full, flushing.\n",
		                zone->conf->name);
		/* Don't worry about sync event. It can't happen while this
		 * event (signing) is not finished. We may thus do the sync
		 * by hand and leave the planned one there to be executed
		 * later. */

		assert(*transaction == NULL);

		/* Transaction rolled back, journal released, we may flush. */
		ret = zones_zonefile_sync_from_ev(zone);
		if (ret != KNOT_EOK) {
			log_zone_error("Failed to sync journal to zone file.\n");
			return ret;
		}

		/* Begin the transaction anew. */
		ret = zones_store_chgsets_try_store(zone, chgsets, transaction);
	}

	return ret;
}

/*----------------------------------------------------------------------------*/

/*! \brief Return 'serial_from' part of the key. */
static inline uint32_t ixfrdb_key_from(uint64_t k)
{
	/*      64    32       0
	 * key = [TO   |   FROM]
	 * Need: Least significant 32 bits.
	 */
	return (uint32_t)(k & ((uint64_t)0x00000000ffffffff));
}

/*----------------------------------------------------------------------------*/

/*! \brief Return 'serial_to' part of the key. */
static inline uint32_t ixfrdb_key_to(uint64_t k)
{
	/*      64    32       0
	 * key = [TO   |   FROM]
	 * Need: Most significant 32 bits.
	 */
	return (uint32_t)(k >> (uint64_t)32);
}

/*----------------------------------------------------------------------------*/

/*! \brief Compare function to match entries with target serial. */
static inline int ixfrdb_key_to_cmp(uint64_t k, uint64_t to)
{
	/*      64    32       0
	 * key = [TO   |   FROM]
	 * Need: Most significant 32 bits.
	 */
	return ((uint64_t)ixfrdb_key_to(k)) - to;
}

/*----------------------------------------------------------------------------*/

/*! \brief Compare function to match entries with starting serial. */
static inline int ixfrdb_key_from_cmp(uint64_t k, uint64_t from)
{
	/*      64    32       0
	 * key = [TO   |   FROM]
	 * Need: Least significant 32 bits.
	 */
	return ((uint64_t)ixfrdb_key_from(k)) - from;
}

/*----------------------------------------------------------------------------*/

/*! \brief Make key for journal from serials. */
static inline uint64_t ixfrdb_key_make(uint32_t from, uint32_t to)
{
	/*      64    32       0
	 * key = [TO   |   FROM]
	 */
	return (((uint64_t)to) << ((uint64_t)32)) | ((uint64_t)from);
}

/*----------------------------------------------------------------------------*/

int zones_changesets_from_binary(knot_changesets_t *chgsets)
{
	/*! \todo #1291 Why doesn't this just increment stream ptr? */

	assert(chgsets != NULL);
	/*
	 * Parses changesets from the binary format stored in chgsets->data
	 * into the changeset_t structures.
	 */
	knot_rrset_t *rrset = 0;
	int ret = 0;

	knot_changeset_t* chs = NULL;
	WALK_LIST(chs, chgsets->sets) {
		/* Read changeset flags. */
		if (chs->data == NULL) {
			return KNOT_EMALF;
		}
		size_t remaining = chs->size;
		memcpy(&chs->flags, chs->data, sizeof(uint32_t));
		remaining -= sizeof(uint32_t);

		/* Read initial changeset RRSet - SOA. */
		uint8_t *stream = chs->data + (chs->size - remaining);
		ret = rrset_deserialize(stream, &remaining, &rrset);
		if (ret != KNOT_EOK) {
			dbg_xfr("xfr: SOA: failed to deserialize data "
			        "from changeset, %s\n", knot_strerror(ret));
			return KNOT_EMALF;
		}

		/* in this special case (changesets loaded
		 * from journal) the SOA serial should already
		 * be set, check it.
		 */
		dbg_xfr_verb("xfr: reading RRSets to REMOVE, first RR is %hu\n",
		             knot_rrset_type(rrset));
		assert(knot_rrset_type(rrset) == KNOT_RRTYPE_SOA);
		assert(chs->serial_from == knot_rdata_soa_serial(rrset));
		knot_changeset_add_soa(chs, rrset, KNOT_CHANGESET_REMOVE);

		/* Read remaining RRSets */
		int in_remove_section = 1;
		while (remaining > 0) {

			/* Parse next RRSet. */
			rrset = 0;
			stream = chs->data + (chs->size - remaining);
			ret = rrset_deserialize(stream, &remaining, &rrset);
			if (ret != KNOT_EOK) {
				dbg_xfr("xfr: failed to deserialize data "
				        "from changeset, %s\n",
				        knot_strerror(ret));
				return KNOT_EMALF;
			}

			/* Check for next SOA. */
			if (knot_rrset_type(rrset) == KNOT_RRTYPE_SOA) {

				/* Move to ADD section if in REMOVE. */
				if (in_remove_section) {
					knot_changeset_add_soa(chs, rrset,
					                       KNOT_CHANGESET_ADD);
					dbg_xfr_verb("xfr: reading RRSets"
					             " to ADD\n");
					in_remove_section = 0;
				} else {
					/* Final SOA. */
					dbg_xfr_verb("xfr: extra SOA\n");
					knot_rrset_deep_free(&rrset, 1);
					break;
				}
			} else {
				/* Remove RRSets. */
				if (in_remove_section) {
					ret = knot_changeset_add_rrset(
						chs, rrset,
						KNOT_CHANGESET_REMOVE);
				} else {
				/* Add RRSets. */
					ret = knot_changeset_add_rrset(
						chs, rrset,
						KNOT_CHANGESET_ADD);
				}

				/* Check result. */
				if (ret != KNOT_EOK) {
					dbg_xfr("xfr: failed to add/remove "
					        "RRSet to changeset: %s\n",
					        knot_strerror(ret));
					return KNOT_ERROR;
				}
			}
		}

		dbg_xfr_verb("xfr: read all RRSets in changeset\n");
	}

	return KNOT_EOK;
}

/*----------------------------------------------------------------------------*/

int zones_load_changesets(const zone_t *zone, knot_changesets_t *dst,
                          uint32_t from, uint32_t to)
{
	if (!zone || !dst) {
		dbg_zones_detail("Bad arguments: zone=%p, dst=%p\n", zone, dst);
		return KNOT_EINVAL;
	}

	/* Fetch zone-specific data. */
	if (!zone->ixfr_db) {
		dbg_zones_detail("Bad arguments: zone->ixfr_db=%p\n", zone->ixfr_db);
		return KNOT_EINVAL;
	}

	/* Check journal file existence. */
	if (!journal_is_used(zone->ixfr_db)) {
		return KNOT_ERANGE; /* Not used, no changesets available. */
	}

	/* Retain journal for changeset loading. */
	int ret = journal_retain(zone->ixfr_db);
	if (ret != KNOT_EOK) {
		return ret;
	}

	/* Read entries from starting serial until finished. */
	uint32_t found_to = from;
	journal_node_t *n = 0;
	ret = journal_fetch(zone->ixfr_db, from, ixfrdb_key_from_cmp, &n);
	if (ret != KNOT_EOK) {
		dbg_xfr("xfr: failed to fetch starting changeset: %s\n",
		        knot_strerror(ret));
		journal_release(zone->ixfr_db);
		return ret;
	}

	while (n != 0 && n != journal_end(zone->ixfr_db)) {

		/* Check for history end. */
		if (to == found_to) {
			break;
		}

		knot_changeset_t *chs = knot_changesets_create_changeset(dst);
		if (chs == NULL) {
			dbg_xfr("xfr: failed to create changeset: %s\n",
			        knot_strerror(ret));
			journal_release(zone->ixfr_db);
			return KNOT_ERROR;
		}

		/* Skip wrong changesets. */
		if (!(n->flags & JOURNAL_VALID) || n->flags & JOURNAL_TRANS) {
			++n;
			continue;
		}

		/* Initialize changeset. */
		dbg_xfr_detail("xfr: reading entry #%zu id=%llu\n",
		               dst->count, (unsigned long long)n->id);
		chs->serial_from = ixfrdb_key_from(n->id);
		chs->serial_to = ixfrdb_key_to(n->id);
		chs->data = malloc(n->len);
		if (!chs->data) {
			journal_release(zone->ixfr_db);
			return KNOT_ENOMEM;
		}

		/* Read journal entry. */
		ret = journal_read_node(zone->ixfr_db, n, (char*)chs->data);
		if (ret != KNOT_EOK) {
			dbg_xfr("xfr: failed to read data from journal\n");
			free(chs->data);
			journal_release(zone->ixfr_db);
			return KNOT_ERROR;
		}

		/* Update changeset binary size. */
		chs->size = n->len;

		/* Next node. */
		found_to = chs->serial_to;
		++n;

		/*! \todo Check consistency. */
	}

	dbg_xfr_detail("xfr: finished reading journal entries\n");
	journal_release(zone->ixfr_db);

	/* Unpack binary data. */
	int unpack_ret = zones_changesets_from_binary(dst);
	if (unpack_ret != KNOT_EOK) {
		dbg_xfr("xfr: failed to unpack changesets "
		        "from binary, %s\n", knot_strerror(unpack_ret));
		return unpack_ret;
	}

	/* Check for complete history. */
	if (to != found_to) {
		dbg_xfr_detail("xfr: load changesets finished, ERANGE\n");
		return KNOT_ERANGE;
	}

	/* History reconstructed. */
	dbg_xfr_detail("xfr: load changesets finished, EOK\n");
	return KNOT_EOK;
}

static void zones_free_merged_changesets(knot_changesets_t *diff_chs,
                                         knot_changesets_t *sec_chs)
{
	/*!
	 * Merged changesets freeing can be quite complicated, since there
	 * are several cases to handle. (NULL and empty changesets)
	 */
	if (diff_chs == NULL &&
	    sec_chs == NULL) {
		return;
	} else if (diff_chs == NULL &&
	           sec_chs != NULL) {
		knot_changesets_free(&sec_chs);
	} else if (sec_chs == NULL &&
	           diff_chs != NULL) {
		knot_changesets_free(&diff_chs);
	} else {
		/*!
		 * Merged changesets, deep free 'diff_chs',
		 * shallow free 'sec_chs', unless one of them is empty.
		 */
		if (zones_changesets_empty(sec_chs)
		    || zones_changesets_empty(diff_chs)) {
			if (knot_changesets_get_last(diff_chs)->soa_to) {
				knot_changesets_get_last(diff_chs)->soa_to = NULL;
			}
			knot_changesets_free(&sec_chs);
			knot_changesets_free(&diff_chs);
		} else {
			/*!
			 * Ending SOA from the merged changeset was used in
			 * zone (same as in DNSSEC changeset). It must not
			 * be freed.
			 */
			assert(knot_changesets_get_last(diff_chs)->serial_to ==
			       knot_changesets_get_last(sec_chs)->serial_to);
			knot_changesets_get_last(diff_chs)->soa_to = NULL;
			knot_changesets_free(&diff_chs);

			/*!
			 * From SOAs from the second changeset was not used,
			 * it must be freed.
			 */
			knot_rrset_deep_free(
			  &(knot_changesets_get_last(sec_chs)->soa_from), 1);

			// Reset sec_chs' chngeset list, else we'd double free.
			init_list(&sec_chs->sets);
			knot_changesets_free(&sec_chs);
		}
	}
}

static int zones_merge_and_store_changesets(zone_t *zone,
                                            knot_changesets_t *diff_chs,
                                            knot_changesets_t *sec_chs,
                                            journal_t **transaction)
{
	assert(zone);
	assert(transaction);

	if (zones_changesets_empty(diff_chs) &&
	    zones_changesets_empty(sec_chs)) {
		return KNOT_EOK;
	}
	if (!zones_changesets_empty(diff_chs) &&
	    zones_changesets_empty(sec_chs)) {
		return zones_store_changesets_begin_and_store(zone, diff_chs,
		                                              transaction);
	}
	if (zones_changesets_empty(diff_chs) &&
	    !zones_changesets_empty(sec_chs)) {
		return zones_store_changesets_begin_and_store(zone, sec_chs,
		                                              transaction);
	}

	knot_changeset_t *diff_ch = knot_changesets_get_last(diff_chs);
	knot_changeset_t *sec_ch =  knot_changesets_get_last(sec_chs);

	/*!
	 * Beginning SOA of second changeset should be equal to ending SOA
	 * of the first changeset.
	 */
	assert(diff_ch->serial_to == sec_ch->serial_from);

	int ret = knot_changeset_merge(diff_ch, sec_ch);
	if (ret != KNOT_EOK) {
		return ret;
	}

	/*!
	 * Now the ending serial of first changeset (the merged one) should be
	 * equal to the ending serial of second changeset. Also the SOAs should
	 * be the same.
	 */
	assert(diff_ch->serial_to == sec_ch->serial_to);
	assert(diff_ch->soa_to == sec_ch->soa_to);

	// Store *ALL* changes to disk.
	ret = zones_store_changesets_begin_and_store(zone, diff_chs,
	                                             transaction);
	if (ret != KNOT_EOK) {
		log_zone_error("Could not store changesets to journal (%s)!",
		               knot_strerror(ret));
		return ret;
	}

	return KNOT_EOK;
}

static int zones_serial_policy(const zone_t *zone)
{
	assert(zone != NULL);

	return zone->conf->serial_policy;
}

static uint32_t zones_next_serial(zone_t *zone)
{
	assert(zone);

	uint32_t old_serial = knot_zone_serial(zone->contents);
	uint32_t new_serial;

	switch (zones_serial_policy(zone)) {
	case CONF_SERIAL_INCREMENT:
		new_serial = (uint32_t)old_serial + 1;
		break;
	case CONF_SERIAL_UNIXTIME:
		new_serial = (uint32_t)time(NULL);
		break;
	default:
		assert(0);
	}

	/* If the new serial is 'lower' or equal than the new one, warn the user.*/
	if (ns_serial_compare(old_serial, new_serial) >= 0) {
		log_zone_warning("New serial will be lower than "
		                 "the current one. Old: %"PRIu32" "
		                 "new: %"PRIu32".\n",
		                 old_serial, new_serial);
	}

	return new_serial;
}

static int replan_zone_sign_after_ddns(zone_t *zone, uint32_t refresh_at)
{
	assert(zone);

	if (zone->dnssec_timer->tv.tv_sec <= refresh_at) {
		return KNOT_EOK;
	}

	zones_cancel_dnssec(zone);
	return zones_schedule_dnssec(zone, refresh_at);
}

static bool apex_rr_changed(const knot_zone_contents_t *old_contents,
                            const knot_zone_contents_t *new_contents,
                            uint16_t type)
{
	const knot_rrset_t *old_rr = knot_node_rrset(old_contents->apex, type);
	const knot_rrset_t *new_rr = knot_node_rrset(new_contents->apex, type);
	if (old_rr== NULL) {
		return new_rr != NULL;
	} else if (new_rr == NULL) {
		return old_rr != NULL;
	}
	return !knot_rrset_equal(old_rr, new_rr, KNOT_RRSET_COMPARE_WHOLE);
}

static bool zones_dnskey_changed(const knot_zone_contents_t *old_contents,
                                 const knot_zone_contents_t *new_contents)
{
	return apex_rr_changed(old_contents, new_contents, KNOT_RRTYPE_DNSKEY);
}

static bool zones_nsec3param_changed(const knot_zone_contents_t *old_contents,
                                     const knot_zone_contents_t *new_contents)
{
	return apex_rr_changed(old_contents, new_contents, KNOT_RRTYPE_NSEC3PARAM);
}

/*!
 * \todo Just a rewrite of existing code, I don't know what is the purpose of this.
 */
static zone_t *create_fake_zone(zone_t *zone)
{
	conf_zone_t *conf = malloc(sizeof(conf_zone_t));
	if (!conf) {
		return NULL;
	}

	conf_init_zone(conf);
	conf->name = strdup(zone->conf->name);

	zone_t *fake = zone_new(conf);

	// steal the zone content
	fake->contents = zone->contents;
	fake->contents->zone = fake;

	return fake;
}

/*! \brief Process UPDATE query.
 *
 * Functions expects that the query is already authenticated
 * and TSIG signature is verified.
 *
 * \note Set parameter 'rcode' according to answering procedure.
 * \note Function expects RCU to be locked.
 *
 * \retval KNOT_EOK if successful.
 * \retval error if not.
 */
int zones_process_update_auth(zone_t *zone, knot_pkt_t *query,
                              knot_rcode_t *rcode, const sockaddr_t *addr,
                              knot_tsig_key_t *tsig_key)
{
	assert(zone);
	assert(query);
	assert(rcode);
	assert(addr);

	int ret = KNOT_EOK;
	dbg_zones_verb("TSIG check successful. Answering query.\n");

	/* Create log message prefix. */
	char *keytag = NULL;
	if (tsig_key) {
		keytag = knot_dname_to_str(tsig_key->name);
	}
	char *r_str = xfr_remote_str(addr, keytag);
	char *msg  = sprintf_alloc("UPDATE of '%s' from %s",
	                           zone->conf->name, r_str ? r_str : "'unknown'");
	free(r_str);
	free(keytag);
	log_zone_info("%s Started.\n", msg);

	/*!
	 * We must prepare a changesets_t structure even though there will
	 * be only one changeset - because of the API.
	 */
	knot_changesets_t *chgsets = knot_changesets_create();
	if (chgsets == NULL) {
		*rcode = KNOT_RCODE_SERVFAIL;
		log_zone_error("%s Cannot create changesets structure.\n", msg);
		free(msg);
		return ret;
	}

	// Process the UPDATE packet, apply to zone, create changesets.
	dbg_zones_verb("Processing UPDATE packet.\n");
	if (knot_changesets_create_changeset(chgsets) == NULL) {
		return KNOT_ENOMEM;
	}
	*rcode = KNOT_RCODE_SERVFAIL; /* SERVFAIL unless it applies correctly. */

	uint32_t new_serial = zones_next_serial(zone);

	knot_zone_contents_t *new_contents = NULL;
	knot_zone_contents_t *old_contents = zone->contents;
	ret = knot_ns_process_update(query, old_contents, &new_contents,
	                             chgsets, rcode, new_serial);
	if (ret != KNOT_EOK) {
		if (ret < 0) {
			log_zone_error("%s %s\n", msg, knot_strerror(ret));
		} else {
			log_zone_notice("%s No change to zone made.\n", msg);
			*rcode = KNOT_RCODE_NOERROR;
		}

		knot_changesets_free(&chgsets);
		free(msg);
		return (ret < 0) ? ret : KNOT_EOK;
	}

	knot_changesets_t *sec_chs = NULL;
	knot_changeset_t *sec_ch = NULL;
	uint32_t refresh_at = 0;

	assert(zone->conf);
	if (zone->conf->dnssec_enable) {
		sec_chs = knot_changesets_create();
		sec_ch = knot_changesets_create_changeset(sec_chs);
		if (sec_chs == NULL || sec_ch == NULL) {
			xfrin_rollback_update(zone->contents, &new_contents,
			                      chgsets->changes);
			knot_changesets_free(&chgsets);
			free(msg);
			return KNOT_ENOMEM;
		}
	}

	zone_t *fake_zone = create_fake_zone(zone);
	if (fake_zone == NULL) {
		log_zone_error("%s: Failed to apply changesets (%s)\n",
		               msg, knot_strerror(KNOT_ENOMEM));
		xfrin_rollback_update(zone->contents, &new_contents,
		                      chgsets->changes);
		knot_changesets_free(&chgsets);
		free(msg);
		return KNOT_ENOMEM;
	}

	// Apply changeset to zone created by DDNS processing

	hattrie_t *sorted_changes = NULL;

	if (zone->conf->dnssec_enable) {
		dbg_zones_verb("%s: Signing the UPDATE\n", msg);
		/*!
		 * Check if the UPDATE changed DNSKEYs. If yes, resign the whole
		 * zone, if not, sign only the changeset.
		 * Do the same if NSEC3PARAM changed.
		 */
		if (zones_dnskey_changed(old_contents, new_contents) ||
		    zones_nsec3param_changed(old_contents, new_contents)) {
			ret = knot_dnssec_zone_sign(fake_zone, sec_ch,
			                            KNOT_SOA_SERIAL_KEEP,
			                            &refresh_at, new_serial);
		} else {
			// Sign the created changeset
			ret = knot_dnssec_sign_changeset(fake_zone,
			                      knot_changesets_get_last(chgsets),
			                      sec_ch, KNOT_SOA_SERIAL_KEEP,
			                      &refresh_at,
			                      new_serial, &sorted_changes);
		}

		if (ret != KNOT_EOK) {
			log_zone_error("%s: Failed to sign incoming update (%s)"
			               "\n", msg, knot_strerror(ret));
			xfrin_rollback_update(zone->contents, &new_contents,
					      chgsets->changes);
			knot_changesets_free(&chgsets);
			knot_changesets_free(&sec_chs);
			free(msg);
			zone_free(&fake_zone);
			zone->contents->zone = zone;
			return ret;
		}

		dbg_zones_detail("%s: UPDATE signed (%zu changes)\n", msg,
		                 knot_changeset_size(sec_ch));
	}

	// Merge changesets
	journal_t *transaction = NULL;
	ret = zones_merge_and_store_changesets(zone, chgsets, sec_chs,
	                                       &transaction);
	if (ret != KNOT_EOK) {
		log_zone_error("%s: Failed to store changesets (%s)\n",
		               msg, knot_strerror(ret));
		xfrin_rollback_update(zone->contents, &new_contents,
		                      chgsets->changes);
		zones_free_merged_changesets(chgsets, sec_chs);
		free(msg);
		zone_free(&fake_zone);
		zone->contents->zone = zone;
		return ret;
	}

	bool new_signatures = !knot_changeset_is_empty(sec_ch);
	// Apply DNSSEC changeset
	if (new_signatures) {
		ret = xfrin_apply_changesets_dnssec(old_contents,
		                                    new_contents,
		                                    sec_chs,
		                                    chgsets,
		                                    sorted_changes);
		knot_zone_clear_sorted_changes(sorted_changes);
		hattrie_free(sorted_changes);
		if (ret != KNOT_EOK) {
			log_zone_error("%s: Failed to sign incoming update %s\n",
			               msg, knot_strerror(ret));
			zones_store_changesets_rollback(transaction);
			zones_free_merged_changesets(chgsets, sec_chs);
			return ret;
		}

		// Plan zone resign if needed
		assert(zone->dnssec_timer);
		ret = replan_zone_sign_after_ddns(zone, refresh_at);
		if (ret != KNOT_EOK) {
			log_zone_error("%s: Failed to replan zone sign %s\n",
			               msg, knot_strerror(ret));
			zones_store_changesets_rollback(transaction);
			zones_free_merged_changesets(chgsets, sec_chs);
			return ret;
		}
	} else {
		// Set NSEC3 nodes if no new signatures were created (or auto DNSSEC is off)
		ret = knot_zone_contents_adjust_nsec3_pointers(new_contents);
		if (ret != KNOT_EOK) {
			zones_store_changesets_rollback(transaction);
			zones_free_merged_changesets(chgsets, sec_chs);
			xfrin_rollback_update(zone->contents, &new_contents,
			                      chgsets->changes);
			free(msg);
			return ret;
		}
	}

	zone_free(&fake_zone);
	zone->contents->zone = zone;

	dbg_zones_verb("%s: DNSSEC changes applied\n", msg);

	// Commit transaction.
	if (transaction) {
		ret = zones_store_changesets_commit(transaction);
		if (ret != KNOT_EOK) {
			log_zone_error("%s: Failed to commit stored "
			               "changesets: %s."
			               "\n", msg, knot_strerror(ret));
			xfrin_rollback_update(zone->contents, &new_contents,
			                      chgsets->changes);
			zones_free_merged_changesets(chgsets, sec_chs);
			free(msg);
			return ret;
		}
	}

	// Switch zone contents.
	zone_retain(zone);      /* Retain pointer for safe RCU unlock. */
	rcu_read_unlock();      /* Unlock for switch. */
	ret = xfrin_switch_zone(zone, new_contents, XFR_TYPE_UPDATE);
	rcu_read_lock();        /* Relock */
	zone_release(zone);     /* Release held pointer. */
	if (ret != KNOT_EOK) {
		log_zone_error("%s Failed to replace current zone - %s\n",
		               msg, knot_strerror(ret));
		// Cleanup old and new contents
		xfrin_rollback_update(zone->contents, &new_contents,
		                      chgsets->changes);

		/* Free changesets, but not the data. */
		zones_free_merged_changesets(chgsets, sec_chs);
		return KNOT_ERROR;
	}

	// Cleanup.
	xfrin_cleanup_successful_update(chgsets->changes);
	if (sec_chs) {
		xfrin_cleanup_successful_update(sec_chs->changes);
	}

	// Free changesets, but not the data.
	zones_free_merged_changesets(chgsets, sec_chs);
	assert(ret == KNOT_EOK);
	*rcode = KNOT_RCODE_NOERROR; /* Mark as successful. */
	if (new_signatures) {
		log_zone_info("%s Signed.\n", msg);
	}
	log_zone_info("%s Finished.\n", msg);

	free(msg);
	msg = NULL;

	/* Trim extra heap. */
	mem_trim();

	/* Sync zonefile immediately if configured. */
	int sync_timeout = zone->conf->dbsync_timeout;
	if (sync_timeout == 0) {
		dbg_zones("%s: syncing zone immediately\n", __func__);
		zones_schedule_ixfr_sync(zone, 0);
	}

	return ret;
}

/*----------------------------------------------------------------------------*/
/* API functions                                                              */
/*----------------------------------------------------------------------------*/

int zones_zonefile_sync(zone_t *zone, journal_t *journal)
{
	if (!zone) {
		return KNOT_EINVAL;
	}
	if (journal == NULL) {
		return KNOT_EINVAL;
	}

	/* Fetch zone data. */
	int ret = KNOT_EOK;

	/* Lock zone data. */
	pthread_mutex_lock(&zone->lock);

	/* Lock RCU for zone contents. */
	rcu_read_lock();

	knot_zone_contents_t *contents = zone->contents;
	if (!contents) {
		rcu_read_unlock();
		pthread_mutex_unlock(&zone->lock);
		return KNOT_EINVAL;
	}

	/* Latest zone serial. */
	const knot_rrset_t *soa_rrs = 0;
	soa_rrs = knot_node_rrset(contents->apex, KNOT_RRTYPE_SOA);
	assert(soa_rrs != NULL);

	int64_t serial_ret = knot_rdata_soa_serial(soa_rrs);
	if (serial_ret < 0) {
		rcu_read_unlock();
		pthread_mutex_unlock(&zone->lock);
		return KNOT_EINVAL;
	}
	uint32_t serial_to = (uint32_t)serial_ret;

	/* Check for difference against zonefile serial. */
	if (zone->zonefile_serial != serial_to) {

		/* Save zone to zonefile. */
		dbg_zones("zones: syncing '%s' differences to '%s' "
		          "(SOA serial %u)\n",
		          zone->conf->name, zone->conf->file, serial_to);
		ret = zones_dump_zone_text(contents, zone->conf->file);
		if (ret != KNOT_EOK) {
			log_zone_warning("Failed to apply differences "
			                 "'%s' to '%s (%s)'\n",
			                 zone->conf->name, zone->conf->file,
			                 knot_strerror(ret));
			rcu_read_unlock();
			pthread_mutex_unlock(&zone->lock);
			return ret;
		}

		/* Update zone version. */
		struct stat st;
		if (stat(zone->conf->file, &st) < 0) {
			log_zone_warning("Failed to apply differences "
			                 "'%s' to '%s (%s)'\n",
			                 zone->conf->name, zone->conf->file,
			                 knot_strerror(KNOT_EACCES));
			rcu_read_unlock();
			pthread_mutex_unlock(&zone->lock);
			return KNOT_ERROR;
		} else {
			zone->zonefile_mtime = st.st_mtime;
		}

		/* Update journal entries. */
		dbg_zones_verb("zones: unmarking all dirty nodes "
		               "in '%s' journal\n",
		               zone->conf->name);
		journal_walk(journal, zones_ixfrdb_sync_apply);

		/* Update zone file serial. */
		dbg_zones("zones: new '%s' zonefile serial is %u\n",
		          zone->conf->name, serial_to);
		zone->zonefile_serial = serial_to;
	} else {
		dbg_zones("zones: '%s' zonefile is in sync "
		          "with differences\n", zone->conf->name);
		ret = KNOT_ERANGE;
	}

	/* Unlock RCU. */
	rcu_read_unlock();

	/* Unlock zone data. */
	pthread_mutex_unlock(&zone->lock);

	return ret;
}

/*----------------------------------------------------------------------------*/

int zones_process_response(knot_nameserver_t *nameserver,
                           int exp_msgid,
                           sockaddr_t *from,
                           knot_pkt_t *packet, uint8_t *response_wire,
                           size_t *rsize)
{
	if (!packet || !rsize || nameserver == NULL || from == NULL ||
	    response_wire == NULL) {
		return KNOT_EINVAL;
	}

	/* Declare no response. */
	*rsize = 0;

	/* Handle SOA query response, cancel EXPIRE timer
	 * and start AXFR transfer if needed.
	 * Reset REFRESH timer on finish.
	 */
	if (knot_pkt_qtype(packet) == KNOT_RRTYPE_SOA) {

		if (knot_wire_get_rcode(packet->wire) != KNOT_RCODE_NOERROR) {
			/*! \todo Handle error response. */
			return KNOT_ERROR;
		}

		/* Find matching zone and ID. */
		rcu_read_lock();
		const knot_dname_t *zone_name = knot_pkt_qname(packet);
		/*! \todo Change the access to the zone db. */
		zone_t *zone = knot_zonedb_find(nameserver->zone_db, zone_name);

		/* Get zone contents. */
		if (!zone || !zone->contents) {
			rcu_read_unlock();
			return KNOT_EINVAL;
		}

		/* Match ID against awaited. */
		uint16_t pkt_id = knot_wire_get_id(packet->wire);
		if ((int)pkt_id != exp_msgid) {
			rcu_read_unlock();
			return KNOT_ERROR;
		}

		/* Check SOA SERIAL. */
		int ret = xfrin_transfer_needed(zone->contents, packet);
		dbg_zones_verb("xfrin_transfer_needed() returned %s\n",
		               knot_strerror(ret));
		if (ret < 0) {
			/* RETRY/EXPIRE timers running, do not interfere. */
			rcu_read_unlock();
			return KNOT_ERROR;
		}

		/* No updates available. */
		if (ret == 0) {
			zones_schedule_refresh(zone, REFRESH_DEFAULT);
			rcu_read_unlock();
			return KNOT_EUPTODATE;
		}

		assert(ret > 0);

		/* Check zone transfer state. */
		pthread_mutex_lock(&zone->lock);
		if (zone->xfr_in.state == XFR_PENDING) {
			pthread_mutex_unlock(&zone->lock);
			rcu_read_unlock();
			return KNOT_EOK; /* Already pending. */
		} else {
			zone->xfr_in.state = XFR_PENDING;
		}

		/* Prepare XFR client transfer. */
		server_t *srv = (server_t *)knot_ns_get_data(nameserver);
		int rqtype = zones_transfer_to_use(zone);
		knot_ns_xfr_t *rq = xfr_task_create(zone, rqtype, XFR_FLAG_TCP);
		if (!rq) {
			pthread_mutex_unlock(&zone->lock);
			rcu_read_unlock();
			return KNOT_ENOMEM;
		}
		xfr_task_setaddr(rq, &zone->xfr_in.master, &zone->xfr_in.via);
		if (zone->xfr_in.tsig_key.name) {
			rq->tsig_key = &zone->xfr_in.tsig_key;
		}

		rcu_read_unlock();
		ret = xfr_enqueue(srv->xfr, rq);
		if (ret != KNOT_EOK) {
			xfr_task_free(rq);
			zone->xfr_in.state = XFR_SCHED; /* Revert state */
		}
		pthread_mutex_unlock(&zone->lock);
	}

	return KNOT_EOK;
}

/*----------------------------------------------------------------------------*/

knot_ns_xfr_type_t zones_transfer_to_use(zone_t *zone)
{
	if (zone == NULL || zone->ixfr_db == NULL) {
		return XFR_TYPE_AIN;
	}

	return XFR_TYPE_IIN;
}

/*----------------------------------------------------------------------------*/

static int zones_open_free_filename(const char *old_name, char **new_name)
{
	/* find zone name not present on the disk */
	size_t name_size = strlen(old_name);
	*new_name = malloc(name_size + 7 + 1);
	if (*new_name == NULL) {
		return -1;
	}
	memcpy(*new_name, old_name, name_size + 1);
	strncat(*new_name, ".XXXXXX", 7);
	dbg_zones_verb("zones: creating temporary zone file\n");
	mode_t old_mode = umask(077);
	int fd = mkstemp(*new_name);
	UNUSED(umask(old_mode));
	if (fd < 0) {
		dbg_zones_verb("zones: couldn't create temporary zone file\n");
		free(*new_name);
		*new_name = NULL;
	}

	return fd;
}

/*----------------------------------------------------------------------------*/

static int zones_dump_zone_text(knot_zone_contents_t *zone, const char *fname)
{
	assert(zone != NULL && fname != NULL);

	char *new_fname = NULL;
	int fd = zones_open_free_filename(fname, &new_fname);
	if (fd < 0) {
		return KNOT_EWRITABLE;
	}

	FILE *f = fdopen(fd, "w");
	if (f == NULL) {
		log_zone_warning("Failed to open file descriptor for text zone.\n");
		unlink(new_fname);
		free(new_fname);
		return KNOT_ERROR;
	}

	if (zone_dump_text(zone, f) != KNOT_EOK) {
		log_zone_warning("Failed to save the transferred zone to '%s'.\n",
		                 new_fname);
		fclose(f);
		unlink(new_fname);
		free(new_fname);
		return KNOT_ERROR;
	}

	/* Set zone file rights to 0640. */
	fchmod(fd, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);

	/* Swap temporary zonefile and new zonefile. */
	fclose(f);

	int ret = rename(new_fname, fname);
	if (ret < 0 && ret != EEXIST) {
		log_zone_warning("Failed to replace old zone file '%s'' with a "
		                 "new zone file '%s'.\n", fname, new_fname);
		unlink(new_fname);
		free(new_fname);
		return KNOT_ERROR;
	}

	free(new_fname);
	return KNOT_EOK;
}

/*----------------------------------------------------------------------------*/

int zones_save_zone(const knot_ns_xfr_t *xfr)
{
	/* Zone is already referenced, no need for RCU locking. */

	if (xfr == NULL || xfr->new_contents == NULL || xfr->zone == NULL) {
		return KNOT_EINVAL;
	}

	dbg_xfr("xfr: %s Saving new zone file.\n", xfr->msg);

	rcu_read_lock();

	knot_zone_contents_t *new_zone = xfr->new_contents;

	const char *zonefile = xfr->zone->conf->file;

	/* Check if the new zone apex dname matches zone name. */
	knot_dname_t *cur_name = knot_dname_from_str(xfr->zone->conf->name);
	const knot_dname_t *new_name = NULL;
	new_name = knot_node_owner(knot_zone_contents_apex(new_zone));
	int r = knot_dname_cmp(cur_name, new_name);
	knot_dname_free(&cur_name);
	if (r != 0) {
		rcu_read_unlock();
		return KNOT_EINVAL;
	}

	assert(zonefile != NULL);

	/* dump the zone into text zone file */
	int ret = zones_dump_zone_text(new_zone, zonefile);
	rcu_read_unlock();
	return ret;
}

/*----------------------------------------------------------------------------*/

int zones_ns_conf_hook(const struct conf_t *conf, void *data)
{
	knot_nameserver_t *ns = (knot_nameserver_t *)data;
	dbg_zones_verb("zones: reconfiguring name server.\n");

	/* Server identification, RFC 4892. */
	ns->identity = conf->identity;
	ns->version = conf->version;

	/* New OPT RR: keep the old pointer and free it after RCU sync. */
	knot_opt_rr_t *opt_rr = knot_edns_new();
	if (opt_rr == NULL) {
		log_server_error("Couldn't create OPT RR, please restart.\n");
	} else {
		knot_edns_set_version(opt_rr, EDNS_VERSION);
		knot_edns_set_payload(opt_rr, conf->max_udp_payload);
		if (conf->nsid_len > 0) {
			knot_edns_add_option(opt_rr, EDNS_OPTION_NSID,
			                     conf->nsid_len,
			                     (const uint8_t *)conf->nsid);
		}
	}

	knot_opt_rr_t *opt_rr_old = ns->opt_rr;
	ns->opt_rr = opt_rr;

	knot_zonedb_t *old_db = NULL;
	int ret = zones_update_db_from_config(conf, ns, &old_db);
	if (ret != KNOT_EOK) {
		return ret;
	}

	synchronize_rcu();

	knot_edns_free(&opt_rr_old);

	dbg_zones_verb("zones: nameserver's zone db: %p, old db: %p\n",
	               ns->zone_db, old_db);

	/* Delete all deprecated zones and delete the old database. */
	knot_zonedb_deep_free(&old_db);

	/* Trim extra heap. */
	mem_trim();

	/* Update events scheduled for zone. */
	knot_zonedb_iter_t it;
	knot_zonedb_iter_begin(ns->zone_db, &it);
	while(!knot_zonedb_iter_finished(&it)) {
		zone_t *zone = knot_zonedb_iter_val(&it);
		zones_schedule_refresh(zone, 0); /* Now. */
		zones_schedule_notify(zone);
		knot_zonedb_iter_next(&it);
	}

	return KNOT_EOK;
}

/*----------------------------------------------------------------------------*/
/* Counting size of changeset in serialized form.                             */
/*----------------------------------------------------------------------------*/

int zones_changeset_binary_size(const knot_changeset_t *chgset, size_t *size)
{
	if (chgset == NULL || size == NULL) {
		return KNOT_EINVAL;
	}

	size_t soa_from_size = rrset_binary_size(chgset->soa_from);
	size_t soa_to_size = rrset_binary_size(chgset->soa_to);

	size_t remove_size = 0;
	knot_rr_ln_t *rr_node = NULL;
	WALK_LIST(rr_node, chgset->remove) {
		knot_rrset_t *rrset = rr_node->rr;
		remove_size += rrset_binary_size(rrset);
	}

	size_t add_size = 0;
	WALK_LIST(rr_node, chgset->add) {
		knot_rrset_t *rrset = rr_node->rr;
		add_size += rrset_binary_size(rrset);
	}

	/*! \todo How is the changeset serialized? Any other parts? */
	*size = soa_from_size + soa_to_size + remove_size + add_size;
	/* + Changeset flags. */
	*size += sizeof(uint32_t);

	return KNOT_EOK;
}

/*----------------------------------------------------------------------------*/
/* Changeset serialization and storing (new)                                  */
/*----------------------------------------------------------------------------*/

static int zones_rrset_write_to_mem(const knot_rrset_t *rr, char **entry,
                                    size_t *remaining) {
	size_t written = 0;
	int ret = rrset_serialize(rr, *((uint8_t **)entry),
	                          &written);
	if (ret == KNOT_EOK) {
		assert(written <= *remaining);
		*remaining -= written;
		*entry += written;
	}

	return ret;
}

static int zones_serialize_and_store_chgset(const knot_changeset_t *chs,
                                            char *entry, size_t max_size)
{
	/* Write changeset flags. */
	memcpy(entry, (char*)&chs->flags, sizeof(uint32_t));
	entry += sizeof(uint32_t);
	max_size -= sizeof(uint32_t);

	/* Serialize SOA 'from'. */
	int ret = zones_rrset_write_to_mem(chs->soa_from, &entry, &max_size);
	if (ret != KNOT_EOK) {
		dbg_zones("%s:%d ret = %s\n", __func__, __LINE__, knot_strerror(ret));
		return KNOT_ERROR;  /*! \todo Other code? */
	}

	/* Serialize RRSets from the 'remove' section. */
	knot_rr_ln_t *rr_node = NULL;
	WALK_LIST(rr_node, chs->remove) {
		knot_rrset_t *rrset = rr_node->rr;
		ret = zones_rrset_write_to_mem(rrset, &entry, &max_size);
		if (ret != KNOT_EOK) {
			dbg_zones("%s:%d ret = %s\n", __func__, __LINE__, knot_strerror(ret));
			return KNOT_ERROR;  /*! \todo Other code? */
		}
	}

	/* Serialize SOA 'to'. */
	ret = zones_rrset_write_to_mem(chs->soa_to, &entry, &max_size);
	if (ret != KNOT_EOK) {
		dbg_zones("%s:%d ret = %s\n", __func__, __LINE__, knot_strerror(ret));
		return KNOT_ERROR;  /*! \todo Other code? */
	}

	/* Serialize RRSets from the 'add' section. */
	WALK_LIST(rr_node, chs->add) {
		knot_rrset_t *rrset = rr_node->rr;
		ret = zones_rrset_write_to_mem(rrset, &entry, &max_size);
		if (ret != KNOT_EOK) {
			dbg_zones("%s:%d ret = %s\n", __func__, __LINE__, knot_strerror(ret));
			return KNOT_ERROR;  /*! \todo Other code? */
		}

	}

	return KNOT_EOK;
}

/*----------------------------------------------------------------------------*/

static int zones_store_changeset(const knot_changeset_t *chs, journal_t *j,
                                 zone_t *zone)
{
	assert(chs != NULL);
	assert(j != NULL);

	dbg_xfr("Saving changeset from %u to %u.\n",
	        chs->serial_from, chs->serial_to);

	uint64_t k = ixfrdb_key_make(chs->serial_from, chs->serial_to);

	/* Count the size of the entire changeset in serialized form. */
	size_t entry_size = 0;

	int ret = zones_changeset_binary_size(chs, &entry_size);
	assert(ret == KNOT_EOK);

	dbg_xfr_verb("Size in serialized form: %zu\n", entry_size);

	/* Reserve space for the journal entry. */
	char *journal_entry = NULL;
	ret = journal_map(j, k, &journal_entry, entry_size);
	if (ret != KNOT_EOK) {
		dbg_xfr("Failed to map space for journal entry: %s.\n",
		        knot_strerror(ret));
		return ret;
	}

	assert(journal_entry != NULL);

	/* Serialize changeset, saving it bit by bit. */
	ret = zones_serialize_and_store_chgset(chs, journal_entry, entry_size);
	/* Unmap the journal entry.
	 * If successfuly written changeset to journal, validate the entry. */
	int unmap_ret = journal_unmap(j, k, journal_entry, ret == KNOT_EOK);
	if (ret == KNOT_EOK && unmap_ret != KNOT_EOK) {
		ret = unmap_ret; /* Propagate the result. */
	}

	return ret;
}

/*----------------------------------------------------------------------------*/

journal_t *zones_store_changesets_begin(zone_t *zone)
{
	if (zone == NULL) {
		return NULL;
	}

	/* Fetch zone-specific data. */
	if (!zone->ixfr_db) {
		return NULL;
	}

	/* Begin transaction, will be released on commit/rollback. */
	int ret = journal_retain(zone->ixfr_db);
	if (ret != KNOT_EOK) {
		return NULL;
	}

	if (journal_trans_begin(zone->ixfr_db) != KNOT_EOK) {
		journal_release(zone->ixfr_db);
		return NULL;
	}

	return zone->ixfr_db;
}

/*----------------------------------------------------------------------------*/

int zones_store_changesets_commit(journal_t *j)
{
	if (j == NULL) {
		return KNOT_EINVAL;
	}

	int ret = journal_trans_commit(j);
	journal_release(j);
	return ret;
}

/*----------------------------------------------------------------------------*/

int zones_store_changesets_rollback(journal_t *j)
{
	if (j == NULL) {
		return KNOT_EINVAL;
	}

	int ret = journal_trans_rollback(j);
	journal_release(j);
	return ret;
}

/*----------------------------------------------------------------------------*/

int zones_store_changesets(zone_t *zone, knot_changesets_t *src, journal_t *j)
{
	if (zone == NULL || src == NULL) {
		return KNOT_EINVAL;
	}

	int ret = KNOT_EOK;

	/* Fetch zone-specific data. */
	if (!zone->ixfr_db) {
		return KNOT_EINVAL;
	}

	/* Begin writing to journal. */
	knot_changeset_t *chs = NULL;
	WALK_LIST(chs, src->sets) {
		/* Make key from serials. */
		ret = zones_store_changeset(chs, j, zone);
		if (ret != KNOT_EOK)
			break;
	}

	/*! @note If the journal is full, this function returns KNOT_EBUSY. */

	/* Written changesets to journal. */
	return ret;
}

/*----------------------------------------------------------------------------*/

int zones_create_changeset(const zone_t *old_zone,
                           const zone_t *new_zone,
                           knot_changeset_t *changeset)
{
	if (old_zone == NULL || old_zone->contents == NULL
	    || new_zone == NULL || new_zone->contents == NULL
	    || changeset == NULL) {
		dbg_zones("zones: create_changesets: "
		          "NULL arguments.\n");
		return KNOT_EINVAL;
	}

	int ret = knot_zone_contents_create_diff(old_zone->contents,
	                                         new_zone->contents,
	                                         changeset);
	if (ret != KNOT_EOK) {
		if (ret == KNOT_ERANGE) {
			dbg_zones_detail("zones: create_changesets: "
			                 "New serial was lower than the old "
			                 "one.\n");
			return KNOT_ERANGE;
		} else if (ret == KNOT_ENODIFF) {
			dbg_zones_detail("zones: create_changesets: "
			                 "New serial was the same as the old "
			                 "one.\n");
			return KNOT_ENODIFF;
		} else {
			dbg_zones("zones: create_changesets: "
			          "Could not create changesets. Reason: %s\n",
			          knot_strerror(ret));
			return KNOT_ERROR;
		}
	}

	return KNOT_EOK;
}

/*----------------------------------------------------------------------------*/

int zones_store_and_apply_chgsets(knot_changesets_t *chs,
                                  zone_t *zone,
                                  knot_zone_contents_t **new_contents,
                                  const char *msgpref, int type)
{
	int ret = KNOT_EOK;
	int apply_ret = KNOT_EOK;
	int switch_ret = KNOT_EOK;

	dbg_xfr("xfr: IXFR/IN serializing and saving changesets\n");
	journal_t *transaction = NULL;
	ret = zones_store_changesets_begin_and_store(zone, chs, &transaction);
	if (ret != KNOT_EOK) {
		log_zone_error("%s Failed to serialize and store "
		               "changesets: %s.\n", msgpref,
		               knot_strerror(ret));
		/* Free changesets, but not the data. */
		knot_changesets_free(&chs);
		return ret;
	}

	/* Now, try to apply the changesets to the zone. */
	apply_ret = xfrin_apply_changesets(zone, chs, new_contents);

	if (apply_ret != KNOT_EOK) {
		log_zone_error("%s Failed to apply changesets.\n", msgpref);

		/* Free changesets, but not the data. */
		zones_store_changesets_rollback(transaction);
		knot_changesets_free(&chs);
		return apply_ret;  // propagate the error above
	}

	/* Commit transaction. */
	ret = zones_store_changesets_commit(transaction);
	if (ret != KNOT_EOK) {
		xfrin_rollback_update(zone->contents, new_contents,
		                      chs->changes);
		log_zone_error("%s Failed to commit stored changesets.\n", msgpref);
		knot_changesets_free(&chs);
		return ret;
	}

	/* Switch zone contents. */
	// Unlock RCU for the switching procedure (would result in deadlock)
	/*! \todo Maybe the unlocking should go inside the switching function.*/
	rcu_read_unlock();
	switch_ret = xfrin_switch_zone(zone, *new_contents, type);
	rcu_read_lock();

	if (switch_ret != KNOT_EOK) {
		log_zone_error("%s Failed to replace current zone.\n", msgpref);
		// Cleanup old and new contents
		xfrin_rollback_update(zone->contents, new_contents,
		                      chs->changes);

		/* Free changesets, but not the data. */
		knot_changesets_free(&chs);
		return KNOT_ERROR;
	}

	xfrin_cleanup_successful_update(chs->changes);

	/* Free changesets, but not the data. */
	knot_changesets_free(&chs);
	assert(ret == KNOT_EOK);
	return KNOT_EOK;
}

/*----------------------------------------------------------------------------*/

int zones_schedule_notify(zone_t *zone)
{
	if (!zone) {
		return KNOT_EINVAL;
	}

	/* Do not issue NOTIFY queries if stub. */
	if (!zone->contents) {
		return KNOT_EOK;
	}

	/* Schedule NOTIFY to slaves. */
	conf_zone_t *cfg = zone->conf;
	conf_remote_t *r = 0;
	WALK_LIST(r, cfg->acl.notify_out) {

		/* Fetch remote. */
		conf_iface_t *cfg_if = r->remote;

		/* Create request. */
		knot_ns_xfr_t *rq = xfr_task_create(zone, XFR_TYPE_NOTIFY, XFR_FLAG_UDP);
		if (!rq) {
			log_zone_error("Failed to create NOTIFY for '%s', "
			               "not enough memory.\n", cfg->name);
			continue;
		}

		/* Assign TSIG if exists. */
		if (cfg_if->key) {
			rq->tsig_key = cfg_if->key;
		}

		/* Parse server address. */
		sockaddr_t addr;
		sockaddr_set(&addr, cfg_if->family, cfg_if->address, cfg_if->port);
		xfr_task_setaddr(rq, &addr, &cfg_if->via);
		rq->data = (void *)((long)cfg->notify_retries);
		if (xfr_enqueue(zone->server->xfr, rq) != KNOT_EOK) {
			log_zone_error("Failed to enqueue NOTIFY for '%s'.\n",
			               cfg->name);
			continue;
		}
	}

	return KNOT_EOK;
}

int zones_schedule_refresh(zone_t *zone, int64_t time)
{
	if (!zone) {
		return KNOT_EINVAL;
	}

	/* Cancel REFRESH timer. */
	evsched_t *sch = zone->server->sched;
	if (zone->xfr_in.timer) {
		evsched_cancel(sch, zone->xfr_in.timer);
	}

	/* Cancel EXPIRE timer. */
	if (zone->xfr_in.expire) {
		evsched_cancel(sch, zone->xfr_in.expire);
	}

	/* Check XFR/IN master server. */
	pthread_mutex_lock(&zone->lock);
	rcu_read_lock();
	zone->xfr_in.state = XFR_IDLE;
	if (zone->xfr_in.has_master) {

		/* Schedule REFRESH timer. */
		if (time < 0) {
			if (zone->contents) {
				time = zones_jitter(zones_soa_refresh(zone));
			} else {
				time = zone->xfr_in.bootstrap_retry;
			}
		}

		evsched_schedule(sch, zone->xfr_in.timer, time);
		dbg_zones("zone: REFRESH '%s' set to %"PRIi64"\n",
		          zone->conf->name, time);
		zone->xfr_in.state = XFR_SCHED;
	}
	rcu_read_unlock();
	pthread_mutex_unlock(&zone->lock);

	return KNOT_EOK;
}

int zones_dnssec_sign(zone_t *zone, bool force, uint32_t *refresh_at)
{
	int ret = KNOT_EOK;
	char *msgpref = NULL;
	*refresh_at = 0;

	knot_changesets_t *chs = knot_changesets_create();
	if (chs == NULL) {
		ret = KNOT_ENOMEM;
		goto done;
	}
	knot_changeset_t *ch = knot_changesets_create_changeset(chs);
	if (ch == NULL) {
		ret = KNOT_ENOMEM;
		goto done;
	}

	char *zname = knot_dname_to_str(zone->name);
	msgpref = sprintf_alloc("DNSSEC: Zone %s -", zname);
	free(zname);
	if (msgpref == NULL) {
		ret = KNOT_ENOMEM;
		goto done;
	}

	if (force) {
		log_zone_info("%s Complete resign started (dropping all "
			      "previous signatures)...\n", msgpref);
	} else {
		log_zone_info("%s Signing zone...\n", msgpref);
	}

	uint32_t new_serial = zones_next_serial(zone);

	if (force) {
		ret = knot_dnssec_zone_sign_force(zone, ch, refresh_at,
		                                  new_serial);
	} else {
		ret = knot_dnssec_zone_sign(zone, ch, KNOT_SOA_SERIAL_UPDATE,
		                            refresh_at, new_serial);
	}
	if (ret != KNOT_EOK) {
		goto done;
	}

	if (!zones_changesets_empty(chs)) {
		knot_zone_contents_t *new_c = NULL;
		ret = zones_store_and_apply_chgsets(chs, zone, &new_c, "DNSSEC",
						    XFR_TYPE_UPDATE);
		chs = NULL; // freed by zones_store_and_apply_chgsets()
		if (ret != KNOT_EOK) {
			log_zone_error("%s Could not sign zone (%s).\n",
				       msgpref, knot_strerror(ret));
			goto done;
		}
	}

	log_zone_info("%s Successfully signed.\n", msgpref);

done:
	knot_changesets_free(&chs);
	free(msgpref);
	return ret;
}

int zones_dnssec_ev(event_t *event)
{
	// We will be working with zone, don't want it to change in the meantime
	rcu_read_lock();
	zone_t *zone = (zone_t *)event->data;
	uint32_t refresh_at = 0;

	int ret = zones_dnssec_sign(zone, false, &refresh_at);
	if (refresh_at != 0) {
		ret = zones_schedule_dnssec(zone, refresh_at);
	}

	rcu_read_unlock();

	return ret;
}

int zones_cancel_dnssec(zone_t *zone)
{
	if (!zone) {
		return KNOT_EINVAL;
	}

	evsched_t *scheduler = zone->server->sched;

	if (zone->dnssec_timer) {
		evsched_cancel(scheduler, zone->dnssec_timer);
	}

	return KNOT_EOK;
}

int zones_schedule_dnssec(zone_t *zone, time_t unixtime)
{
	if (!zone) {
		return KNOT_EINVAL;
	}

	evsched_t *scheduler = zone->server->sched;

	// event not created yet
	if (zone->dnssec_timer == NULL) {
		return KNOT_EAGAIN;
	}

	char *zname = knot_dname_to_str(zone->name);

	// absolute time -> relative time

	time_t now = time(NULL);
	int32_t relative = 0;
	if (unixtime <= now) {
		log_zone_warning("DNSSEC: Zone %s: Signature life time too low, "
		                 "set higher value in configuration!\n", zname);
	} else {
		relative = unixtime - now;
	}

	// log the message

	char time_str[64] = {'\0'};
	struct tm time_gm = {0};

	gmtime_r(&unixtime, &time_gm);

	strftime(time_str, sizeof(time_str), KNOT_LOG_TIME_FORMAT, &time_gm);

	log_zone_info("DNSSEC: Zone %s: Next signing planned on %s.\n",
	              zname, time_str);

	free(zname);

	// schedule

	evsched_schedule(scheduler, zone->dnssec_timer, relative * 1000);

	return KNOT_EOK;
}

/*!
 * \brief Schedule IXFR sync for given zone.
 */
void zones_schedule_ixfr_sync(zone_t *zone, int dbsync_timeout)
{
	assert(zone);
	assert(zone->server);
	assert(zone->server->sched);

	if (zone->ixfr_dbsync != NULL) {
		evsched_t *sched = zone->server->sched;
		evsched_schedule(sched, zone->ixfr_dbsync, dbsync_timeout * 1000);
	}
}

int zones_process_update_response(knot_ns_xfr_t *data, uint8_t *rwire, size_t *rsize)
{
	/* Processing of a forwarded response:
	 * change packet id
	 */
	int ret = KNOT_EOK;
	knot_wire_set_id(rwire, (uint16_t)data->packet_nr);

	/* Forward the response. */
	ret = data->send(data->fwd_src_fd, &data->fwd_addr, rwire, *rsize);
	if (ret != *rsize) {
		ret = KNOT_ECONN;
	} else {
		ret = KNOT_EOK;
	}

	/* As it is a response, do not reply back. */
	*rsize = 0;
	return ret;
}

int zones_verify_tsig_query(const knot_pkt_t *query,
                            const knot_tsig_key_t *key,
                            knot_rcode_t *rcode, uint16_t *tsig_rcode,
                            uint64_t *tsig_prev_time_signed)
{
	assert(key != NULL);
	assert(rcode != NULL);
	assert(tsig_rcode != NULL);

	if (query->tsig_rr == NULL) {
		dbg_zones("TSIG key required, but not in query - REFUSED.\n");
		*rcode = KNOT_RCODE_REFUSED;
		return KNOT_TSIG_EBADKEY;
	}

	/*
	 * 1) Check if we support the requested algorithm.
	 */
	knot_tsig_algorithm_t alg = tsig_rdata_alg(query->tsig_rr);
	if (knot_tsig_digest_length(alg) == 0) {
		log_answer_info("Unsupported digest algorithm "
		                "requested, treating as bad key\n");
		/*! \todo [TSIG] It is unclear from RFC if I
		 *               should treat is as a bad key
		 *               or some other error.
		 */
		*rcode = KNOT_RCODE_NOTAUTH;
		*tsig_rcode = KNOT_RCODE_BADKEY;
		return KNOT_TSIG_EBADKEY;
	}

	const knot_dname_t *kname = knot_rrset_owner(query->tsig_rr);
	assert(kname != NULL);

	/*
	 * 2) Find the particular key used by the TSIG.
	 *    Check not only name, but also the algorithm.
	 */
	if (key && kname && knot_dname_cmp(key->name, kname) == 0
	    && key->algorithm == alg) {
		dbg_zones_verb("Found claimed TSIG key for comparison\n");
	} else {
		*rcode = KNOT_RCODE_NOTAUTH;
		*tsig_rcode = KNOT_RCODE_BADKEY;
		return KNOT_TSIG_EBADKEY;
	}

	/*
	 * 3) Validate the query with TSIG.
	 */
	/* Prepare variables for TSIG */
	/*! \todo These need to be saved to the response somehow. */
	//size_t tsig_size = tsig_wire_maxsize(key);
	size_t digest_max_size = knot_tsig_digest_length(key->algorithm);
	//size_t digest_size = 0;
	//uint64_t tsig_prev_time_signed = 0;
	//uint8_t *digest = (uint8_t *)malloc(digest_max_size);
	//memset(digest, 0 , digest_max_size);

	/* Copy MAC from query. */
	dbg_zones_verb("Validating TSIG from query\n");

	//const uint8_t* mac = tsig_rdata_mac(tsig_rr);
	size_t mac_len = tsig_rdata_mac_length(query->tsig_rr);

	int ret = KNOT_EOK;

	if (mac_len > digest_max_size) {
		*rcode = KNOT_RCODE_FORMERR;
		dbg_zones("MAC length %zu exceeds digest "
		       "maximum size %zu\n", mac_len, digest_max_size);
		return KNOT_EMALF;
	} else {
		//memcpy(digest, mac, mac_len);
		//digest_size = mac_len;

		/* Check query TSIG. */
		ret = knot_tsig_server_check(query->tsig_rr,
		                             query->wire,
		                             query->size, key);
		dbg_zones_verb("knot_tsig_server_check() returned %s\n",
		               knot_strerror(ret));

		/* Evaluate TSIG check results. */
		switch(ret) {
		case KNOT_EOK:
			*rcode = KNOT_RCODE_NOERROR;
			break;
		case KNOT_TSIG_EBADKEY:
			*tsig_rcode = KNOT_RCODE_BADKEY;
			*rcode = KNOT_RCODE_NOTAUTH;
			break;
		case KNOT_TSIG_EBADSIG:
			*tsig_rcode = KNOT_RCODE_BADSIG;
			*rcode = KNOT_RCODE_NOTAUTH;
			break;
		case KNOT_TSIG_EBADTIME:
			*tsig_rcode = KNOT_RCODE_BADTIME;
			// store the time signed from the query
			*tsig_prev_time_signed = tsig_rdata_time_signed(query->tsig_rr);
			*rcode = KNOT_RCODE_NOTAUTH;
			break;
		case KNOT_EMALF:
			*rcode = KNOT_RCODE_FORMERR;
			break;
		default:
			*rcode = KNOT_RCODE_SERVFAIL;
		}
	}

	return ret;
}

/*!
 * \brief Apply changesets to zone from journal.
 */
int zones_journal_apply(zone_t *zone)
{
	/* Fetch zone. */
	if (!zone) {
		return KNOT_EINVAL;
	}

	rcu_read_lock();
	knot_zone_contents_t *contents = zone->contents;
	if (!contents) {
		rcu_read_unlock();
		return KNOT_ENOENT;
	}

	/* Fetch SOA serial. */
	const knot_rrset_t *soa_rrs = 0;
	soa_rrs = knot_node_rrset(contents->apex, KNOT_RRTYPE_SOA);
	assert(soa_rrs != NULL);
	int64_t serial_ret = knot_rdata_soa_serial(soa_rrs);
	if (serial_ret < 0) {
		rcu_read_unlock();
		return KNOT_EINVAL;
	}
	uint32_t serial = (uint32_t)serial_ret;

	/* Load all pending changesets. */
	dbg_zones_verb("zones: loading all changesets of '%s' from SERIAL %u\n",
	               zone->conf->name, serial);
	knot_changesets_t* chsets = knot_changesets_create();
	if (chsets == NULL) {
		rcu_read_unlock();
		return KNOT_ERROR;
	}

	/*! \todo Check what should be the upper bound. */
	int ret = zones_load_changesets(zone, chsets, serial, serial - 1);
	if (ret == KNOT_EOK || ret == KNOT_ERANGE) {
		if (!EMPTY_LIST(chsets->sets)) {
			/* Apply changesets. */
			log_zone_info("Applying '%zu' changesets from journal "
			              "to zone '%s'.\n",
			              chsets->count, zone->conf->name);
			knot_zone_contents_t *contents = NULL;
			int apply_ret = xfrin_apply_changesets(zone, chsets,
			                                       &contents);
			if (apply_ret != KNOT_EOK) {
				log_zone_error("Failed to apply changesets to"
				               " '%s' - Apply failed: %s\n",
				               zone->conf->name,
				               knot_strerror(apply_ret));
				ret = KNOT_ERROR;
			} else {
				/* Switch zone immediately. */
				log_zone_info("Zone '%s' serial %u -> %u.\n",
				              zone->conf->name,
				              serial, knot_zone_serial(contents));
				dbg_zones("Old zone contents: %p, new: %p\n",
				          zone->contents, contents);
				rcu_read_unlock();
				apply_ret = xfrin_switch_zone(zone, contents,
							      XFR_TYPE_IIN);
				rcu_read_lock();
				if (apply_ret == KNOT_EOK) {
					xfrin_cleanup_successful_update(
							chsets->changes);
				} else {
					log_zone_error("Failed to apply "
					               "changesets to '%s' - Switch failed: "
					               "%s\n", zone->conf->name,
					               knot_strerror(apply_ret));
					ret = KNOT_ERROR;

					// Cleanup old and new contents
					xfrin_rollback_update(zone->contents,
					                      &contents,
					                      chsets->changes);
				}
			}
		}
	} else {
		dbg_zones("zones: failed to load changesets - %s\n",
		          knot_strerror(ret));
	}

	/* Free changesets and return. */
	rcu_read_unlock();
	knot_changesets_free(&chsets);
	return ret;
}

/*!
 * \brief Creates diff and DNSSEC changesets and stores them to journal.
 */
int zones_do_diff_and_sign(const conf_zone_t *z, zone_t *zone,
                           const knot_nameserver_t *ns, bool zone_changed)
{
	/* Calculate differences. */
	rcu_read_lock();
	zone_t *z_old = knot_zonedb_find(ns->zone_db, zone->name);
	/* Ensure both new and old have zone contents. */
	knot_zone_contents_t *zc = zone ? zone->contents : NULL;
	knot_zone_contents_t *zc_old = z_old ? z_old->contents : NULL;

	dbg_zones("Going to calculate diff. Old contents: %p, new: %p\n",
	          zc_old, zc);

	knot_changesets_t *diff_chs = NULL;
	if (z->build_diffs && zc && zc_old && zone_changed) {
		diff_chs = knot_changesets_create();
		if (diff_chs == NULL) {
			rcu_read_unlock();
			return KNOT_ENOMEM;
		}
		knot_changeset_t *diff_ch =
			knot_changesets_create_changeset(diff_chs);
		if (diff_ch == NULL) {
			knot_changesets_free(&diff_chs);
			rcu_read_unlock();
			return KNOT_ENOMEM;
		}
		dbg_zones("Generating diff.\n");
		int ret = zones_create_changeset(z_old,
		                                 zone, diff_ch);
		if (ret == KNOT_ENODIFF) {
			log_zone_warning("Zone file for '%s' changed, but "
			                 "serial didn't - won't create "
			                 "changesets.\n", z->name);
		} else if (ret != KNOT_EOK) {
			log_zone_warning("Failed to calculate differences from "
			                 "the zone file update: %s\n",
			                 knot_strerror(ret));
		}
		/* Even if there's nothing to create the diff from
		 * we can still sign the zone - inconsistencies may happen. */
		// TODO consider returning straight away when serial did not change
		if (ret != KNOT_EOK && ret != KNOT_ENODIFF) {
			knot_changesets_free(&diff_chs);
			rcu_read_unlock();
			return ret;
		}
	}

	/* Run DNSSEC signing if enabled (no zone change needed) */
	knot_changesets_t *sec_chs = NULL;
	knot_changeset_t *sec_ch = NULL;
	knot_zone_contents_t *new_contents = NULL;
	uint32_t refresh_at = 0;
	if (z->dnssec_enable) {
		sec_chs = knot_changesets_create();
		if (sec_chs == NULL) {
			knot_changesets_free(&diff_chs);
			rcu_read_unlock();
			return KNOT_ENOMEM;
		}
		/* Extra changeset is needed. */
		sec_ch = knot_changesets_create_changeset(sec_chs);
		if (sec_ch == NULL) {
			knot_changesets_free(&diff_chs);
			knot_changesets_free(&sec_chs);
			rcu_read_unlock();
			return KNOT_ENOMEM;
		}

		log_zone_info("DNSSEC: Zone %s - Signing started...\n",
		              z->name);

		uint32_t new_serial = zones_next_serial(zone);

		/*!
		 * Update serial even if diff did that. This way it's always
		 * possible to flush the changes to zonefile.
		 */
		int ret = knot_dnssec_zone_sign(zone, sec_ch,
		                                KNOT_SOA_SERIAL_UPDATE,
		                                &refresh_at, new_serial);
		if (ret != KNOT_EOK) {
			knot_changesets_free(&diff_chs);
			knot_changesets_free(&sec_chs);
			rcu_read_unlock();
			return ret;
		}
	}

	/* Merge changesets created by diff and sign. */
	journal_t *transaction = NULL;
	int ret = zones_merge_and_store_changesets(zone, diff_chs,
	                                           sec_chs,
	                                           &transaction);
	if (ret != KNOT_EOK) {
		knot_changesets_free(&diff_chs);
		knot_changesets_free(&sec_chs);
		rcu_read_unlock();
		return ret;
	}

	bool new_signatures = sec_ch && !knot_changeset_is_empty(sec_ch);
	/* Apply DNSSEC changeset. */
	if (new_signatures) {
		ret = xfrin_apply_changesets(zone, sec_chs,
		                             &new_contents);
		if (ret != KNOT_EOK) {
			zones_store_changesets_rollback(transaction);
			zones_free_merged_changesets(diff_chs, sec_chs);
			rcu_read_unlock();
			return ret;
		}
		assert(new_contents);
	}

	/* Commit transaction. */
	if (transaction) {
		ret = zones_store_changesets_commit(transaction);
		if (ret != KNOT_EOK) {
			log_zone_error("Failed to commit stored changesets: %s."
			               "\n", knot_strerror(ret));
			zones_free_merged_changesets(diff_chs, sec_chs);
			rcu_read_unlock();
			return ret;
		}
	}

	/* Switch zone contents. */
	if (new_contents) {
		rcu_read_unlock();
		ret = xfrin_switch_zone(zone, new_contents,
		                        XFR_TYPE_DNSSEC);
		rcu_read_lock();
		if (ret != KNOT_EOK) {
			// Cleanup old and new contents
			xfrin_rollback_update(zone->contents,
			                      &new_contents,
			                      sec_chs->changes);
			zones_free_merged_changesets(diff_chs, sec_chs);
			rcu_read_unlock();
			return ret;
		}
	}

	if (new_signatures) {
		xfrin_cleanup_successful_update(sec_chs->changes);
		log_zone_info("DNSSEC: Zone %s - Successfully signed.\n",
		              z->name);
	}

	rcu_read_unlock();

	zones_free_merged_changesets(diff_chs, sec_chs);

	// Schedule next zone signing
	if (z->dnssec_enable) {
		ret = zones_schedule_dnssec(zone, refresh_at);
	}

	return ret;
}
