/*  Copyright (C) 2020 CZ.NIC, z.s.p.o. <knot-dns@labs.nic.cz>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "knot/zone/catalog.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <urcu.h>

#include "contrib/openbsd/siphash.h"
#include "contrib/string.h"
#include "contrib/wire_ctx.h"

#include "knot/common/log.h"
#include "knot/conf/conf.h"
#include "knot/updates/zone-update.h"

#define CATALOG_VERSION "1.0"
#define CATALOG_ZONE_VERSION "2" // must be just one char long
#define CATALOG_ZONES_LABEL "\x05""zones"
#define CATALOG_SOA_REFRESH 3600
#define CATALOG_SOA_RETRY 600
#define CATALOG_SOA_EXPIRE (INT32_MAX - 1)

const MDB_val catalog_iter_prefix = { 1, "" };

knot_dname_t *catalog_member_owner(const knot_dname_t *member,
                                   const knot_dname_t *catzone,
                                   time_t member_time)
{
	SIPHASH_CTX hash;
	SIPHASH_KEY shkey = { 0 }; // only used for hashing -> zero key
	SipHash24_Init(&hash, &shkey);
	SipHash24_Update(&hash, member, knot_dname_size(member));
	uint64_t u64time = htobe64(member_time);
	SipHash24_Update(&hash, &u64time, sizeof(u64time));
	uint64_t hashres = SipHash24_End(&hash);

	char *hexhash = bin_to_hex((uint8_t *)&hashres, sizeof(hashres));
	if (hexhash == NULL) {
		return NULL;
	}
	size_t hexlen = strlen(hexhash);
	assert(hexlen == 16);
	size_t zoneslen = knot_dname_size((uint8_t *)CATALOG_ZONES_LABEL);
	assert(hexlen <= KNOT_DNAME_MAXLABELLEN && zoneslen <= KNOT_DNAME_MAXLABELLEN);
	size_t catzlen = knot_dname_size(catzone);

	size_t outlen = hexlen + zoneslen + catzlen;
	knot_dname_t *out;
	if (outlen > KNOT_DNAME_MAXLEN || (out = malloc(outlen)) == NULL) {
		free(hexhash);
		return NULL;
	}

	wire_ctx_t wire = wire_ctx_init(out, outlen);
	wire_ctx_write_u8(&wire, hexlen);
	wire_ctx_write(&wire, hexhash, hexlen);
	wire_ctx_write(&wire, CATALOG_ZONES_LABEL, zoneslen);
	wire_ctx_skip(&wire, -1);
	wire_ctx_write(&wire, catzone, catzlen);
	assert(wire.error == KNOT_EOK);

	free(hexhash);
	return out;
}

static bool check_zone_version(const zone_contents_t *zone)
{
	size_t zone_size = knot_dname_size(zone->apex->owner);
	knot_dname_t sub[zone_size + 8];
	memcpy(sub, "\x07""version", 8);
	memcpy(sub + 8, zone->apex->owner, zone_size);

	const zone_node_t *ver_node = zone_contents_find_node(zone, sub);
	knot_rdataset_t *ver_rr = node_rdataset(ver_node, KNOT_RRTYPE_TXT);
	if (ver_rr == NULL) {
		return false;
	}

	knot_rdata_t *rd = ver_rr->rdata;
	for (int i = 0; i < ver_rr->count; i++) {
		if (rd->len == 2 && rd->data[1] == CATALOG_ZONE_VERSION[0]) {
			return true;
		}
		rd = knot_rdataset_next(rd);
	}
	return false;
}

void catalog_init(catalog_t *cat, const char *path, size_t mapsize)
{
	knot_lmdb_init(&cat->db, path, mapsize, MDB_NOTLS, NULL);
}

// does NOT check for catalog zone version by RFC, this is Knot-specific in the cat LMDB !
static void check_cat_version(catalog_t *cat)
{
	if (cat->ro_txn->ret == KNOT_EOK) {
		MDB_val key = { 8, "\x01version" };
		if (knot_lmdb_find(cat->ro_txn, &key, KNOT_LMDB_EXACT)) {
			if (strncmp(CATALOG_VERSION, cat->ro_txn->cur_val.mv_data,
			            cat->ro_txn->cur_val.mv_size) != 0) {
				log_warning("unmatching catalog version");
			}
		} else if (cat->rw_txn != NULL) {
			MDB_val val = { strlen(CATALOG_VERSION), CATALOG_VERSION };
			knot_lmdb_insert(cat->rw_txn, &key, &val);
		}
	}
}

int catalog_open(catalog_t *cat)
{
	if (!knot_lmdb_is_open(&cat->db)) {
		int ret = knot_lmdb_open(&cat->db);
		if (ret != KNOT_EOK) {
			return ret;
		}
	}
	if (cat->ro_txn == NULL) {
		knot_lmdb_txn_t *ro_txn = calloc(1, sizeof(*ro_txn));
		if (ro_txn == NULL) {
			return KNOT_ENOMEM;
		}
		knot_lmdb_begin(&cat->db, ro_txn, false);
		cat->ro_txn = ro_txn;
	}
	check_cat_version(cat);
	return cat->ro_txn->ret;
}

int catalog_begin(catalog_t *cat)
{
	int ret = catalog_open(cat);
	if (ret != KNOT_EOK) {
		return ret;
	}
	knot_lmdb_txn_t *rw_txn = calloc(1, sizeof(*rw_txn));
	if (rw_txn == NULL) {
		return KNOT_ENOMEM;
	}
	knot_lmdb_begin(&cat->db, rw_txn, true);
	if (rw_txn->ret != KNOT_EOK) {
		ret = rw_txn->ret;
		free(rw_txn);
		return ret;
	}
	assert(cat->rw_txn == NULL); // LMDB prevents two existing RW txns at a time
	cat->rw_txn = rw_txn;
	check_cat_version(cat);
	return cat->rw_txn->ret;
}

int catalog_commit(catalog_t *cat)
{
	knot_lmdb_txn_t *rw_txn = rcu_xchg_pointer(&cat->rw_txn, NULL);
	knot_lmdb_commit(rw_txn);
	int ret = rw_txn->ret;
	free(rw_txn);
	if (ret != KNOT_EOK) {
		return ret;
	}

	// now refresh RO txn
	knot_lmdb_txn_t *ro_txn = calloc(1, sizeof(*ro_txn));
	if (ro_txn == NULL) {
		return KNOT_ENOMEM;
	}
	knot_lmdb_begin(&cat->db, ro_txn, false);
	cat->old_ro_txn = rcu_xchg_pointer(&cat->ro_txn, ro_txn);

	return KNOT_EOK;
}

void catalog_commit_cleanup(catalog_t *cat)
{
	knot_lmdb_txn_t *old_ro_txn = rcu_xchg_pointer(&cat->old_ro_txn, NULL);
	if (old_ro_txn != NULL) {
		knot_lmdb_abort(old_ro_txn);
		free(old_ro_txn);
	}
}

int catalog_deinit(catalog_t *cat)
{
	assert(cat->rw_txn == NULL);
	if (cat->ro_txn != NULL) {
		knot_lmdb_abort(cat->ro_txn);
		free(cat->ro_txn);
	}
	if (cat->old_ro_txn != NULL) {
		knot_lmdb_abort(cat->old_ro_txn);
		free(cat->old_ro_txn);
	}
	knot_lmdb_deinit(&cat->db);
	return KNOT_EOK;
}

static int bailiwick_shift(const knot_dname_t *subname, const knot_dname_t *name)
{
	const knot_dname_t *res = subname;
	while (!knot_dname_is_equal(res, name)) {
		if (*res == '\0') {
			return -1;
		}
		res = knot_wire_next_label(res, NULL);
	}
	return res - subname;
}

int catalog_add(catalog_t *cat, const knot_dname_t *member,
                const knot_dname_t *owner, const knot_dname_t *catzone,
                uint32_t ord)
{
	if (cat->rw_txn == NULL) {
		return KNOT_EINVAL;
	}
	int bail = bailiwick_shift(owner, catzone);
	if (bail < 0) {
		return KNOT_EOUTOFZONE;
	}
	assert(bail >= 0 && bail < 256);
	MDB_val key = knot_lmdb_make_key("BNI", 0, member, ord); // 0 for future purposes
	MDB_val val = knot_lmdb_make_key("BBN", 0, bail, owner);

	knot_lmdb_insert(cat->rw_txn, &key, &val);
	free(key.mv_data);
	free(val.mv_data);
	return cat->rw_txn->ret;
}

int catalog_del(catalog_t *cat, const knot_dname_t *member, uint32_t ord)
{
	if (cat->rw_txn == NULL) {
		return KNOT_EINVAL;
	}
	MDB_val key = knot_lmdb_make_key("BNI", 0, member, ord);
	knot_lmdb_del_prefix(cat->rw_txn, &key);
	free(key.mv_data);
	return cat->rw_txn->ret;
}

static void catalog_curval(MDB_val *key, MDB_val *val, const knot_dname_t **member,
                           const knot_dname_t **owner, const knot_dname_t **catzone,
                           uint32_t *ord)
{
	uint8_t zero, shift;
	if (member != NULL) {
		knot_lmdb_unmake_key(key->mv_data, key->mv_size,
		                     "BNI", &zero, member, ord);
	}
	const knot_dname_t *ow;
	knot_lmdb_unmake_key(val->mv_data, val->mv_size, "BBN", &zero, &shift, &ow);
	if (owner != NULL) {
		*owner = ow;
	}
	if (catzone != NULL) {
		*catzone = ow + shift;
	}
}

typedef struct {
	catalog_apply_cb_t cb;
	void *ctx;
	bool shadowed;
	const knot_dname_t *last_member;
} catalog_apply_ctx_t;

static int catalog_apply_cb(MDB_val *key, MDB_val *val, void *ctx)
{
	catalog_apply_ctx_t *iter_ctx = ctx;
	const knot_dname_t *mem, *ow, *cz;
	uint32_t ord = 0;
	catalog_curval(key, val, &mem, &ow, &cz, &ord);
	if (iter_ctx->shadowed || !knot_dname_is_equal(mem, iter_ctx->last_member)) {
		iter_ctx->last_member = mem;
		return iter_ctx->cb(mem, ow, cz, ord, iter_ctx->ctx);
	} else {
		return KNOT_EOK;
	}
}

int catalog_apply(catalog_t *cat, const knot_dname_t *for_member,
		  catalog_apply_cb_t cb, void *ctx, bool rw, bool shadowed)
{
	MDB_val prefix = knot_lmdb_make_key(for_member == NULL ? "B" : "BN", 0, for_member);
	catalog_apply_ctx_t iter_ctx = { cb, ctx, shadowed, NULL };
	knot_lmdb_txn_t *use_txn = rw ? cat->rw_txn : cat->ro_txn;
	int ret = knot_lmdb_apply_threadsafe(use_txn, &prefix, true, catalog_apply_cb, &iter_ctx);
	free(prefix.mv_data);
	return ret;
}

bool catalog_has_member(catalog_t *cat, const knot_dname_t *member)
{
	if (cat->ro_txn == NULL) {
		return false;
	}
	MDB_val prefix = knot_lmdb_make_key("BN", 0, member);
	bool res = knot_lmdb_find_prefix(cat->ro_txn, &prefix);
	free(prefix.mv_data);
	return res;
}

typedef struct {
	catalog_upd_val_t **out_list;
	catalog_upd_val_t **out_end;
} cmtu_ctx_t;

static int cmtu_add(const knot_dname_t *mem, const knot_dname_t *ow,
                    const knot_dname_t *catz, uint32_t ord, void *ctx)
{
	catalog_upd_val_t *val = malloc(sizeof(*val));
	if (val == NULL) {
		return KNOT_ENOMEM;
	}
	val->member  = (knot_dname_t *)mem;
	val->owner   = (knot_dname_t *)ow;
	val->catzone = (knot_dname_t *)catz;
	val->ord     = ord;
	val->type    = MEMB_UPD_ORIG;

	cmtu_ctx_t *cmtu = ctx;
	if (*cmtu->out_list == NULL) {
		*cmtu->out_list = val;
		*cmtu->out_end  = val;
	} else {
		(*cmtu->out_end)->next = val;
		*cmtu->out_end  = val;
	}
	return KNOT_EOK;
}

int catalog_member_to_update(catalog_t *cat, const knot_dname_t *member,
                             catalog_upd_val_t **out_list, catalog_upd_val_t **out_end)
{
	if (cat->ro_txn == NULL) {
		return KNOT_ENOENT;
	}
	cmtu_ctx_t cmtu = { out_list, out_end };
	int ret = catalog_apply(cat, member, cmtu_add, &cmtu, false, true);
	if (ret == KNOT_EOK && *out_list == NULL) {
		ret = KNOT_ENOENT;
	}
	return ret;
}

static const knot_dname_t *get_uniq(const knot_dname_t *ptr_owner,
                                    const knot_dname_t *catz)
{
	int labels = knot_dname_labels(ptr_owner, NULL);
	labels -= knot_dname_labels(catz, NULL);
	assert(labels >= 2);
	return ptr_owner + knot_dname_prefixlen(ptr_owner, labels - 2, NULL);
}

static bool same_uniq(const knot_dname_t *owner1, const knot_dname_t *catz1,
                      const knot_dname_t *owner2, const knot_dname_t *catz2)
{
	const knot_dname_t *uniq1 = get_uniq(owner1, catz1), *uniq2 = get_uniq(owner2, catz2);
	if (*uniq1 != *uniq2) {
		return false;
	}
	return memcmp(uniq1 + 1, uniq2 + 1, *uniq1) == 0;
}

static int get_zone_cb(const knot_dname_t *mem, const knot_dname_t *ow,
                       const knot_dname_t *catz, uint32_t ord, void *ctx)
{
	UNUSED(mem);
	UNUSED(ow);
	UNUSED(ord);
	uint8_t *storage = ctx;
	assert(storage[0] == 0);
	if (knot_dname_store(storage, catz) == 0) {
		return KNOT_EINVAL;
	}
	assert(storage[0] != 0);
	return KNOT_EOK;
}

int catalog_get_zone_threadsafe(catalog_t *cat, const knot_dname_t *member,
                                knot_dname_storage_t catzone)
{
	if (cat->ro_txn == NULL) {
		return KNOT_ENOENT;
	}

	memset(catzone, 0, sizeof(knot_dname_storage_t));

	int ret = catalog_apply(cat, member, get_zone_cb, (uint8_t *)catzone, false, false);
	if (ret == KNOT_EOK && catzone[0] == 0) {
		ret = KNOT_ENOENT;
	}

	return ret;
}

inline static bool same_catalog(knot_lmdb_txn_t *txn, const knot_dname_t *catalog)
{
	if (catalog == NULL) {
		return true;
	}
	const knot_dname_t *txn_cat = NULL;
	uint32_t ord = 0;
	catalog_curval(&txn->cur_key, &txn->cur_val, NULL, NULL, &txn_cat, &ord);
	return knot_dname_is_equal(txn_cat, catalog);
}

int catalog_copy(knot_lmdb_db_t *from, knot_lmdb_db_t *to,
                 const knot_dname_t *zone_only, bool read_rw_txn)
{
	if (!knot_lmdb_exists(from)) {
		return KNOT_EOK;
	}
	int ret = knot_lmdb_open(from);
	if (ret == KNOT_EOK) {
		ret = knot_lmdb_open(to);
	}
	if (ret != KNOT_EOK) {
		return ret;
	}
	knot_lmdb_txn_t txn_r = { 0 }, txn_w = { 0 };
	knot_lmdb_begin(from, &txn_r, read_rw_txn); // using RW txn not to conflict with still-open RO txn
	knot_lmdb_begin(to, &txn_w, true);
	knot_lmdb_foreach(&txn_w, (MDB_val *)&catalog_iter_prefix) {
		if (same_catalog(&txn_w, zone_only)) {
			knot_lmdb_del_cur(&txn_w);
		}
	}
	knot_lmdb_foreach(&txn_r, (MDB_val *)&catalog_iter_prefix) {
		if (same_catalog(&txn_r, zone_only)) {
			knot_lmdb_insert(&txn_w, &txn_r.cur_key, &txn_r.cur_val);
		}
	}
	if (txn_r.ret != KNOT_EOK) {
		knot_lmdb_abort(&txn_r);
		knot_lmdb_abort(&txn_w);
		return txn_r.ret;
	}
	knot_lmdb_commit(&txn_r);
	knot_lmdb_commit(&txn_w);
	return txn_w.ret;
}

int catalog_update_init(catalog_update_t *u)
{
	u->upd = trie_create(NULL);
	if (u->upd == NULL) {
		return KNOT_ENOMEM;
	}
	pthread_mutex_init(&u->mutex, 0);
	u->error = KNOT_EOK;
	return KNOT_EOK;
}

catalog_update_t *catalog_update_new()
{
	catalog_update_t *u = calloc(1, sizeof(*u));
	if (u != NULL) {
		int ret = catalog_update_init(u);
		if (ret != KNOT_EOK) {
			free(u);
			u = NULL;
		}
	}
	return u;
}

static int freecb(trie_val_t *tval, void *unused)
{
	UNUSED(unused);
	catalog_upd_val_t *val = *tval;
	while (val != NULL) {
		catalog_upd_val_t *next = val->next;
		free(val);
		val = next;
	}
	return 0;
}

void catalog_update_clear(catalog_update_t *u)
{
	trie_apply(u->upd, freecb, NULL);
	trie_clear(u->upd);
	u->error = KNOT_EOK;
}

void catalog_update_deinit(catalog_update_t *u)
{
	pthread_mutex_destroy(&u->mutex);
	trie_free(u->upd);
}

void catalog_update_free(catalog_update_t *u)
{
	if (u != NULL) {
		catalog_update_deinit(u);
		free(u);
	}
}

static catalog_upd_val_t *new_upd_val(const knot_dname_t *member,
                                      const knot_dname_t *owner,
                                      size_t bail, uint32_t ord,
                                      catalog_upd_type_t type)
{
	size_t member_size = knot_dname_size(member);
	size_t owner_size = knot_dname_size(owner);
	assert(bail <= owner_size);

	catalog_upd_val_t *val = malloc(sizeof(*val) + member_size + owner_size);
	if (val == NULL) {
		return NULL;
	}
	val->member = (knot_dname_t *)(val + 1);
	val->owner = val->member + member_size;
	val->catzone = val->owner + bail;
	memcpy(val->member, member, member_size);
	memcpy(val->owner, owner, owner_size);
	val->type = type;
	val->next = NULL;
	val->ord = ord;
	return val;
}

static bool upd_cancel_out(catalog_upd_val_t **p_orig, const knot_dname_t *new_owner,
                           int new_bail, catalog_upd_type_t new_type)
{
	catalog_upd_val_t *orig = *p_orig;
	if (knot_dname_is_equal(orig->owner, new_owner) &&
	    orig->catzone - orig->owner == new_bail &&
	    orig->type != new_type) {
		// same member -- assumed
		// same owner -- compared
		// same catz -- "bail" compared
		// unequal type -- compared
		// opposite type -- assumed
		*p_orig = orig->next;
		free(orig);
		return true;
	}
	return false;
}

int catalog_update_add(catalog_update_t *u, const knot_dname_t *member,
                       const knot_dname_t *owner, const knot_dname_t *catzone,
                       bool remove)
{
	int bail = bailiwick_shift(owner, catzone);
	if (bail < 0) {
		return KNOT_EOUTOFZONE;
	}
	assert(bail >= 0 && bail < 256);

	knot_dname_storage_t lf_storage;
	uint8_t *lf = knot_dname_lf(member, lf_storage);

	catalog_upd_type_t type = remove ? MEMB_UPD_REM : MEMB_UPD_ADD;

	trie_val_t *found = trie_get_ins(u->upd, lf + 1, lf[0]);
	catalog_upd_val_t **add_to = (catalog_upd_val_t **)found;
	while (*add_to != NULL) {
		if (upd_cancel_out(add_to, owner, bail, type)) {
			if (*found == NULL) { // everything canceled out
				(void)trie_del(u->upd, lf + 1, lf[0], NULL);
			}
			return KNOT_EOK;
		}
		add_to = &(*add_to)->next;
	}
	// now add_to points at the end of the linked list

	*add_to = new_upd_val(member, owner, bail, 0xdeadbeef, type);
	if (*add_to == NULL) {
		return KNOT_ENOMEM;
	}
	return KNOT_EOK;
}

catalog_upd_val_t *catalog_update_get(catalog_update_t *u, const knot_dname_t *member)
{
	knot_dname_storage_t lf_storage;
	uint8_t *lf = knot_dname_lf(member, lf_storage);

	trie_val_t *found = trie_get_try(u->upd, lf + 1, lf[0]);
	return found == NULL ? NULL : *(catalog_upd_val_t **)found;
}

typedef struct {
	catalog_update_t *u;
	const knot_dname_t *apex;
	bool remove;
} cat_upd_ctx_t;

static int cat_update_add_node(zone_node_t *node, void *data)
{
	cat_upd_ctx_t *ctx = data;
	const knot_rdataset_t *ptr = node_rdataset(node, KNOT_RRTYPE_PTR);
	if (ptr == NULL || ptr->count == 0) {
		return KNOT_EOK;
	}
	knot_rdata_t *rdata = ptr->rdata;
	int ret = KNOT_EOK;
	for (int i = 0; ret == KNOT_EOK && i < ptr->count; i++) {
		const knot_dname_t *member = knot_ptr_name(rdata);
		ret = catalog_update_add(ctx->u, member, node->owner, ctx->apex, ctx->remove);
		rdata = knot_rdataset_next(rdata);
	}
	return ret;
}

static size_t dname_append(knot_dname_storage_t storage, const knot_dname_t *name)
{
	size_t old_len = knot_dname_size(storage);
	size_t name_len = knot_dname_size(name);
	size_t new_len = old_len - 1 + name_len;
	if (old_len == 0 || name_len == 0 || new_len > KNOT_DNAME_MAXLEN) {
		return 0;
	}
	memcpy(storage + old_len - 1, name, name_len);
	return new_len;
}

int catalog_update_from_zone(catalog_update_t *u, struct zone_contents *zone,
                             bool remove, bool check_ver)
{
	if (check_ver && !check_zone_version(zone)) {
		return KNOT_EZONEINVAL;
	}

	knot_dname_storage_t sub;
	if (knot_dname_store(sub, (uint8_t *)CATALOG_ZONES_LABEL) == 0 ||
	    dname_append(sub, zone->apex->owner ) == 0) {
		return KNOT_EINVAL;
	}

	if (zone_contents_find_node(zone, sub) == NULL) {
		return KNOT_EOK;
	}

	cat_upd_ctx_t ctx = { u, zone->apex->owner, remove };
	pthread_mutex_lock(&u->mutex);
	int ret = zone_tree_sub_apply(zone->nodes, sub, false, cat_update_add_node, &ctx);
	pthread_mutex_unlock(&u->mutex);
	return ret;
}

inline static bool same_ow_cat(catalog_upd_val_t *a, catalog_upd_val_t *b)
{
	return knot_dname_is_equal(a->owner, b->owner) &&
	       knot_dname_is_equal(a->catzone, b->catzone);
}

/*!
 * \brief TODO huge comment
 * \param val
 * \return
 */
static int finalize_member(catalog_upd_val_t *val)
{
	catalog_upd_val_t *final_cur = val, *orig_first = val, *i, *last = val;
	if (orig_first->type != MEMB_UPD_ORIG) {
		orig_first = NULL; // no record in catalog for this member yet
	}

	uint32_t next_ord = 0;

	while (val != NULL) { // iterate thru linked-list
		switch (val->type) {
		case MEMB_UPD_ORIG:
			next_ord = val->ord + 1;
			break;
		case MEMB_UPD_REM:
			// find what is being removed
			for (i = final_cur; i != val; i = i->next) {
				if ((i->type == MEMB_UPD_ORIG || i->type == MEMB_UPD_ADD) && same_ow_cat(i, val)) {
					// removal of existing record
					val->ord = i->ord;

					if (i->type == MEMB_UPD_ADD) { // addition cancels out with removal
						val->type = MEMB_UPD_INVALID;
						i->type = MEMB_UPD_INVALID;
					}

					if (i == final_cur) {
						do { // removal of target first record in cat
							final_cur = final_cur->next;
						} while (final_cur != NULL && final_cur->type != MEMB_UPD_ORIG && final_cur->type != MEMB_UPD_ADD);
					}
					break; // removal accepted, exit for loop
				}
			}
			if (i == val) { // nothing found to be removed
				val->type = MEMB_UPD_INVALID;
			}
			break;
		case MEMB_UPD_INVALID:
			break;
		case MEMB_UPD_ADD:
			val->ord = next_ord++;
			if (final_cur->type != MEMB_UPD_ORIG && final_cur->type != MEMB_UPD_ADD) {
				final_cur = val;
			}
			if (final_cur == val && orig_first != NULL) {
				val->type = MEMB_UPD_UPD_EFFECT;
			}
			break;
		default:
			return KNOT_ERROR;
		}
		last = val;
		val = val->next;
	}

	if (final_cur != orig_first && final_cur != NULL) { // the effective record for this member will be changed
		switch (final_cur->type) {
		case MEMB_UPD_ADD:
			final_cur->type = MEMB_UPD_ADD_EFFECT;
			break;
		case MEMB_UPD_ORIG:
			last->next = new_upd_val(final_cur->member, final_cur->owner,
			                         bailiwick_shift(final_cur->owner, final_cur->catzone),
			                         final_cur->ord, MEMB_UPD_UPD_EFFECT);
			if (last->next == NULL) {
				return KNOT_ENOENT;
			}
			assert(orig_first != NULL);
			if (!same_uniq(orig_first->owner, orig_first->catzone, final_cur->owner, final_cur->catzone)) {
				last->next->type = MEMB_UPD_UPD_PURGE;
			}
			break;
		case MEMB_UPD_UPD_EFFECT:
			assert(orig_first != NULL);
			if (!same_uniq(orig_first->owner, orig_first->catzone, final_cur->owner, final_cur->catzone)) {
				final_cur->type = MEMB_UPD_UPD_PURGE;
			}
			break;
		default:
			assert(0);
		}
	} else if (final_cur == NULL && orig_first != NULL) {
		for (val = orig_first; val != NULL; val = val->next) {
			if (val->type == MEMB_UPD_REM && same_ow_cat(orig_first, val)) {
				val->type = MEMB_UPD_REM_EFFECT;
				break;
			}
		}
		assert(val != NULL);
	}
	return KNOT_EOK;
}

int catalog_update_finalize(catalog_update_t *u, catalog_t *cat)
{
	if (u == NULL || cat == NULL) {
		return KNOT_EINVAL;
	}

	int ret = KNOT_EOK;
	catalog_it_t *it = catalog_it_begin(u);
	while (!catalog_it_finished(it) && ret == KNOT_EOK) {
		catalog_upd_val_t *val = catalog_it_val(it), *cat_list = NULL, *cat_end = NULL;
		ret = catalog_member_to_update(cat, val->member, &cat_list, &cat_end);
		if (ret == KNOT_ENOENT) {
			ret = finalize_member(val);
		} else if (ret == KNOT_EOK) {
			cat_end->next = val; // concatenate linked lists for cat and from cat_upd
			ret = finalize_member(cat_list);
			while (cat_list != val) { // free resources from catalog_member_to_update()
				cat_end = cat_list->next;
				free(cat_list);
				cat_list = cat_end;
			}
		}
		catalog_it_next(it);
	}
	catalog_it_free(it);

	return ret;
}

static void set_rdata(knot_rrset_t *rrset, uint8_t *data, uint16_t len)
{
	knot_rdata_init(rrset->rrs.rdata, len, data);
	rrset->rrs.size = knot_rdata_size(len);
}

struct zone_contents *catalog_update_to_zone(catalog_update_t *u, const knot_dname_t *catzone,
                                             uint32_t soa_serial)
{
	if (u->error != KNOT_EOK) {
		return NULL;
	}
	zone_contents_t *c = zone_contents_new(catzone, true);
	if (c == NULL) {
		return c;
	}

	zone_node_t *unused = NULL;
	uint8_t invalid[9] = "\x07""invalid";
	uint8_t version[9] = "\x07""version";
	uint8_t cat_version[2] = "\x01" CATALOG_ZONE_VERSION;

	// prepare common rrset with one rdata item
	uint8_t rdata[256] = { 0 };
	knot_rrset_t rrset;
	knot_rrset_init(&rrset, (knot_dname_t *)catzone, KNOT_RRTYPE_SOA, KNOT_CLASS_IN, 0);
	rrset.rrs.rdata = (knot_rdata_t *)rdata;
	rrset.rrs.count = 1;

	// set catalog zone's SOA
	uint8_t data[250];
	assert(sizeof(knot_rdata_t) + sizeof(data) <= sizeof(rdata));
	wire_ctx_t wire = wire_ctx_init(data, sizeof(data));
	wire_ctx_write(&wire, invalid, sizeof(invalid));
	wire_ctx_write(&wire, invalid, sizeof(invalid));
	wire_ctx_write_u32(&wire, soa_serial);
	wire_ctx_write_u32(&wire, CATALOG_SOA_REFRESH);
	wire_ctx_write_u32(&wire, CATALOG_SOA_RETRY);
	wire_ctx_write_u32(&wire, CATALOG_SOA_EXPIRE);
	wire_ctx_write_u32(&wire, 0);
	set_rdata(&rrset, data, wire_ctx_offset(&wire));
	if (zone_contents_add_rr(c, &rrset, &unused) != KNOT_EOK) {
		goto fail;
	}

	// set catalog zone's NS
	unused = NULL;
	rrset.type = KNOT_RRTYPE_NS;
	set_rdata(&rrset, invalid, sizeof(invalid));
	if (zone_contents_add_rr(c, &rrset, &unused) != KNOT_EOK) {
		goto fail;
	}

	// set catalog zone's version TXT
	unused = NULL;
	knot_dname_storage_t owner;
	if (knot_dname_store(owner, version) == 0 || dname_append(owner, catzone) == 0) {
		goto fail;
	}
	rrset.owner = owner;
	rrset.type = KNOT_RRTYPE_TXT;
	set_rdata(&rrset, cat_version, sizeof(cat_version));
	if (zone_contents_add_rr(c, &rrset, &unused) != KNOT_EOK) {
		goto fail;
	}

	// insert member zone PTR records
	rrset.type = KNOT_RRTYPE_PTR;
	catalog_it_t *it = catalog_it_begin(u);
	while (!catalog_it_finished(it)) {
		catalog_upd_val_t *val = catalog_it_val(it);
		rrset.owner = val->owner;
		set_rdata(&rrset, val->member, knot_dname_size(val->member));
		unused = NULL;
		if (zone_contents_add_rr(c, &rrset, &unused) != KNOT_EOK) {
			goto fail;
		}
		catalog_it_next(it);
	}
	catalog_it_free(it);

	return c;

fail:
	zone_contents_deep_free(c);
	return NULL;
}

int catalog_update_to_update(catalog_update_t *u, struct zone_update *zu)
{
	knot_rrset_t ptr;
	knot_rrset_init(&ptr, NULL, KNOT_RRTYPE_PTR, KNOT_CLASS_IN, 0);
	uint8_t tmp[KNOT_DNAME_MAXLEN + sizeof(knot_rdata_t)];
	ptr.rrs.rdata = (knot_rdata_t *)tmp;
	ptr.rrs.count = 1;

	int ret = u->error;
	catalog_it_t *it = catalog_it_begin(u);
	while (!catalog_it_finished(it) && ret == KNOT_EOK) {
		catalog_upd_val_t *val = catalog_it_val(it);
		bool same_cat = knot_dname_is_equal(zu->zone->name, val->catzone);
		ptr.owner = val->owner;
		set_rdata(&ptr, val->member, knot_dname_size(val->member));
		switch (val->type) {
		case MEMB_UPD_ADD:
			if (same_cat) {
				ret = zone_update_add(zu, &ptr);
			}
			break;
		case MEMB_UPD_REM:
			if (same_cat) {
				ret = zone_update_remove(zu, &ptr);
			}
			break;
		default:
			ret = KNOT_EINVAL;
		}
		catalog_it_next(it);
	}
	catalog_it_free(it);
	return ret;
}

typedef struct {
	const knot_dname_t *zone;
	catalog_update_t *u;
} del_all_ctx_t;

static int del_all_cb(MDB_val *key, MDB_val *val, void *dactx)
{
	const knot_dname_t *mem, *ow, *cz;
	uint32_t ord = 0;
	catalog_curval(key, val, &mem, &ow, &cz, &ord);
	del_all_ctx_t *ctx = dactx;
	if (knot_dname_is_equal(cz, ctx->zone)) {
		// TODO possible speedup by indexing which member zones belong to a catalog zone
		return catalog_update_add(ctx->u, mem, ow, cz, true);
	} else {
		return KNOT_EOK;
	}
}

int catalog_update_del_all(catalog_update_t *u, catalog_t *cat, const knot_dname_t *zone)
{
	int ret = catalog_open(cat);
	if (ret != KNOT_EOK) {
		return ret;
	}

	pthread_mutex_lock(&u->mutex);
	del_all_ctx_t ctx = { zone, u };
	ret = knot_lmdb_apply_threadsafe(cat->ro_txn, &catalog_iter_prefix, true, del_all_cb, &ctx);
	pthread_mutex_unlock(&u->mutex);
	return ret;
}

int catalog_update_commit(catalog_update_t *u, catalog_t *cat)
{
	catalog_it_t *it = catalog_it_begin(u);
	if (catalog_it_finished(it)) {
		catalog_it_free(it);
		return KNOT_EOK;
	}
	int ret = catalog_begin(cat);
	while (!catalog_it_finished(it) && ret == KNOT_EOK) {
		catalog_upd_val_t *val = catalog_it_val(it);
		while (val != NULL) {
			switch (val->type) {
			case MEMB_UPD_ADD:
			case MEMB_UPD_ADD_EFFECT:
				ret = catalog_add(cat, val->member, val->owner, val->catzone, val->ord);
				break;
			case MEMB_UPD_UPD_EFFECT:
			case MEMB_UPD_UPD_PURGE:
				ret = catalog_add(cat, val->member, val->owner, val->catzone, val->ord); // possible re-add with same ord
				break; // TODO check that already first in catalog ?
			case MEMB_UPD_REM:
			case MEMB_UPD_REM_EFFECT:
				ret = catalog_del(cat, val->member, val->ord);
				break;
			case MEMB_UPD_INVALID:
				break; // no action
			default:
				assert(0);
			}
			val = val->next;
		}
		catalog_it_next(it);
	}
	catalog_it_free(it);
	if (ret == KNOT_EOK) {
		ret = catalog_commit(cat);
	}
	return KNOT_EOK;
}

static void print_dname(const knot_dname_t *d)
{
	knot_dname_txt_storage_t tmp;
	knot_dname_to_str(tmp, d, sizeof(tmp));
	printf("%s  ", tmp);
}

static void print_dname3(const char *prefix, const knot_dname_t *a, const knot_dname_t *b,
                         const knot_dname_t *c, uint32_t ord)
{
	printf("%s", prefix);
	print_dname(a);
	print_dname(b);
	print_dname(c);
	printf("%u\n", ord);
}

static int catalog_print_cb(const knot_dname_t *mem, const knot_dname_t *ow,
                            const knot_dname_t *cz, uint32_t ord, void *ctx)
{
	print_dname3("", mem, ow, cz, ord);
	(*(ssize_t *)ctx)++;
	return KNOT_EOK;
}

void catalog_print(catalog_t *cat)
{
	ssize_t total = 0;

	printf(";; <catalog zone> <record owner> <record zone> <ord_number>\n");

	if (cat != NULL) {
		int ret = catalog_open(cat);
		if (ret == KNOT_EOK) {
			ret = catalog_apply(cat, NULL, catalog_print_cb, &total, false, true);
		}
		if (ret != KNOT_EOK) {
			printf("Catalog print failed (%s)\n", knot_strerror(ret));
			return;
		}
	}

	printf("Total records: %zd\n", total);
}

void catalog_update_print(catalog_update_t *u)
{
	const static char* sign[MEMB_UPD_MAX] = { " !", " +", " -", " @", "+@", " ^", "^!", "-@" };
	ssize_t counts[MEMB_UPD_MAX] = { 0 };

	printf(";; <catalog zone> <record owner> <record zone> <ord_number>\n");

	if (u != NULL) {
		catalog_it_t *it = catalog_it_begin(u);
		while (!catalog_it_finished(it)) {
			catalog_upd_val_t *val = catalog_it_val(it);
			while (val != NULL) {
				assert(val->type < MEMB_UPD_MAX);
				print_dname3(sign[val->type], val->member, val->owner, val->catzone, val->ord);
				counts[val->type]++;
				val = val->next;
			}
			catalog_it_next(it);
		}
		catalog_it_free(it);
	}

	printf("Total changes:");
	for (int i = 1; i < MEMB_UPD_MAX; i++) {
		printf(" %s%zd", sign[i], counts[i]);
	}
	printf("\n");
}
