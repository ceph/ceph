// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Inktank Storage, Inc.
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "common/errno.h"
#include "ReplicatedBackend.h"
#include "ECBackend.h"
#include "PGBackend.h"
#include "OSD.h"
#include "erasure-code/ErasureCodePlugin.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, PGBackend *pgb)
{
    return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

// -- ObjectModDesc --
struct RollbackVisitor : public ObjectModDesc::Visitor
{
    const hobject_t &hoid;
    PGBackend *pg;
    ObjectStore::Transaction t;
    RollbackVisitor(
        const hobject_t &hoid,
        PGBackend *pg) : hoid(hoid), pg(pg) {}
    void append(uint64_t old_size)
    {
        ObjectStore::Transaction temp;
        pg->rollback_append(hoid, old_size, &temp);
        temp.append(t);
        temp.swap(t);
    }
    void setattrs(map<string, boost::optional<bufferlist> > &attrs)
    {
        ObjectStore::Transaction temp;
        pg->rollback_setattrs(hoid, attrs, &temp);
        temp.append(t);
        temp.swap(t);
    }
    void rmobject(version_t old_version)
    {
        ObjectStore::Transaction temp;
        pg->rollback_stash(hoid, old_version, &temp);
        temp.append(t);
        temp.swap(t);
    }
    void create()
    {
        ObjectStore::Transaction temp;
        pg->rollback_create(hoid, &temp);
        temp.append(t);
        temp.swap(t);
    }
    void update_snaps(set<snapid_t> &snaps)
    {
        // pass
    }
};

void PGBackend::rollback(
    const hobject_t &hoid,
    const ObjectModDesc &desc,
    ObjectStore::Transaction *t)
{
    assert(desc.can_rollback());
    RollbackVisitor vis(hoid, this);
    desc.visit(&vis);
    t->append(vis.t);
}


void PGBackend::on_change_cleanup(ObjectStore::Transaction *t)
{
    dout(10) << __func__ << dendl;
    // clear temp
    for (set<hobject_t, hobject_t::BitwiseComparator>::iterator i = temp_contents.begin();
            i != temp_contents.end();
            ++i)
    {
        dout(10) << __func__ << ": Removing oid "
                 << *i << " from the temp collection" << dendl;
        t->remove(
            coll,
            ghobject_t(*i, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
    }
    temp_contents.clear();
}

int PGBackend::objects_list_partial(
    const hobject_t &begin,
    int min,
    int max,
    vector<hobject_t> *ls,
    hobject_t *next)
{
    assert(ls);
    // Starts with the smallest generation to make sure the result list
    // has the marker object (it might have multiple generations
    // though, which would be filtered).
    ghobject_t _next;
    if (!begin.is_min())
        _next = ghobject_t(begin, 0, get_parent()->whoami_shard().shard);
    ls->reserve(max);
    int r = 0;
    while (!_next.is_max() && ls->size() < (unsigned)min)
    {
        vector<ghobject_t> objects;
        int r = store->collection_list(
                    coll,
                    _next,
                    ghobject_t::get_max(),
                    parent->sort_bitwise(),
                    max - ls->size(),
                    &objects,
                    &_next);
        if (r != 0)
            break;
        for (vector<ghobject_t>::iterator i = objects.begin();
                i != objects.end();
                ++i)
        {
            if (i->is_pgmeta() || i->hobj.is_temp())
            {
                continue;
            }
            if (i->is_no_gen())
            {
                ls->push_back(i->hobj);
            }
        }
    }
    if (r == 0)
        *next = _next.hobj;
    return r;
}

int PGBackend::objects_list_range(
    const hobject_t &start,
    const hobject_t &end,
    snapid_t seq,
    vector<hobject_t> *ls,
    vector<ghobject_t> *gen_obs)
{
    assert(ls);
    vector<ghobject_t> objects;
    int r = store->collection_list(
                coll,
                ghobject_t(start, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
                ghobject_t(end, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
                parent->sort_bitwise(),
                INT_MAX,
                &objects,
                NULL);
    ls->reserve(objects.size());
    for (vector<ghobject_t>::iterator i = objects.begin();
            i != objects.end();
            ++i)
    {
        if (i->is_pgmeta() || i->hobj.is_temp())
        {
            continue;
        }
        if (i->is_no_gen())
        {
            ls->push_back(i->hobj);
        }
        else if (gen_obs)
        {
            gen_obs->push_back(*i);
        }
    }
    return r;
}

int PGBackend::objects_get_attr(
    const hobject_t &hoid,
    const string &attr,
    bufferlist *out)
{
    bufferptr bp;
    int r = store->getattr(
                coll,
                ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
                attr.c_str(),
                bp);
    if (r >= 0 && out)
    {
        out->clear();
        out->push_back(bp);
    }
    return r;
}

int PGBackend::objects_get_attrs(
    const hobject_t &hoid,
    map<string, bufferlist> *out)
{
    return store->getattrs(
               coll,
               ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
               *out);
}

void PGBackend::rollback_setattrs(
    const hobject_t &hoid,
    map<string, boost::optional<bufferlist> > &old_attrs,
    ObjectStore::Transaction *t)
{
    map<string, bufferlist> to_set;
    assert(!hoid.is_temp());
    for (map<string, boost::optional<bufferlist> >::iterator i = old_attrs.begin();
            i != old_attrs.end();
            ++i)
    {
        if (i->second)
        {
            to_set[i->first] = i->second.get();
        }
        else
        {
            t->rmattr(
                coll,
                ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
                i->first);
        }
    }
    t->setattrs(
        coll,
        ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
        to_set);
}

void PGBackend::rollback_append(
    const hobject_t &hoid,
    uint64_t old_size,
    ObjectStore::Transaction *t)
{
    assert(!hoid.is_temp());
    t->truncate(
        coll,
        ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
        old_size);
}

void PGBackend::rollback_stash(
    const hobject_t &hoid,
    version_t old_version,
    ObjectStore::Transaction *t)
{
    assert(!hoid.is_temp());
    t->remove(
        coll,
        ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
    t->collection_move_rename(
        coll,
        ghobject_t(hoid, old_version, get_parent()->whoami_shard().shard),
        coll,
        ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
}

void PGBackend::rollback_create(
    const hobject_t &hoid,
    ObjectStore::Transaction *t)
{
    assert(!hoid.is_temp());
    t->remove(
        coll,
        ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
}

void PGBackend::trim_stashed_object(
    const hobject_t &hoid,
    version_t old_version,
    ObjectStore::Transaction *t)
{
    assert(!hoid.is_temp());
    t->remove(
        coll, ghobject_t(hoid, old_version, get_parent()->whoami_shard().shard));
}

PGBackend *PGBackend::build_pg_backend(
    const pg_pool_t &pool,
    const OSDMapRef curmap,
    Listener *l,
    coll_t coll,
    ObjectStore *store,
    CephContext *cct)
{
    switch (pool.type)
    {
    case pg_pool_t::TYPE_REPLICATED:
    {
        return new ReplicatedBackend(l, coll, store, cct);
    }
    case pg_pool_t::TYPE_ERASURE:
    {
        ErasureCodeInterfaceRef ec_impl;
        ErasureCodeProfile profile = curmap->get_erasure_code_profile(pool.erasure_code_profile);
        assert(profile.count("plugin"));
        stringstream ss;
        ceph::ErasureCodePluginRegistry::instance().factory(
            profile.find("plugin")->second,
            g_conf->erasure_code_dir,
            profile,
            &ec_impl,
            &ss);
        assert(ec_impl);
        return new ECBackend(
                   l,
                   coll,
                   store,
                   cct,
                   ec_impl,
                   pool.stripe_width);
    }
    default:
        assert(0);
        return NULL;
    }
}

/*
 * pg lock may or may not be held
 */
void PGBackend::be_scan_list(
    ScrubMap &map, const vector<hobject_t> &ls, bool deep, uint32_t seed,
    ThreadPool::TPHandle &handle)
{
    dout(10) << __func__ << " scanning " << ls.size() << " objects"
             << (deep ? " deeply" : "") << dendl;
    int i = 0;
    for (vector<hobject_t>::const_iterator p = ls.begin();
            p != ls.end();
            ++p, i++)
    {
        handle.reset_tp_timeout();
        hobject_t poid = *p;

        struct stat st;
        int r = store->stat(
                    coll,
                    ghobject_t(
                        poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
                    &st,
                    true);
        if (r == 0)
        {
            ScrubMap::object &o = map.objects[poid];
            o.size = st.st_size;
            assert(!o.negative);
            store->getattrs(
                coll,
                ghobject_t(
                    poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
                o.attrs);

            // calculate the CRC32 on deep scrubs
            if (deep)
            {
                be_deep_scrub(*p, seed, o, handle);
            }

            dout(25) << __func__ << "  " << poid << dendl;
        }
        else if (r == -ENOENT)
        {
            dout(25) << __func__ << "  " << poid << " got " << r
                     << ", skipping" << dendl;
        }
        else if (r == -EIO)
        {
            dout(25) << __func__ << "  " << poid << " got " << r
                     << ", read_error" << dendl;
            ScrubMap::object &o = map.objects[poid];
            o.read_error = true;
        }
        else
        {
            derr << __func__ << " got: " << cpp_strerror(r) << dendl;
            assert(0);
        }
    }
}

enum scrub_error_type PGBackend::be_compare_scrub_objects(
    pg_shard_t auth_shard,
    const ScrubMap::object &auth,
    const object_info_t& auth_oi,
    bool okseed,
    const ScrubMap::object &candidate,
    ostream &errorstream)
{
    enum scrub_error_type error = CLEAN;
    if (candidate.read_error)
    {
        // This can occur on stat() of a shallow scrub, but in that case size will
        // be invalid, and this will be over-ridden below.
        error = DEEP_ERROR;
        errorstream << "candidate had a read error";
    }
    if (auth.digest_present && candidate.digest_present)
    {
        if (auth.digest != candidate.digest)
        {
            if (error != CLEAN)
                errorstream << ", ";
            error = DEEP_ERROR;
            bool known = okseed && auth_oi.is_data_digest() &&
                         auth.digest == auth_oi.data_digest;
            errorstream << "data_digest 0x" << std::hex << candidate.digest
                        << " != "
                        << (known ? "known" : "best guess")
                        << " data_digest 0x" << auth.digest << std::dec
                        << " from auth shard " << auth_shard;
        }
    }
    if (auth.omap_digest_present && candidate.omap_digest_present)
    {
        if (auth.omap_digest != candidate.omap_digest)
        {
            if (error != CLEAN)
                errorstream << ", ";
            error = DEEP_ERROR;
            bool known = okseed && auth_oi.is_omap_digest() &&
                         auth.digest == auth_oi.omap_digest;
            errorstream << "omap_digest 0x" << std::hex << candidate.omap_digest
                        << " != "
                        << (known ? "known" : "best guess")
                        << " omap_digest 0x" << auth.omap_digest << std::dec
                        << " from auth shard " << auth_shard;
        }
    }
    // Shallow error takes precendence because this will be seen by
    // both types of scrubs.
    if (auth.size != candidate.size)
    {
        if (error != CLEAN)
            errorstream << ", ";
        error = SHALLOW_ERROR;
        errorstream << "size " << candidate.size
                    << " != known size " << auth.size;
    }
    for (map<string,bufferptr>::const_iterator i = auth.attrs.begin();
            i != auth.attrs.end();
            ++i)
    {
        if (!candidate.attrs.count(i->first))
        {
            if (error != CLEAN)
                errorstream << ", ";
            error = SHALLOW_ERROR;
            errorstream << "missing attr " << i->first;
        }
        else if (candidate.attrs.find(i->first)->second.cmp(i->second))
        {
            if (error != CLEAN)
                errorstream << ", ";
            error = SHALLOW_ERROR;
            errorstream << "attr value mismatch " << i->first;
        }
    }
    for (map<string,bufferptr>::const_iterator i = candidate.attrs.begin();
            i != candidate.attrs.end();
            ++i)
    {
        if (!auth.attrs.count(i->first))
        {
            if (error != CLEAN)
                errorstream << ", ";
            error = SHALLOW_ERROR;
            errorstream << "extra attr " << i->first;
        }
    }
    return error;
}

map<pg_shard_t, ScrubMap *>::const_iterator
PGBackend::be_select_auth_object(
    const hobject_t &obj,
    const map<pg_shard_t,ScrubMap*> &maps,
    bool okseed,
    object_info_t *auth_oi)
{
    map<pg_shard_t, ScrubMap *>::const_iterator auth = maps.end();
    for (map<pg_shard_t, ScrubMap *>::const_iterator j = maps.begin();
            j != maps.end();
            ++j)
    {
        map<hobject_t, ScrubMap::object, hobject_t::BitwiseComparator>::iterator i =
            j->second->objects.find(obj);
        if (i == j->second->objects.end())
        {
            continue;
        }
        if (i->second.read_error)
        {
            // scrub encountered read error, probably corrupt
            dout(10) << __func__ << ": rejecting osd " << j->first
                     << " for obj " << obj
                     << ", read_error"
                     << dendl;
            continue;
        }
        map<string, bufferptr>::iterator k = i->second.attrs.find(OI_ATTR);
        if (k == i->second.attrs.end())
        {
            // no object info on object, probably corrupt
            dout(10) << __func__ << ": rejecting osd " << j->first
                     << " for obj " << obj
                     << ", no oi attr"
                     << dendl;
            continue;
        }

        bufferlist bl;
        bl.push_back(k->second);
        object_info_t oi;
        try
        {
            bufferlist::iterator bliter = bl.begin();
            ::decode(oi, bliter);
        }
        catch (...)
        {
            dout(10) << __func__ << ": rejecting osd " << j->first
                     << " for obj " << obj
                     << ", corrupt oi attr"
                     << dendl;
            // invalid object info, probably corrupt
            continue;
        }

        // note candidate in case we can't find anything better, because
        // something is better than nothing.  FIXME.
        auth = j;
        *auth_oi = oi;

        uint64_t correct_size = be_get_ondisk_size(oi.size);
        if (correct_size != i->second.size)
        {
            // invalid size, probably corrupt
            dout(10) << __func__ << ": rejecting osd " << j->first
                     << " for obj " << obj
                     << ", size mismatch"
                     << dendl;
            // invalid object info, probably corrupt
            continue;
        }
        if (parent->get_pool().is_replicated())
        {
            if (okseed && oi.is_data_digest() && i->second.digest_present &&
                    oi.data_digest != i->second.digest)
            {
                dout(10) << __func__ << ": rejecting osd " << j->first
                         << " for obj " << obj
                         << ", data digest mismatch 0x" << std::hex
                         << i->second.digest << " != 0x" << oi.data_digest
                         << std::dec << dendl;
                continue;
            }
            if (okseed && oi.is_omap_digest() && i->second.omap_digest_present &&
                    oi.omap_digest != i->second.omap_digest)
            {
                dout(10) << __func__ << ": rejecting osd " << j->first
                         << " for obj " << obj
                         << ", omap digest mismatch 0x" << std::hex
                         << i->second.omap_digest << " != 0x" << oi.omap_digest
                         << std::dec << dendl;
                continue;
            }
        }
        break;
    }
    dout(10) << __func__ << ": selecting osd " << auth->first
             << " for obj " << obj
             << " with oi " << *auth_oi
             << dendl;
    return auth;
}

void PGBackend::be_compare_scrubmaps(
    const map<pg_shard_t,ScrubMap*> &maps,
    bool okseed,
    bool repair,
    map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> &missing,
    map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> &inconsistent,
    map<hobject_t, list<pg_shard_t>, hobject_t::BitwiseComparator> &authoritative,
    map<hobject_t, pair<uint32_t,uint32_t>, hobject_t::BitwiseComparator> &missing_digest,
    int &shallow_errors, int &deep_errors,
    const spg_t& pgid,
    const vector<int> &acting,
    ostream &errorstream)
{
    map<hobject_t,ScrubMap::object, hobject_t::BitwiseComparator>::const_iterator i;
    map<pg_shard_t, ScrubMap *, hobject_t::BitwiseComparator>::const_iterator j;
    set<hobject_t, hobject_t::BitwiseComparator> master_set;
    utime_t now = ceph_clock_now(NULL);

    // Construct master set
    for (j = maps.begin(); j != maps.end(); ++j)
    {
        for (i = j->second->objects.begin(); i != j->second->objects.end(); ++i)
        {
            master_set.insert(i->first);
        }
    }

    // Check maps against master set and each other
    for (set<hobject_t, hobject_t::BitwiseComparator>::const_iterator k = master_set.begin();
            k != master_set.end();
            ++k)
    {
        object_info_t auth_oi;
		//先找到作为auth
        map<pg_shard_t, ScrubMap *>::const_iterator auth =
            be_select_auth_object(*k, maps, okseed, &auth_oi);
        list<pg_shard_t> auth_list;
        if (auth == maps.end())
        {
            dout(10) << __func__ << ": unable to find any auth object" << dendl;
            ++shallow_errors;
            errorstream << pgid << " shard " << j->first
                        << ": soid failed to pick suitable auth object\n";
            continue;
        }
        auth_list.push_back(auth->first);

        ScrubMap::object& auth_object = auth->second->objects[*k];
        set<pg_shard_t> cur_missing;
        set<pg_shard_t> cur_inconsistent;
        bool clean = true;
		//用其他的跟auth对比
        for (j = maps.begin(); j != maps.end(); ++j)
        {
            if (j == auth)
                continue;
            if (j->second->objects.count(*k))
            {
                // Compare
                stringstream ss;
                enum scrub_error_type error =
                    be_compare_scrub_objects(auth->first,
                                             auth_object,
                                             auth_oi,
                                             okseed,
                                             j->second->objects[*k],
                                             ss);
                if (error != CLEAN)
                {
                    clean = false;
                    cur_inconsistent.insert(j->first);
                    if (error == SHALLOW_ERROR)
                        ++shallow_errors;
                    else
                        ++deep_errors;
                    errorstream << pgid << " shard " << j->first << ": soid " << *k
                                << " " << ss.str() << "\n";
                }
                else
                {
                    auth_list.push_back(j->first);
                }
            }
            else
            {
                clean = false;
                cur_missing.insert(j->first);
                ++shallow_errors;
                errorstream << pgid << " shard " << j->first << " missing " << *k
                            << "\n";
            }
        }
        if (!cur_missing.empty())
        {
            missing[*k] = cur_missing;
        }
        if (!cur_inconsistent.empty())
        {
            inconsistent[*k] = cur_inconsistent;
        }
        if (!cur_inconsistent.empty() || !cur_missing.empty())
        {
            authoritative[*k] = auth_list;
        }

        if (okseed &&
                clean &&
                parent->get_pool().is_replicated())
        {
            enum
            {
                NO = 0,
                MAYBE = 1,
                FORCE = 2,
            } update = NO;

            // recorded digest != actual digest?
            if (auth_oi.is_data_digest() && auth_object.digest_present &&
                    auth_oi.data_digest != auth_object.digest)
            {
                ++deep_errors;
                errorstream << pgid << " recorded data digest 0x"
                            << std::hex << auth_oi.data_digest << " != on disk 0x"
                            << auth_object.digest << std::dec << " on " << auth_oi.soid
                            << "\n";
                if (repair)
                    update = FORCE;
            }
            if (auth_oi.is_omap_digest() && auth_object.omap_digest_present &&
                    auth_oi.omap_digest != auth_object.omap_digest)
            {
                ++deep_errors;
                errorstream << pgid << " recorded omap digest 0x"
                            << std::hex << auth_oi.data_digest << " != on disk 0x"
                            << auth_object.digest << std::dec << " on " << auth_oi.soid
                            << "\n";
                if (repair)
                    update = FORCE;
            }

            if (auth_object.digest_present && auth_object.omap_digest_present &&
                    (!auth_oi.is_data_digest() || !auth_oi.is_omap_digest()))
            {
                dout(20) << __func__ << " missing digest on " << *k << dendl;
                update = MAYBE;
            }
            if (g_conf->osd_debug_scrub_chance_rewrite_digest &&
                    (((unsigned)rand() % 100) >
                     g_conf->osd_debug_scrub_chance_rewrite_digest))
            {
                dout(20) << __func__ << " randomly updating digest on " << *k << dendl;
                update = MAYBE;
            }
            if (update != NO)
            {
                utime_t age = now - auth_oi.local_mtime;
				//deep scrub的时候，如果本地最近更新时间超过2小时，并且object info中记录了
				//的digest与计算出来的不符合，那么就更新digest
                if (update == FORCE ||
                        age > g_conf->osd_deep_scrub_update_digest_min_age)
                {
                    dout(20) << __func__ << " will update digest on " << *k << dendl;
                    missing_digest[*k] = make_pair(auth_object.digest,
                                                   auth_object.omap_digest);
                }
                else
                {
                    dout(20) << __func__ << " missing digest but age " << age
                             << " < " << g_conf->osd_deep_scrub_update_digest_min_age
                             << " on " << *k << dendl;
                }
            }
        }
    }
}
