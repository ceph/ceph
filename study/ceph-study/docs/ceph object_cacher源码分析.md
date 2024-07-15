1. map_write函数分析
```
map_write oex rbd_data.5e326b8b4567.0000000000000786 238481~511
238481~1022:bh1:  238481~1022 

场景1：
write：238481~1022    =>  bh1[238481~1022]   
write: 238481~511        => bh1[238481~511]   ,  bh2[238992~511]
				     => bh1[238481~511]
场景2：
write:  2636873~1533  =>  bh1[2636873~1533]
write:  2637895~1022  =>  split: bh1[2636873~1533]  =>   bh[2636873~1022], bh[2637895~511]
                               => bh[2637895~1022]
场景3：
write: 2029440~1533   => bh1[2029440~1533]
write:  2028929~1022  => new: bh[2028929~511],  
				      => split: bh1[2029440~1533]  =>  bh[2029440~511],   bh[2029951~1022]
                                       => merge:  bh[2028929~511],  bh[2029440~511]  => bh[2028929~1022]

场景4：
write: 175503~1022 => bh1[175503~1022]
write: 174481~1533 => new:   bh[174481~1022]
				=> split:   bh1[175503~1022]  =>  bh[175503~511], bh[176014~511]
				=> merge:  bh[174481~1022], bh[175503~511]  => bh[174481~1533]
```




```
ObjectCacher::BufferHead *ObjectCacher::Object::map_write(ObjectExtent &ex,
							  ceph_tid_t tid)
{
  if (oc->m_object_lock) {
    assert(this->lock.is_wlocked());
  }

  BufferHead *final = 0;

  ldout(oc->cct, 10) << "map_write oex " << ex.oid
      	       << " " << ex.offset << "~" << ex.length << dendl;

  loff_t cur = ex.offset;
  loff_t left = ex.length;

  map<loff_t, BufferHead*>::const_iterator p = data_lower_bound(ex.offset);
  while (left > 0) {
    loff_t max = left;

    // at end ?
    if (p == data.end()) {
      if (final == NULL) {
        final = new BufferHead(this);
        replace_journal_tid(final, tid);
        final->set_start( cur );
        final->set_length( max );
        oc->bh_add(this, final);
        ldout(oc->cct, 10) << "map_write adding trailing bh " << *final << dendl;
      } else {
        oc->bh_stat_sub(final);
        final->set_length(final->length() + max);
        oc->bh_stat_add(final);
      }
      left -= max;
      cur += max;
      continue;
    }

    ldout(oc->cct, 10) << "cur is " << cur << ", p is " << *p->second << dendl;
    //oc->verify_stats();

    if (p->first <= cur) {
      BufferHead *bh = p->second;
      ldout(oc->cct, 10) << "map_write bh " << *bh << " intersected" << dendl;

      if (p->first < cur) {
        assert(final == 0);
        if (cur + max >= bh->end()) {
          // we want right bit (one splice)
          final = split(bh, cur);   // just split it, take right half.
          maybe_rebuild_buffer(bh);
          replace_journal_tid(final, tid);
          ++p;
          assert(p->second == final);
        } else {
          // we want middle bit (two splices)
          final = split(bh, cur);
          maybe_rebuild_buffer(bh);
          ++p;
          assert(p->second == final);
          auto right = split(final, cur+max);
          maybe_rebuild_buffer(right);
          replace_journal_tid(final, tid);
        }
      } else {
        assert(p->first == cur);
        if (bh->length() <= max) {
          // whole bufferhead, piece of cake.
        } else {
          // we want left bit (one splice)
          auto right = split(bh, cur + max);        // just split
          maybe_rebuild_buffer(right);
        }
        if (final) {
          oc->mark_dirty(bh);
          oc->mark_dirty(final);
          --p;  // move iterator back to final
          assert(p->second == final);
          replace_journal_tid(bh, tid);
          merge_left(final, bh);
        } else {
          final = bh;
          replace_journal_tid(final, tid);
        }
      }

      // keep going.
      loff_t lenfromcur = final->end() - cur;
      cur += lenfromcur;
      left -= lenfromcur;
      ++p;
      continue;
    } else {
      // gap!
      loff_t next = p->first;
      loff_t glen = MIN(next - cur, max);
      ldout(oc->cct, 10) << "map_write gap " << cur << "~" << glen << dendl;
      if (final) {
        oc->bh_stat_sub(final);
        final->set_length(final->length() + glen);
        oc->bh_stat_add(final);
      } else {
        final = new BufferHead(this);
	replace_journal_tid(final, tid);
        final->set_start( cur );
        final->set_length( glen );
        oc->bh_add(this, final);
      }

      cur += glen;
      left -= glen;
      continue;    // more?
    }
  }

  // set version
  assert(final);
  assert(final->get_journal_tid() == tid);
  ldout(oc->cct, 10) << "map_write final is " << *final << dendl;

  return final;
}
```
