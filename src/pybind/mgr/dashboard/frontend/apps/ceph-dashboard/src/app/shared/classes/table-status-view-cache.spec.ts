import { ViewCacheStatus } from '../enum/view-cache-status.enum';
import { TableStatusViewCache } from './table-status-view-cache';

describe('TableStatusViewCache', () => {
  it('should create an instance', () => {
    const ts = new TableStatusViewCache();
    expect(ts).toBeTruthy();
    expect(ts).toEqual({ msg: '', type: 'ghost' });
  });

  it('should create a ValueStale instance', () => {
    let ts = new TableStatusViewCache(ViewCacheStatus.ValueStale);
    expect(ts).toEqual({ type: 'secondary', msg: 'Displaying previously cached data.' });

    ts = new TableStatusViewCache(ViewCacheStatus.ValueStale, 'foo bar');
    expect(ts).toEqual({
      type: 'secondary',
      msg: 'Displaying previously cached data for foo bar.'
    });
  });

  it('should create a ValueNone instance', () => {
    let ts = new TableStatusViewCache(ViewCacheStatus.ValueNone);
    expect(ts).toEqual({ type: 'primary', msg: 'Retrieving data. Please wait...' });

    ts = new TableStatusViewCache(ViewCacheStatus.ValueNone, 'foo bar');
    expect(ts).toEqual({ type: 'primary', msg: 'Retrieving data for foo bar. Please wait...' });
  });

  it('should create a ValueException instance', () => {
    let ts = new TableStatusViewCache(ViewCacheStatus.ValueException);
    expect(ts).toEqual({
      type: 'danger',
      msg: 'Could not load data. Please check the cluster health.'
    });

    ts = new TableStatusViewCache(ViewCacheStatus.ValueException, 'foo bar');
    expect(ts).toEqual({
      type: 'danger',
      msg: 'Could not load data for foo bar. Please check the cluster health.'
    });
  });
});
