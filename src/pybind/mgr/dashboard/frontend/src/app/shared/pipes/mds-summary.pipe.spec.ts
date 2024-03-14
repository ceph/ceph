import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { MdsSummaryPipe } from './mds-summary.pipe';

describe('MdsSummaryPipe', () => {
  let pipe: MdsSummaryPipe;

  configureTestBed({
    providers: [MdsSummaryPipe]
  });

  beforeEach(() => {
    pipe = TestBed.inject(MdsSummaryPipe);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms with 0 active and 2 standy', () => {
    const payload = {
      standbys: [{ name: 'a' }],
      filesystems: [{ mdsmap: { info: [{ state: 'up:standby-replay' }] } }]
    };

    expect(pipe.transform(payload)).toEqual({
      success: 0,
      info: 2,
      total: 2
    });
  });

  it('transforms with 1 active and 1 standy', () => {
    const payload = {
      standbys: [{ name: 'b' }],
      filesystems: [{ mdsmap: { info: [{ state: 'up:active', name: 'a' }] } }]
    };
    expect(pipe.transform(payload)).toEqual({
      success: 1,
      info: 1,
      total: 2
    });
  });

  it('transforms with 0 filesystems', () => {
    const payload: Record<string, any> = {
      standbys: [0],
      filesystems: []
    };

    expect(pipe.transform(payload)).toEqual({
      success: 0,
      info: 0,
      total: 0
    });
  });

  it('transforms without filesystem', () => {
    const payload = { standbys: [{ name: 'a' }] };

    expect(pipe.transform(payload)).toEqual({
      success: 0,
      info: 1,
      total: 1
    });
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toEqual({
      success: 0,
      info: 0,
      total: 0
    });
  });
});
