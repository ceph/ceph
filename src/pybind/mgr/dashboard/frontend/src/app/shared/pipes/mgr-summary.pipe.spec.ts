import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { MgrSummaryPipe } from './mgr-summary.pipe';

describe('MgrSummaryPipe', () => {
  let pipe: MgrSummaryPipe;

  configureTestBed({
    providers: [MgrSummaryPipe]
  });

  beforeEach(() => {
    pipe = TestBed.inject(MgrSummaryPipe);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toEqual({
      success: 0,
      info: 0,
      total: 0
    });
  });

  it('transforms with 1 active and 2 standbys', () => {
    const payload = {
      active_name: 'x',
      standbys: [{ name: 'y' }, { name: 'z' }]
    };
    const expected = { success: 1, info: 2, total: 3 };

    expect(pipe.transform(payload)).toEqual(expected);
  });
});
