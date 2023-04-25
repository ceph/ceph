import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { OsdSummaryPipe } from './osd-summary.pipe';

describe('OsdSummaryPipe', () => {
  let pipe: OsdSummaryPipe;

  configureTestBed({
    providers: [OsdSummaryPipe]
  });

  beforeEach(() => {
    pipe = TestBed.inject(OsdSummaryPipe);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });

  it('transforms having 3 osd with 3 up, 3 in, 0 down, 0 out', () => {
    const value = {
      osds: [
        { up: 1, in: 1, state: ['up', 'exists'] },
        { up: 1, in: 1, state: ['up', 'exists'] },
        { up: 1, in: 1, state: ['up', 'exists'] }
      ]
    };
    expect(pipe.transform(value)).toEqual({
      total: 3,
      down: 0,
      out: 0,
      up: 3,
      in: 3,
      nearfull: 0,
      full: 0
    });
  });
});
