import { TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';

import { MgrSummaryPipe } from './mgr-summary.pipe';

describe('MgrSummaryPipe', () => {
  let pipe: MgrSummaryPipe;

  configureTestBed({
    providers: [MgrSummaryPipe, i18nProviders]
  });

  beforeEach(() => {
    pipe = TestBed.get(MgrSummaryPipe);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });

  it('transforms with active_name undefined', () => {
    const value = {
      active_name: undefined,
      standbys: []
    };
    expect(pipe.transform(value)).toBe('active: n/a');
  });

  it('transforms with 1 active and 2 standbys', () => {
    const value = {
      active_name: 'a',
      standbys: ['b', 'c']
    };
    expect(pipe.transform(value)).toBe('active: a, 2 standbys');
  });
});
