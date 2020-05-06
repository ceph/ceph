import { TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';

import { MgrSummaryPipe } from './mgr-summary.pipe';

describe('MgrSummaryPipe', () => {
  let pipe: MgrSummaryPipe;

  configureTestBed({
    providers: [MgrSummaryPipe, i18nProviders]
  });

  beforeEach(() => {
    pipe = TestBed.inject(MgrSummaryPipe);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });

  it('transforms with active_name undefined', () => {
    const payload: Record<string, any> = {
      active_name: undefined,
      standbys: []
    };
    const expected = [
      { class: 'popover-info', content: 'n/a active', titleText: '' },
      { class: 'card-text-line-break', content: '', titleText: '' },
      { class: 'popover-info', content: '0 standby', titleText: '' }
    ];

    expect(pipe.transform(payload)).toEqual(expected);
  });

  it('transforms with 1 active and 2 standbys', () => {
    const payload = {
      active_name: 'x',
      standbys: [{ name: 'y' }, { name: 'z' }]
    };
    const expected = [
      { class: 'popover-info', content: '1 active', titleText: 'active daemon: x' },
      { class: 'card-text-line-break', content: '', titleText: '' },
      { class: 'popover-info', content: '2 standby', titleText: 'standby daemons: y, z' }
    ];

    expect(pipe.transform(payload)).toEqual(expected);
  });
});
