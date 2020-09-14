import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
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
    const expected = [
      { class: 'popover-info', content: '0 active', titleText: '1 standbyReplay' },
      { class: 'card-text-line-break', content: '', titleText: '' },
      { class: 'popover-info', content: '2 standby', titleText: 'standby daemons: a' }
    ];

    expect(pipe.transform(payload)).toEqual(expected);
  });

  it('transforms with 1 active and 1 standy', () => {
    const payload = {
      standbys: [{ name: 'b' }],
      filesystems: [{ mdsmap: { info: [{ state: 'up:active', name: 'a' }] } }]
    };
    const expected = [
      { class: 'popover-info', content: '1 active', titleText: 'active daemon: a' },
      { class: 'card-text-line-break', content: '', titleText: '' },
      { class: 'popover-info', content: '1 standby', titleText: 'standby daemons: b' }
    ];
    expect(pipe.transform(payload)).toEqual(expected);
  });

  it('transforms with 0 filesystems', () => {
    const payload: Record<string, any> = {
      standbys: [0],
      filesystems: []
    };
    const expected = [{ class: 'popover-info', content: 'no filesystems', titleText: '' }];

    expect(pipe.transform(payload)).toEqual(expected);
  });

  it('transforms without filesystem', () => {
    const payload = { standbys: [{ name: 'a' }] };
    const expected = [
      { class: 'popover-info', content: '1 up', titleText: '' },
      { class: 'card-text-line-break', content: '', titleText: '' },
      { class: 'popover-info', content: 'no filesystems', titleText: 'standby daemons: a' }
    ];

    expect(pipe.transform(payload)).toEqual(expected);
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });
});
