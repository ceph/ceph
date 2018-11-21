import { TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { MdsSummaryPipe } from './mds-summary.pipe';

describe('MdsSummaryPipe', () => {
  let pipe: MdsSummaryPipe;

  configureTestBed({
    providers: [MdsSummaryPipe, i18nProviders]
  });

  beforeEach(() => {
    pipe = TestBed.get(MdsSummaryPipe);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms with 0 active and 2 standy', () => {
    const payload = {
      standbys: [0],
      filesystems: [{ mdsmap: { info: [{ state: 'up:standby-replay' }] } }]
    };
    const expected = [
      { class: '', content: '0 active' },
      { class: 'card-text-line-break', content: '' },
      { class: '', content: '2 standby' }
    ];

    expect(pipe.transform(payload)).toEqual(expected);
  });

  it('transforms with 1 active and 1 standy', () => {
    const payload = {
      standbys: [0],
      filesystems: [{ mdsmap: { info: [{ state: 'up:active' }] } }]
    };
    const expected = [
      { class: '', content: '1 active' },
      { class: 'card-text-line-break', content: '' },
      { class: '', content: '1 standby' }
    ];
    expect(pipe.transform(payload)).toEqual(expected);
  });

  it('transforms with 0 filesystems', () => {
    const payload = {
      standbys: [0],
      filesystems: []
    };
    const expected = [{ class: '', content: 'no filesystems' }];

    expect(pipe.transform(payload)).toEqual(expected);
  });

  it('transforms without filesystem', () => {
    const payload = { standbys: [0] };
    const expected = [
      { class: '', content: '1 up' },
      { class: 'card-text-line-break', content: '' },
      { class: '', content: 'no filesystems' }
    ];

    expect(pipe.transform(payload)).toEqual(expected);
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });
});
