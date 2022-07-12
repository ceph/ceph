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
    expect(pipe.transform(value)).toEqual([
      {
        content: '3 total',
        class: ''
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '3 up, 3 in',
        class: ''
      }
    ]);
  });

  it('transforms having 3 osd with 2 up, 1 in, 1 down, 2 out', () => {
    const value = {
      osds: [
        { up: 1, in: 1, state: ['up', 'exists'] },
        { up: 1, in: 0, state: ['up', 'exists'] },
        { up: 0, in: 0, state: ['exists'] }
      ]
    };
    expect(pipe.transform(value)).toEqual([
      {
        content: '3 total',
        class: ''
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '2 up, 1 in',
        class: ''
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '1 down, 2 out',
        class: 'card-text-error'
      }
    ]);
  });

  it('transforms having 3 osd with 2 up, 3 in, 1 full, 1 nearfull, 1 down, 0 out', () => {
    const value = {
      osds: [
        { up: 1, in: 1, state: ['up', 'nearfull'] },
        { up: 1, in: 1, state: ['up', 'exists'] },
        { up: 0, in: 1, state: ['full'] }
      ]
    };
    expect(pipe.transform(value)).toEqual([
      {
        content: '3 total',
        class: ''
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '2 up, 3 in',
        class: ''
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '1 down',
        class: 'card-text-error'
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '1 near full',
        class: 'card-text-error'
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '1 full',
        class: 'card-text-error'
      }
    ]);
  });

  it('transforms having 3 osd with 3 up, 2 in, 0 down, 1 out', () => {
    const value = {
      osds: [
        { up: 1, in: 1, state: ['up', 'exists'] },
        { up: 1, in: 1, state: ['up', 'exists'] },
        { up: 1, in: 0, state: ['up', 'exists'] }
      ]
    };
    expect(pipe.transform(value)).toEqual([
      {
        content: '3 total',
        class: ''
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '3 up, 2 in',
        class: ''
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '1 out',
        class: 'card-text-error'
      }
    ]);
  });

  it('transforms having 4 osd with 3 up, 2 in, 1 down, another 2 out', () => {
    const value = {
      osds: [
        { up: 1, in: 1, state: ['up', 'exists'] },
        { up: 1, in: 0, state: ['up', 'exists'] },
        { up: 1, in: 0, state: ['up', 'exists'] },
        { up: 0, in: 1, state: ['exists'] }
      ]
    };
    expect(pipe.transform(value)).toEqual([
      {
        content: '4 total',
        class: ''
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '3 up, 2 in',
        class: ''
      },
      {
        content: '',
        class: 'card-text-line-break'
      },
      {
        content: '1 down, 2 out',
        class: 'card-text-error'
      }
    ]);
  });
});
