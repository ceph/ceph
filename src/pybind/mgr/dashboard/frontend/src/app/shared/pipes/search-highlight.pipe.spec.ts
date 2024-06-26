import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { SearchHighlightPipe } from './search-highlight.pipe';

describe('SearchHighlightPipe', () => {
  let pipe: SearchHighlightPipe;

  configureTestBed({
    providers: [SearchHighlightPipe]
  });

  beforeEach(() => {
    pipe = TestBed.inject(SearchHighlightPipe);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms with a matching keyword ', () => {
    const value = 'overall HEALTH_WARN Dashboard debug mode is enabled';
    const args = 'Dashboard';
    const expected = 'overall HEALTH_WARN <mark>Dashboard</mark> debug mode is enabled';

    expect(pipe.transform(value, args)).toEqual(expected);
  });

  it('transforms with a matching keyword having regex character', () => {
    const value = 'loreum ipsum .? dolor sit amet';
    const args = '.?';
    const expected = 'loreum ipsum <mark>.?</mark> dolor sit amet';

    expect(pipe.transform(value, args)).toEqual(expected);
  });

  it('transforms with empty search keyword', () => {
    const value = 'overall HEALTH_WARN Dashboard debug mode is enabled';
    expect(pipe.transform(value, '')).toBe(value);
  });
});
