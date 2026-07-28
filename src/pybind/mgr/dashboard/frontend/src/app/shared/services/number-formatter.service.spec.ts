import { configureTestBed } from '~/testing/unit-test-helper';
import { TestBed } from '@angular/core/testing';

import { NumberFormatterService } from './number-formatter.service';

describe('FormatToService', () => {
  let service: NumberFormatterService;

  configureTestBed({});

  beforeEach(() => {
    service = TestBed.inject(NumberFormatterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
