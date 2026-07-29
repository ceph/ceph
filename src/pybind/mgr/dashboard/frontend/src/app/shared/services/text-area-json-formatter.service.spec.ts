import { configureTestBed } from '~/testing/unit-test-helper';
import { TestBed } from '@angular/core/testing';

import { TextAreaJsonFormatterService } from './text-area-json-formatter.service';

describe('TextAreaJsonFormatterService', () => {
  let service: TextAreaJsonFormatterService;

  configureTestBed({});

  beforeEach(() => {
    service = TestBed.inject(TextAreaJsonFormatterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
