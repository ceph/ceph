import { TestBed } from '@angular/core/testing';

import { TextAreaJsonFormatterService } from './text-area-json-formatter.service';

describe('TextAreaJsonFormatterService', () => {
  let service: TextAreaJsonFormatterService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(TextAreaJsonFormatterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
