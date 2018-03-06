import { inject, TestBed } from '@angular/core/testing';

import { FormatterService } from './formatter.service';

describe('FormatterService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [FormatterService]
    });
  });

  it('should be created', inject([FormatterService], (service: FormatterService) => {
    expect(service).toBeTruthy();
  }));
});
