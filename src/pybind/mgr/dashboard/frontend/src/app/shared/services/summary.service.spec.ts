import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { AuthStorageService } from './auth-storage.service';
import { SummaryService } from './summary.service';

describe('SummaryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [SummaryService, AuthStorageService],
      imports: [HttpClientTestingModule]
    });
  });

  it(
    'should be created',
    inject([SummaryService], (service: SummaryService) => {
      expect(service).toBeTruthy();
    })
  );
});
