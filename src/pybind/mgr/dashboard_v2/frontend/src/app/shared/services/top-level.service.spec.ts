import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { SharedModule } from '../shared.module';
import { TopLevelService } from './top-level.service';

describe('TopLevelService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [TopLevelService],
      imports: [HttpClientTestingModule, SharedModule]
    });
  });

  it(
    'should be created',
    inject([TopLevelService], (service: TopLevelService) => {
      expect(service).toBeTruthy();
    })
  );
});
