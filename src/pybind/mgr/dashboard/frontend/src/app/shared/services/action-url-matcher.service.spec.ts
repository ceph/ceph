import { TestBed } from '@angular/core/testing';

import { ActionUrlMatcherService } from './action-url-matcher.service';

describe('ActionUrlMatcherService', () => {
  let service: ActionUrlMatcherService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ActionUrlMatcherService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
