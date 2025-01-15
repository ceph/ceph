import { TestBed } from '@angular/core/testing';

import { RgwTopicService } from './rgw-topic.service';

describe('RgwTopicService', () => {
  let service: RgwTopicService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(RgwTopicService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
