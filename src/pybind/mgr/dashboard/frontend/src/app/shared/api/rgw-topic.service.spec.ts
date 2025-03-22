import { TestBed } from '@angular/core/testing';

import { RgwTopicService } from './rgw-topic.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('RgwTopicService', () => {
  let service: RgwTopicService;

  configureTestBed({
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(RgwTopicService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
