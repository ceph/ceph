import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { RgwBucketService } from './rgw-bucket.service';

describe('RgwBucketService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [RgwBucketService],
      imports: [HttpClientTestingModule, HttpClientModule]
    });
  });

  it(
    'should be created',
    inject([RgwBucketService], (service: RgwBucketService) => {
      expect(service).toBeTruthy();
    })
  );
});
