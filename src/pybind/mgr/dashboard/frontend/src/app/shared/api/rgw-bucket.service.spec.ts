import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { RgwBucketService } from './rgw-bucket.service';

describe('RgwBucketService', () => {
  configureTestBed({
    providers: [RgwBucketService],
    imports: [HttpClientTestingModule, HttpClientModule]
  });

  it(
    'should be created',
    inject([RgwBucketService], (service: RgwBucketService) => {
      expect(service).toBeTruthy();
    })
  );
});
