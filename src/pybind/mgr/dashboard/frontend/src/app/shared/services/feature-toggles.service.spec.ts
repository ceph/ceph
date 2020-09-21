import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { discardPeriodicTasks, fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { FeatureTogglesService } from './feature-toggles.service';

describe('FeatureTogglesService', () => {
  let httpTesting: HttpTestingController;
  let service: FeatureTogglesService;

  configureTestBed({
    providers: [FeatureTogglesService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(FeatureTogglesService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should fetch HTTP endpoint once and only once', fakeAsync(() => {
    const mockFeatureTogglesMap = [
      {
        rbd: true,
        mirroring: true,
        iscsi: true,
        cephfs: true,
        rgw: true
      }
    ];

    service
      .get()
      .subscribe((featureTogglesMap) => expect(featureTogglesMap).toEqual(mockFeatureTogglesMap));
    tick();

    // Second subscription shouldn't trigger a new HTTP request
    service
      .get()
      .subscribe((featureTogglesMap) => expect(featureTogglesMap).toEqual(mockFeatureTogglesMap));

    const req = httpTesting.expectOne(service.API_URL);
    req.flush(mockFeatureTogglesMap);
    discardPeriodicTasks();
  }));
});
