import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { DaemonService } from './daemon.service';

describe('DaemonService', () => {
  let service: DaemonService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [DaemonService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(DaemonService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call action', () => {
    const put_data: any = {
      action: 'start',
      container_image: null
    };
    service.action('osd.1', 'start').subscribe();
    const req = httpTesting.expectOne('api/daemon/osd.1');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(put_data);
  });
});
