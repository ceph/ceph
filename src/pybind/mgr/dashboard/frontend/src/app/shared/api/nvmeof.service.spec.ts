import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NvmeofService } from '../../shared/api/nvmeof.service';

describe('NvmeofService', () => {
  let service: NvmeofService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [NvmeofService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(NvmeofService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call listGateways', () => {
    service.listGateways().subscribe();
    const req = httpTesting.expectOne('api/nvmeof/gateway');
    expect(req.request.method).toBe('GET');
  });
});
