import { UpgradeService } from './upgrade.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { SummaryService } from '../services/summary.service';
import { BehaviorSubject } from 'rxjs';

export class SummaryServiceMock {
  summaryDataSource = new BehaviorSubject({
    version:
      'ceph version 18.1.3-12222-gcd0cd7cb ' +
      '(b8193bb4cda16ccc5b028c3e1df62bc72350a15d) reef (dev)'
  });
  summaryData$ = this.summaryDataSource.asObservable();

  subscribe(call: any) {
    return this.summaryData$.subscribe(call);
  }
}

describe('UpgradeService', () => {
  let service: UpgradeService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [UpgradeService, { provide: SummaryService, useClass: SummaryServiceMock }]
  });

  beforeEach(() => {
    service = TestBed.inject(UpgradeService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call upgrade list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/cluster/upgrade');
    expect(req.request.method).toBe('GET');
  });

  it('should not show any version if the registry versions are older than the cluster version', () => {
    const upgradeInfoPayload = {
      image: 'quay.io/ceph-test/ceph',
      registry: 'quay.io',
      versions: ['18.1.0', '18.1.1', '18.1.2']
    };
    const expectedVersions: string[] = [];
    expect(service.versionAvailableForUpgrades(upgradeInfoPayload).versions).toEqual(
      expectedVersions
    );
  });

  it('should start the upgrade', () => {
    service.start('18.1.0').subscribe();
    const req = httpTesting.expectOne('api/cluster/upgrade/start');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ version: '18.1.0' });
  });
});
