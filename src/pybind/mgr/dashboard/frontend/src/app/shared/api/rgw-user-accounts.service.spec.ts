import { TestBed } from '@angular/core/testing';

import { RgwUserAccountsService } from './rgw-user-accounts.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { Account } from '~/app/ceph/rgw/models/rgw-user-accounts';
import { RgwHelper } from '~/testing/unit-test-helper';

const mockAccountData: Account[] = [
  {
    id: 'RGW80617806988089685',
    tenant: '',
    name: '',
    email: '',
    quota: {
      enabled: false,
      check_on_raw: false,
      max_size: -1,
      max_size_kb: 0,
      max_objects: -1
    },
    bucket_quota: {
      enabled: false,
      check_on_raw: false,
      max_size: -1,
      max_size_kb: 0,
      max_objects: -1
    },
    max_users: 1000,
    max_roles: 1000,
    max_groups: 1000,
    max_buckets: 1000,
    max_access_keys: 4
  },
  {
    id: 'RGW12444466134482748',
    tenant: '',
    name: '',
    email: '',
    quota: {
      enabled: false,
      check_on_raw: false,
      max_size: -1,
      max_size_kb: 0,
      max_objects: -1
    },
    bucket_quota: {
      enabled: false,
      check_on_raw: false,
      max_size: -1,
      max_size_kb: 0,
      max_objects: -1
    },
    max_users: 1000,
    max_roles: 1000,
    max_groups: 1000,
    max_buckets: 1000,
    max_access_keys: 4
  }
];

describe('RgwUserAccountsService', () => {
  let service: RgwUserAccountsService;
  let httpTesting: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [RgwUserAccountsService],
      imports: [HttpClientTestingModule]
    });
    service = TestBed.inject(RgwUserAccountsService);
    httpTesting = TestBed.inject(HttpTestingController);
    RgwHelper.selectDaemon();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should fetch detailed list of accounts', () => {
    service.list(true).subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/accounts?${RgwHelper.DAEMON_QUERY_PARAM}&detailed=true`
    );
    expect(req.request.method).toBe('GET');
    req.flush(mockAccountData);
  });

  it('should fetch single account detail', () => {
    service.get('RGW80617806988089685').subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/accounts/get?${RgwHelper.DAEMON_QUERY_PARAM}&account_id=RGW80617806988089685`
    );
    expect(req.request.method).toBe('GET');
    req.flush(mockAccountData[0]);
  });

  it('should call create method', () => {
    service.create(mockAccountData[0]).subscribe();
    const req = httpTesting.expectOne(`api/rgw/accounts?${RgwHelper.DAEMON_QUERY_PARAM}`);
    expect(req.request.method).toBe('POST');
    req.flush(mockAccountData[0]);
  });

  it('should call modify method', () => {
    service.modify(mockAccountData[0]).subscribe();
    const req = httpTesting.expectOne(`api/rgw/accounts/set?${RgwHelper.DAEMON_QUERY_PARAM}`);
    expect(req.request.method).toBe('PUT');
    req.flush(mockAccountData[0]);
  });

  it('should call remove method', () => {
    service.remove('RGW12444466134482748').subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/accounts/RGW12444466134482748?${RgwHelper.DAEMON_QUERY_PARAM}`
    );
    expect(req.request.method).toBe('DELETE');
    req.flush(null);
  });

  it('should call setQuota method', () => {
    service
      .setQuota('RGW12444466134482748', {
        quota_type: 'bucket',
        max_size: '10MB',
        max_objects: '1000',
        enabled: true
      })
      .subscribe();
    const req = httpTesting.expectOne(`api/rgw/accounts/RGW12444466134482748/quota`);
    expect(req.request.method).toBe('PUT');
    req.flush(null);
  });
});
