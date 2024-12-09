import { TestBed } from '@angular/core/testing';

import { RgwUserAccountsService } from './rgw-user-accounts.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { Accounts } from '~/app/ceph/rgw/models/rgw-user-accounts';

const mockAccountData: Accounts[] = [
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
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should fetch detailed list of accounts', () => {
    service.list(true).subscribe();
    const req = httpTesting.expectOne('api/rgw/accounts?detailed=true');
    expect(req.request.method).toBe('GET');
    req.flush(mockAccountData);
  });
});
