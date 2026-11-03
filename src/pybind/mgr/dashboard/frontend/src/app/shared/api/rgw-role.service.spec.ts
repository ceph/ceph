import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { RgwRoleService } from './rgw-role.service';

describe('RgwRoleService', () => {
  let service: RgwRoleService;
  let httpTesting: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule]
    });
    service = TestBed.inject(RgwRoleService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list with account_id', () => {
    service.list('test-account').subscribe();
    const req = httpTesting.expectOne('api/rgw/accounts/test-account/roles');
    expect(req.request.method).toBe('GET');
  });

  it('should call get with account_id', () => {
    service.get('test-role', 'test-account').subscribe();
    const req = httpTesting.expectOne('api/rgw/accounts/test-account/roles/test-role');
    expect(req.request.method).toBe('GET');
  });

  it('should call create', () => {
    const payload = { role_name: 'test', account_id: 'test-account' };
    service.create(payload as any).subscribe();
    const req = httpTesting.expectOne('api/rgw/accounts/test-account/roles');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(payload);
  });

  it('should call update', () => {
    const payload = { max_session_duration: 2, account_id: 'test-account' };
    service.update('test-role', payload as any).subscribe();
    const req = httpTesting.expectOne('api/rgw/accounts/test-account/roles');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(payload);
  });

  it('should call delete with account_id', () => {
    service.delete('test-role', 'test-account').subscribe();
    const req = httpTesting.expectOne('api/rgw/accounts/test-account/roles/test-role');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call exists and return true when role is found', () => {
    service.exists('test-role', 'test-account').subscribe((exists) => {
      expect(exists).toBe(true);
    });
    const req = httpTesting.expectOne('api/rgw/accounts/test-account/roles/test-role');
    expect(req.request.method).toBe('GET');
    req.flush({ RoleName: 'test-role' });
  });

  it('should call exists and return false when role is missing', () => {
    service.exists('missing-role', 'test-account').subscribe((exists) => {
      expect(exists).toBe(false);
    });
    const req = httpTesting.expectOne('api/rgw/accounts/test-account/roles/missing-role');
    expect(req.request.method).toBe('GET');
    req.flush({ status: 404, statusText: 'Not Found' }, { status: 404, statusText: 'Not Found' });
  });

  it('should call attachPolicy', () => {
    service
      .attachPolicy('test-role', 'test-policy', '{"Statement":[]}', 'test-account')
      .subscribe();
    const req = httpTesting.expectOne('api/rgw/accounts/test-account/roles/test-role/policy');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({
      role_name: 'test-role',
      policy_name: 'test-policy',
      policy_doc: '{"Statement":[]}'
    });
  });

  it('should call deletePolicy', () => {
    service.deletePolicy('test-role', 'test-policy', 'test-account').subscribe();
    const req = httpTesting.expectOne(
      'api/rgw/accounts/test-account/roles/test-role/policy/test-policy'
    );
    expect(req.request.method).toBe('DELETE');
  });
});
