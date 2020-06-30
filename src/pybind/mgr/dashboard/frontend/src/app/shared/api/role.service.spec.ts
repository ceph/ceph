import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { RoleService } from './role.service';

describe('RoleService', () => {
  let service: RoleService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [RoleService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(RoleService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/role');
    expect(req.request.method).toBe('GET');
  });

  it('should call delete', () => {
    service.delete('role1').subscribe();
    const req = httpTesting.expectOne('api/role/role1');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call get', () => {
    service.get('role1').subscribe();
    const req = httpTesting.expectOne('api/role/role1');
    expect(req.request.method).toBe('GET');
  });

  it('should call clone', () => {
    service.clone('foo', 'bar').subscribe();
    const req = httpTesting.expectOne('api/role/foo/clone?new_name=bar');
    expect(req.request.method).toBe('POST');
  });

  it('should check if role name exists', () => {
    let exists: boolean;
    service.exists('role1').subscribe((res: boolean) => {
      exists = res;
    });
    const req = httpTesting.expectOne('api/role');
    expect(req.request.method).toBe('GET');
    req.flush([{ name: 'role0' }, { name: 'role1' }]);
    expect(exists).toBeTruthy();
  });

  it('should check if role name does not exist', () => {
    let exists: boolean;
    service.exists('role2').subscribe((res: boolean) => {
      exists = res;
    });
    const req = httpTesting.expectOne('api/role');
    expect(req.request.method).toBe('GET');
    req.flush([{ name: 'role0' }, { name: 'role1' }]);
    expect(exists).toBeFalsy();
  });
});
