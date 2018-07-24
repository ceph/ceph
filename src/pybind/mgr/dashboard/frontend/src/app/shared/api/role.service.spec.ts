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
    service = TestBed.get(RoleService);
    httpTesting = TestBed.get(HttpTestingController);
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
});
