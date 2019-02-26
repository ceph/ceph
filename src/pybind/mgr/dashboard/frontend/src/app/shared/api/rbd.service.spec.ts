import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { RbdConfigurationService } from '../services/rbd-configuration.service';
import { RbdService } from './rbd.service';

describe('RbdService', () => {
  let service: RbdService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [RbdService, RbdConfigurationService, i18nProviders],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(RbdService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call create', () => {
    service.create('foo').subscribe();
    const req = httpTesting.expectOne('api/block/image');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual('foo');
  });

  it('should call delete', () => {
    service.delete('poolName', 'rbdName').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call update', () => {
    service.update('poolName', 'rbdName', 'foo').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName');
    expect(req.request.body).toEqual('foo');
    expect(req.request.method).toBe('PUT');
  });

  it('should call get', () => {
    service.get('poolName', 'rbdName').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName');
    expect(req.request.method).toBe('GET');
  });

  it('should call list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/block/image');
    expect(req.request.method).toBe('GET');
  });

  it('should call copy', () => {
    service.copy('poolName', 'rbdName', 'foo').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName/copy');
    expect(req.request.body).toEqual('foo');
    expect(req.request.method).toBe('POST');
  });

  it('should call flatten', () => {
    service.flatten('poolName', 'rbdName').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName/flatten');
    expect(req.request.body).toEqual(null);
    expect(req.request.method).toBe('POST');
  });

  it('should call defaultFeatures', () => {
    service.defaultFeatures().subscribe();
    const req = httpTesting.expectOne('api/block/image/default_features');
    expect(req.request.method).toBe('GET');
  });

  it('should call createSnapshot', () => {
    service.createSnapshot('poolName', 'rbdName', 'snapshotName').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName/snap');
    expect(req.request.body).toEqual({
      snapshot_name: 'snapshotName'
    });
    expect(req.request.method).toBe('POST');
  });

  it('should call renameSnapshot', () => {
    service.renameSnapshot('poolName', 'rbdName', 'snapshotName', 'foo').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName/snap/snapshotName');
    expect(req.request.body).toEqual({
      new_snap_name: 'foo'
    });
    expect(req.request.method).toBe('PUT');
  });

  it('should call protectSnapshot', () => {
    service.protectSnapshot('poolName', 'rbdName', 'snapshotName', true).subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName/snap/snapshotName');
    expect(req.request.body).toEqual({
      is_protected: true
    });
    expect(req.request.method).toBe('PUT');
  });

  it('should call rollbackSnapshot', () => {
    service.rollbackSnapshot('poolName', 'rbdName', 'snapshotName').subscribe();
    const req = httpTesting.expectOne(
      'api/block/image/poolName/rbdName/snap/snapshotName/rollback'
    );
    expect(req.request.body).toEqual(null);
    expect(req.request.method).toBe('POST');
  });

  it('should call cloneSnapshot', () => {
    service.cloneSnapshot('poolName', 'rbdName', 'snapshotName', null).subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName/snap/snapshotName/clone');
    expect(req.request.body).toEqual(null);
    expect(req.request.method).toBe('POST');
  });

  it('should call deleteSnapshot', () => {
    service.deleteSnapshot('poolName', 'rbdName', 'snapshotName').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName/snap/snapshotName');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call moveTrash', () => {
    service.moveTrash('poolName', 'rbdName', 1).subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName/rbdName/move_trash');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ delay: 1 });
  });
});
