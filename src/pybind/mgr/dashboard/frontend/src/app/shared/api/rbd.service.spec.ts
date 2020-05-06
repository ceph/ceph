import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { ImageSpec } from '../models/image-spec';
import { RbdConfigurationService } from '../services/rbd-configuration.service';
import { RbdService } from './rbd.service';

describe('RbdService', () => {
  let service: RbdService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [RbdService, RbdConfigurationService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(RbdService);
    httpTesting = TestBed.inject(HttpTestingController);
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
    service.delete(new ImageSpec('poolName', null, 'rbdName')).subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call update', () => {
    service.update(new ImageSpec('poolName', null, 'rbdName'), 'foo').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName');
    expect(req.request.body).toEqual('foo');
    expect(req.request.method).toBe('PUT');
  });

  it('should call get', () => {
    service.get(new ImageSpec('poolName', null, 'rbdName')).subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName');
    expect(req.request.method).toBe('GET');
  });

  it('should call list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/block/image');
    expect(req.request.method).toBe('GET');
  });

  it('should call copy', () => {
    service.copy(new ImageSpec('poolName', null, 'rbdName'), 'foo').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName/copy');
    expect(req.request.body).toEqual('foo');
    expect(req.request.method).toBe('POST');
  });

  it('should call flatten', () => {
    service.flatten(new ImageSpec('poolName', null, 'rbdName')).subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName/flatten');
    expect(req.request.body).toEqual(null);
    expect(req.request.method).toBe('POST');
  });

  it('should call defaultFeatures', () => {
    service.defaultFeatures().subscribe();
    const req = httpTesting.expectOne('api/block/image/default_features');
    expect(req.request.method).toBe('GET');
  });

  it('should call createSnapshot', () => {
    service.createSnapshot(new ImageSpec('poolName', null, 'rbdName'), 'snapshotName').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName/snap');
    expect(req.request.body).toEqual({
      snapshot_name: 'snapshotName'
    });
    expect(req.request.method).toBe('POST');
  });

  it('should call renameSnapshot', () => {
    service
      .renameSnapshot(new ImageSpec('poolName', null, 'rbdName'), 'snapshotName', 'foo')
      .subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName/snap/snapshotName');
    expect(req.request.body).toEqual({
      new_snap_name: 'foo'
    });
    expect(req.request.method).toBe('PUT');
  });

  it('should call protectSnapshot', () => {
    service
      .protectSnapshot(new ImageSpec('poolName', null, 'rbdName'), 'snapshotName', true)
      .subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName/snap/snapshotName');
    expect(req.request.body).toEqual({
      is_protected: true
    });
    expect(req.request.method).toBe('PUT');
  });

  it('should call rollbackSnapshot', () => {
    service
      .rollbackSnapshot(new ImageSpec('poolName', null, 'rbdName'), 'snapshotName')
      .subscribe();
    const req = httpTesting.expectOne(
      'api/block/image/poolName%2FrbdName/snap/snapshotName/rollback'
    );
    expect(req.request.body).toEqual(null);
    expect(req.request.method).toBe('POST');
  });

  it('should call cloneSnapshot', () => {
    service
      .cloneSnapshot(new ImageSpec('poolName', null, 'rbdName'), 'snapshotName', null)
      .subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName/snap/snapshotName/clone');
    expect(req.request.body).toEqual(null);
    expect(req.request.method).toBe('POST');
  });

  it('should call deleteSnapshot', () => {
    service.deleteSnapshot(new ImageSpec('poolName', null, 'rbdName'), 'snapshotName').subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName/snap/snapshotName');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call moveTrash', () => {
    service.moveTrash(new ImageSpec('poolName', null, 'rbdName'), 1).subscribe();
    const req = httpTesting.expectOne('api/block/image/poolName%2FrbdName/move_trash');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ delay: 1 });
  });

  describe('should compose image spec', () => {
    it('with namespace', () => {
      expect(new ImageSpec('mypool', 'myns', 'myimage').toString()).toBe('mypool/myns/myimage');
    });

    it('without namespace', () => {
      expect(new ImageSpec('mypool', null, 'myimage').toString()).toBe('mypool/myimage');
    });
  });

  describe('should parse image spec', () => {
    it('with namespace', () => {
      const imageSpec = ImageSpec.fromString('mypool/myns/myimage');
      expect(imageSpec.poolName).toBe('mypool');
      expect(imageSpec.namespace).toBe('myns');
      expect(imageSpec.imageName).toBe('myimage');
    });

    it('without namespace', () => {
      const imageSpec = ImageSpec.fromString('mypool/myimage');
      expect(imageSpec.poolName).toBe('mypool');
      expect(imageSpec.namespace).toBeNull();
      expect(imageSpec.imageName).toBe('myimage');
    });
  });
});
