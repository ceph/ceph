import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { cdEncode, cdEncodeNot } from '../decorators/cd-encode';

@cdEncode
@Injectable()
export class RbdService {
  constructor(private http: HttpClient) {}

  create(rbd) {
    return this.http.post('api/block/image', rbd, { observe: 'response' });
  }

  delete(poolName, rbdName) {
    return this.http.delete(`api/block/image/${poolName}/${rbdName}`, { observe: 'response' });
  }

  update(poolName, rbdName, rbd) {
    return this.http.put(`api/block/image/${poolName}/${rbdName}`, rbd, { observe: 'response' });
  }

  get(poolName, rbdName) {
    return this.http.get(`api/block/image/${poolName}/${rbdName}`);
  }

  list() {
    return this.http.get('api/block/image');
  }

  copy(poolName, rbdName, rbd) {
    return this.http.post(`api/block/image/${poolName}/${rbdName}/copy`, rbd, {
      observe: 'response'
    });
  }

  flatten(poolName, rbdName) {
    return this.http.post(`api/block/image/${poolName}/${rbdName}/flatten`, null, {
      observe: 'response'
    });
  }

  defaultFeatures() {
    return this.http.get('api/block/image/default_features');
  }

  createSnapshot(poolName, rbdName, @cdEncodeNot snapshotName) {
    const request = {
      snapshot_name: snapshotName
    };
    return this.http.post(`api/block/image/${poolName}/${rbdName}/snap`, request, {
      observe: 'response'
    });
  }

  renameSnapshot(poolName, rbdName, snapshotName, @cdEncodeNot newSnapshotName) {
    const request = {
      new_snap_name: newSnapshotName
    };
    return this.http.put(`api/block/image/${poolName}/${rbdName}/snap/${snapshotName}`, request, {
      observe: 'response'
    });
  }

  protectSnapshot(poolName, rbdName, snapshotName, @cdEncodeNot isProtected) {
    const request = {
      is_protected: isProtected
    };
    return this.http.put(`api/block/image/${poolName}/${rbdName}/snap/${snapshotName}`, request, {
      observe: 'response'
    });
  }

  rollbackSnapshot(poolName, rbdName, snapshotName) {
    return this.http.post(
      `api/block/image/${poolName}/${rbdName}/snap/${snapshotName}/rollback`,
      null,
      { observe: 'response' }
    );
  }

  cloneSnapshot(poolName, rbdName, snapshotName, request) {
    return this.http.post(
      `api/block/image/${poolName}/${rbdName}/snap/${snapshotName}/clone`,
      request,
      { observe: 'response' }
    );
  }

  deleteSnapshot(poolName, rbdName, snapshotName) {
    return this.http.delete(`api/block/image/${poolName}/${rbdName}/snap/${snapshotName}`, {
      observe: 'response'
    });
  }
}
