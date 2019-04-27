import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { map } from 'rxjs/operators';

import { cdEncode, cdEncodeNot } from '../decorators/cd-encode';
import { RbdConfigurationService } from '../services/rbd-configuration.service';
import { ApiModule } from './api.module';
import { RbdPool } from './rbd.model';

@cdEncode
@Injectable({
  providedIn: ApiModule
})
export class RbdService {
  constructor(private http: HttpClient, private rbdConfigurationService: RbdConfigurationService) {}

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
    return this.http.get<RbdPool[]>('api/block/image').pipe(
      map((pools) =>
        pools.map((pool) => {
          pool.value.map((image) => {
            if (!image.configuration) {
              return image;
            }
            image.configuration.map((option) =>
              Object.assign(option, this.rbdConfigurationService.getOptionByName(option.name))
            );
            return image;
          });
          return pool;
        })
      )
    );
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

  listTrash() {
    return this.http.get(`api/block/image/trash/`);
  }

  moveTrash(poolName, rbdName, delay) {
    return this.http.post(
      `api/block/image/${poolName}/${rbdName}/move_trash`,
      { delay: delay },
      { observe: 'response' }
    );
  }

  purgeTrash(poolName) {
    return this.http.post(`api/block/image/trash/purge/?pool_name=${poolName}`, null, {
      observe: 'response'
    });
  }

  restoreTrash(poolName, imageId, newImageName) {
    return this.http.post(
      `api/block/image/trash/${poolName}/${imageId}/restore`,
      { new_image_name: newImageName },
      { observe: 'response' }
    );
  }

  removeTrash(poolName, imageId, imageName, force = false) {
    return this.http.delete(
      `api/block/image/trash/${poolName}/${imageId}/?image_name=${imageName}&force=${force}`,
      { observe: 'response' }
    );
  }
}
