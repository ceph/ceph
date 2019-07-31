import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
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

  getImageSpec(poolName: string, namespace: string, rbdName: string) {
    namespace = namespace ? `${namespace}/` : '';
    return `${poolName}/${namespace}${rbdName}`;
  }

  parseImageSpec(@cdEncodeNot imageSpec: string) {
    const imageSpecSplited = imageSpec.split('/');
    const poolName = imageSpecSplited[0];
    const namespace = imageSpecSplited.length >= 3 ? imageSpecSplited[1] : null;
    const imageName = imageSpecSplited.length >= 3 ? imageSpecSplited[2] : imageSpecSplited[1];
    return [poolName, namespace, imageName];
  }

  isRBDPool(pool) {
    return _.indexOf(pool.application_metadata, 'rbd') !== -1 && !pool.pool_name.includes('/');
  }

  create(rbd) {
    return this.http.post('api/block/image', rbd, { observe: 'response' });
  }

  delete(poolName, namespace, rbdName) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    return this.http.delete(`api/block/image/${encodeURIComponent(imageSpec)}`, {
      observe: 'response'
    });
  }

  update(poolName, namespace, rbdName, rbd) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    return this.http.put(`api/block/image/${encodeURIComponent(imageSpec)}`, rbd, {
      observe: 'response'
    });
  }

  get(poolName, namespace, rbdName) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    return this.http.get(`api/block/image/${encodeURIComponent(imageSpec)}`);
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

  copy(poolName, namespace, rbdName, rbd) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    return this.http.post(`api/block/image/${encodeURIComponent(imageSpec)}/copy`, rbd, {
      observe: 'response'
    });
  }

  flatten(poolName, namespace, rbdName) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    return this.http.post(`api/block/image/${encodeURIComponent(imageSpec)}/flatten`, null, {
      observe: 'response'
    });
  }

  defaultFeatures() {
    return this.http.get('api/block/image/default_features');
  }

  createSnapshot(poolName, namespace, rbdName, @cdEncodeNot snapshotName) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    const request = {
      snapshot_name: snapshotName
    };
    return this.http.post(`api/block/image/${encodeURIComponent(imageSpec)}/snap`, request, {
      observe: 'response'
    });
  }

  renameSnapshot(poolName, namespace, rbdName, snapshotName, @cdEncodeNot newSnapshotName) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    const request = {
      new_snap_name: newSnapshotName
    };
    return this.http.put(
      `api/block/image/${encodeURIComponent(imageSpec)}/snap/${snapshotName}`,
      request,
      {
        observe: 'response'
      }
    );
  }

  protectSnapshot(poolName, namespace, rbdName, snapshotName, @cdEncodeNot isProtected) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    const request = {
      is_protected: isProtected
    };
    return this.http.put(
      `api/block/image/${encodeURIComponent(imageSpec)}/snap/${snapshotName}`,
      request,
      {
        observe: 'response'
      }
    );
  }

  rollbackSnapshot(poolName, namespace, rbdName, snapshotName) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    return this.http.post(
      `api/block/image/${encodeURIComponent(imageSpec)}/snap/${snapshotName}/rollback`,
      null,
      { observe: 'response' }
    );
  }

  cloneSnapshot(poolName, namespace, rbdName, snapshotName, request) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    return this.http.post(
      `api/block/image/${encodeURIComponent(imageSpec)}/snap/${snapshotName}/clone`,
      request,
      { observe: 'response' }
    );
  }

  deleteSnapshot(poolName, namespace, rbdName, snapshotName) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    return this.http.delete(
      `api/block/image/${encodeURIComponent(imageSpec)}/snap/${snapshotName}`,
      {
        observe: 'response'
      }
    );
  }

  listTrash() {
    return this.http.get(`api/block/image/trash/`);
  }

  createNamespace(pool, namespace) {
    const request = {
      namespace: namespace
    };
    return this.http.post(`api/block/pool/${pool}/namespace`, request, { observe: 'response' });
  }

  listNamespaces(pool) {
    return this.http.get(`api/block/pool/${pool}/namespace/`);
  }

  deleteNamespace(pool, namespace) {
    return this.http.delete(`api/block/pool/${pool}/namespace/${namespace}`, {
      observe: 'response'
    });
  }

  moveTrash(poolName, namespace, rbdName, delay) {
    const imageSpec = this.getImageSpec(poolName, namespace, rbdName);
    return this.http.post(
      `api/block/image/${encodeURIComponent(imageSpec)}/move_trash`,
      { delay: delay },
      { observe: 'response' }
    );
  }

  purgeTrash(poolName) {
    return this.http.post(`api/block/image/trash/purge/?pool_name=${poolName}`, null, {
      observe: 'response'
    });
  }

  restoreTrash(poolName, namespace, imageId, newImageName) {
    const imageSpec = this.getImageSpec(poolName, namespace, imageId);
    return this.http.post(
      `api/block/image/trash/${encodeURIComponent(imageSpec)}/restore`,
      { new_image_name: newImageName },
      { observe: 'response' }
    );
  }

  removeTrash(poolName, namespace, imageId, force = false) {
    const imageSpec = this.getImageSpec(poolName, namespace, imageId);
    return this.http.delete(
      `api/block/image/trash/${encodeURIComponent(imageSpec)}/?force=${force}`,
      { observe: 'response' }
    );
  }
}
