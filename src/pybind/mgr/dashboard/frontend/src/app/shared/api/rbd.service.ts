import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { map } from 'rxjs/operators';

import { cdEncode, cdEncodeNot } from '../decorators/cd-encode';
import { ImageSpec } from '../models/image-spec';
import { RbdConfigurationService } from '../services/rbd-configuration.service';
import { ApiModule } from './api.module';
import { RbdPool } from './rbd.model';

@cdEncode
@Injectable({
  providedIn: ApiModule
})
export class RbdService {
  constructor(private http: HttpClient, private rbdConfigurationService: RbdConfigurationService) {}

  isRBDPool(pool) {
    return _.indexOf(pool.application_metadata, 'rbd') !== -1 && !pool.pool_name.includes('/');
  }

  create(rbd) {
    return this.http.post('api/block/image', rbd, { observe: 'response' });
  }

  delete(imageSpec: ImageSpec) {
    return this.http.delete(`api/block/image/${imageSpec.toStringEncoded()}`, {
      observe: 'response'
    });
  }

  update(imageSpec: ImageSpec, rbd) {
    return this.http.put(`api/block/image/${imageSpec.toStringEncoded()}`, rbd, {
      observe: 'response'
    });
  }

  get(imageSpec: ImageSpec) {
    return this.http.get(`api/block/image/${imageSpec.toStringEncoded()}`);
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

  copy(imageSpec: ImageSpec, rbd) {
    return this.http.post(`api/block/image/${imageSpec.toStringEncoded()}/copy`, rbd, {
      observe: 'response'
    });
  }

  flatten(imageSpec: ImageSpec) {
    return this.http.post(`api/block/image/${imageSpec.toStringEncoded()}/flatten`, null, {
      observe: 'response'
    });
  }

  defaultFeatures() {
    return this.http.get('api/block/image/default_features');
  }

  createSnapshot(imageSpec: ImageSpec, @cdEncodeNot snapshotName) {
    const request = {
      snapshot_name: snapshotName
    };
    return this.http.post(`api/block/image/${imageSpec.toStringEncoded()}/snap`, request, {
      observe: 'response'
    });
  }

  renameSnapshot(imageSpec: ImageSpec, snapshotName, @cdEncodeNot newSnapshotName) {
    const request = {
      new_snap_name: newSnapshotName
    };
    return this.http.put(
      `api/block/image/${imageSpec.toStringEncoded()}/snap/${snapshotName}`,
      request,
      {
        observe: 'response'
      }
    );
  }

  protectSnapshot(imageSpec: ImageSpec, snapshotName, @cdEncodeNot isProtected) {
    const request = {
      is_protected: isProtected
    };
    return this.http.put(
      `api/block/image/${imageSpec.toStringEncoded()}/snap/${snapshotName}`,
      request,
      {
        observe: 'response'
      }
    );
  }

  rollbackSnapshot(imageSpec: ImageSpec, snapshotName) {
    return this.http.post(
      `api/block/image/${imageSpec.toStringEncoded()}/snap/${snapshotName}/rollback`,
      null,
      { observe: 'response' }
    );
  }

  cloneSnapshot(imageSpec: ImageSpec, snapshotName, request) {
    return this.http.post(
      `api/block/image/${imageSpec.toStringEncoded()}/snap/${snapshotName}/clone`,
      request,
      { observe: 'response' }
    );
  }

  deleteSnapshot(imageSpec: ImageSpec, snapshotName) {
    return this.http.delete(`api/block/image/${imageSpec.toStringEncoded()}/snap/${snapshotName}`, {
      observe: 'response'
    });
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

  moveTrash(imageSpec: ImageSpec, delay) {
    return this.http.post(
      `api/block/image/${imageSpec.toStringEncoded()}/move_trash`,
      { delay: delay },
      { observe: 'response' }
    );
  }

  purgeTrash(poolName) {
    return this.http.post(`api/block/image/trash/purge/?pool_name=${poolName}`, null, {
      observe: 'response'
    });
  }

  restoreTrash(imageSpec: ImageSpec, @cdEncodeNot newImageName: string) {
    return this.http.post(
      `api/block/image/trash/${imageSpec.toStringEncoded()}/restore`,
      { new_image_name: newImageName },
      { observe: 'response' }
    );
  }

  removeTrash(imageSpec: ImageSpec, force = false) {
    return this.http.delete(
      `api/block/image/trash/${imageSpec.toStringEncoded()}/?force=${force}`,
      { observe: 'response' }
    );
  }
}
