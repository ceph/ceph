import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { map } from 'rxjs/operators';

import { ApiClient } from '~/app/shared/api/api-client';
import { cdEncode, cdEncodeNot } from '../decorators/cd-encode';
import { ImageSpec } from '../models/image-spec';
import { RbdConfigurationService } from '../services/rbd-configuration.service';
import { RbdPool } from './rbd.model';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class RbdService extends ApiClient {
  constructor(private http: HttpClient, private rbdConfigurationService: RbdConfigurationService) {
    super();
  }

  isRBDPool(pool: any) {
    return _.indexOf(pool.application_metadata, 'rbd') !== -1 && !pool.pool_name.includes('/');
  }

  create(rbd: any) {
    return this.http.post('api/block/image', rbd, { observe: 'response' });
  }

  delete(imageSpec: ImageSpec) {
    return this.http.delete(`api/block/image/${imageSpec.toStringEncoded()}`, {
      observe: 'response'
    });
  }

  update(imageSpec: ImageSpec, rbd: any) {
    return this.http.put(`api/block/image/${imageSpec.toStringEncoded()}`, rbd, {
      observe: 'response'
    });
  }

  get(imageSpec: ImageSpec) {
    return this.http.get(`api/block/image/${imageSpec.toStringEncoded()}`);
  }

  list(params: any) {
    return this.http
      .get<RbdPool[]>('api/block/image', {
        params: params,
        headers: { Accept: this.getVersionHeaderValue(2, 0) },
        observe: 'response'
      })
      .pipe(
        map((response: any) => {
          return response['body'].map((pool: any) => {
            pool.value.map((image: any) => {
              if (!image.configuration) {
                return image;
              }
              image.configuration.map((option: any) =>
                Object.assign(option, this.rbdConfigurationService.getOptionByName(option.name))
              );
              return image;
            });
            pool['headers'] = response.headers;
            return pool;
          });
        })
      );
  }

  copy(imageSpec: ImageSpec, rbd: any) {
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

  cloneFormatVersion() {
    return this.http.get<number>('api/block/image/clone_format_version');
  }

  createSnapshot(
    imageSpec: ImageSpec,
    @cdEncodeNot snapshotName: string,
    mirrorImageSnapshot: boolean
  ) {
    const request = {
      snapshot_name: snapshotName,
      mirrorImageSnapshot: mirrorImageSnapshot
    };
    return this.http.post(`api/block/image/${imageSpec.toStringEncoded()}/snap`, request, {
      observe: 'response'
    });
  }

  renameSnapshot(imageSpec: ImageSpec, snapshotName: string, @cdEncodeNot newSnapshotName: string) {
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

  protectSnapshot(imageSpec: ImageSpec, snapshotName: string, @cdEncodeNot isProtected: boolean) {
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

  rollbackSnapshot(imageSpec: ImageSpec, snapshotName: string) {
    return this.http.post(
      `api/block/image/${imageSpec.toStringEncoded()}/snap/${snapshotName}/rollback`,
      null,
      { observe: 'response' }
    );
  }

  cloneSnapshot(imageSpec: ImageSpec, snapshotName: string, request: any) {
    return this.http.post(
      `api/block/image/${imageSpec.toStringEncoded()}/snap/${snapshotName}/clone`,
      request,
      { observe: 'response' }
    );
  }

  deleteSnapshot(imageSpec: ImageSpec, snapshotName: string) {
    return this.http.delete(`api/block/image/${imageSpec.toStringEncoded()}/snap/${snapshotName}`, {
      observe: 'response'
    });
  }

  listTrash() {
    return this.http.get(`api/block/image/trash/`);
  }

  createNamespace(pool: string, namespace: string) {
    const request = {
      namespace: namespace
    };
    return this.http.post(`api/block/pool/${pool}/namespace`, request, { observe: 'response' });
  }

  listNamespaces(pool: string) {
    return this.http.get(`api/block/pool/${pool}/namespace/`);
  }

  deleteNamespace(pool: string, namespace: string) {
    return this.http.delete(`api/block/pool/${pool}/namespace/${namespace}`, {
      observe: 'response'
    });
  }

  moveTrash(imageSpec: ImageSpec, delay: number) {
    return this.http.post(
      `api/block/image/${imageSpec.toStringEncoded()}/move_trash`,
      { delay: delay },
      { observe: 'response' }
    );
  }

  purgeTrash(poolName: string) {
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
