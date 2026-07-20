import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { cdEncode } from '../decorators/cd-encode';
import { RbdConfigurationEntry } from '../models/configuration';
import { RbdConfigurationService } from '../services/rbd-configuration.service';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class PoolService {
  apiPath = 'api/pool';

  formTooltips = {
    compressionModes: {
      none: $localize`None: Never compress data.`,
      passive: $localize`Passive: Do not compress data unless the write operation has a compressible hint set.`,
      aggressive: $localize`Aggressive: Compress data unless the write operation has an incompressible hint set.`,
      force: $localize`Force: Try to compress data no matter what.`
    },
    pgAutoscaleModes: {
      off: $localize`Disable autoscaling for this pool. PGs distribute data in Ceph, and autoscaling auto-adjusts their count per pool as usage changes.`,
      on: $localize`Enable automated adjustments of the PG count for the given pool. PGs distribute data in Ceph, and autoscaling auto-adjusts their count per pool as usage changes.`,
      warn: $localize`Raise health checks when the PG count is in need of adjustment. PGs distribute data in Ceph, and autoscaling auto-adjusts their count per pool as usage changes.`
    }
  };

  constructor(private http: HttpClient, private rbdConfigurationService: RbdConfigurationService) {}

  create(pool: any) {
    return this.http.post(this.apiPath, pool, { observe: 'response' });
  }

  update(pool: any) {
    let name: string;
    if (pool.hasOwnProperty('srcpool')) {
      name = pool.srcpool;
      delete pool.srcpool;
    } else {
      name = pool.pool;
      delete pool.pool;
    }
    return this.http.put(`${this.apiPath}/${encodeURIComponent(name)}`, pool, {
      observe: 'response'
    });
  }

  delete(name: string) {
    return this.http.delete(`${this.apiPath}/${name}`, { observe: 'response' });
  }

  get(poolName: string) {
    return this.http.get(`${this.apiPath}/${poolName}`);
  }

  getList() {
    return this.http.get(`${this.apiPath}?stats=true`);
  }

  getConfiguration(poolName: string): Observable<RbdConfigurationEntry[]> {
    return this.http.get<RbdConfigurationEntry[]>(`${this.apiPath}/${poolName}/configuration`).pipe(
      // Add static data maintained in RbdConfigurationService
      map((values) =>
        values.map((entry) =>
          Object.assign(entry, this.rbdConfigurationService.getOptionByName(entry.name))
        )
      )
    );
  }

  getInfo() {
    return this.http.get(`ui-${this.apiPath}/info`);
  }

  list(attrs: string[] = []) {
    const attrsStr = attrs.join(',');
    return this.http
      .get(`${this.apiPath}?attrs=${attrsStr}`)
      .toPromise()
      .then((resp: any) => {
        return resp;
      });
  }
}
