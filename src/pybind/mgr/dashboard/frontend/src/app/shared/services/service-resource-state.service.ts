import { HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ReplaySubject } from 'rxjs';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { CephServiceSpec, decodeServiceNameFromRoute } from '~/app/shared/models/service.interface';

@Injectable()
export class ServiceResourceStateService {
  private serviceSource = new ReplaySubject<CephServiceSpec | null>(1);

  readonly service$ = this.serviceSource.asObservable();

  constructor(private cephServiceService: CephServiceService) {}

  load(serviceNameRoute: string): void {
    const serviceName = decodeServiceNameFromRoute(serviceNameRoute);
    if (!serviceName) {
      this.serviceSource.next(null);
      return;
    }

    this.cephServiceService.list(new HttpParams(), serviceName).observable.subscribe({
      next: (services: CephServiceSpec[]) => {
        const service =
          services.find((item: CephServiceSpec) => item.service_name === serviceName) ?? null;
        this.serviceSource.next(service);
      },
      error: () => {
        this.serviceSource.next(null);
      }
    });
  }
}
