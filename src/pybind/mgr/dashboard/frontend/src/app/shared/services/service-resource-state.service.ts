import { HttpParams } from '@angular/common/http';
import { Injectable, OnDestroy } from '@angular/core';
import { ReplaySubject, Subscription } from 'rxjs';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { CephServiceSpec, decodeServiceNameFromRoute } from '~/app/shared/models/service.interface';

@Injectable()
export class ServiceResourceStateService implements OnDestroy {
  private serviceSource = new ReplaySubject<CephServiceSpec | null>(1);
  private loadSubscription?: Subscription;

  readonly service$ = this.serviceSource.asObservable();

  constructor(private cephServiceService: CephServiceService) {}

  ngOnDestroy(): void {
    this.loadSubscription?.unsubscribe();
  }

  load(serviceNameRoute: string): void {
    this.loadSubscription?.unsubscribe();

    const serviceName = decodeServiceNameFromRoute(serviceNameRoute);

    if (!serviceName) {
      this.serviceSource.next(null);
      return;
    }

    this.loadSubscription = this.cephServiceService
      .list(new HttpParams(), serviceName)
      .observable.subscribe({
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
