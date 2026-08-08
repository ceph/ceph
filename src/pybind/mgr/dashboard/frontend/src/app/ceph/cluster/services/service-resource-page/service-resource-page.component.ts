import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs';

import { URLVerbs } from '~/app/shared/constants/app.constants';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { CephServiceCertificate, CephServiceSpec } from '~/app/shared/models/service.interface';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';
import { ServiceResourceStateService } from '~/app/shared/services/service-resource-state.service';
import { PlacementPipe } from '../placement.pipe';
import { ServiceCertificateStatusPipe } from '~/app/shared/pipes/service-certificate-status.pipe';

@Component({
  selector: 'cd-service-resource-page',
  templateUrl: './service-resource-page.component.html',
  styleUrls: ['./service-resource-page.component.scss'],
  providers: [CdDatePipe, RelativeDatePipe, ServiceCertificateStatusPipe],
  standalone: false
})
export class ServiceResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  section = 'overview';
  service?: CephServiceSpec;
  notFound = false;
  overviewFields: OverviewField[] = [];
  private placementPipe = new PlacementPipe();

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private serviceResourceStateService: ServiceResourceStateService,
    private relativeDatePipe: RelativeDatePipe,
    private certStatusPipe: ServiceCertificateStatusPipe
  ) {}

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? 'overview';

    this.sub.add(
      this.serviceResourceStateService.service$.subscribe((service) => {
        this.applyService(service);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  get certificate(): CephServiceCertificate | undefined {
    return this.service?.certificate;
  }

  get hasCertificate(): boolean {
    return !!this.certificate?.has_certificate;
  }

  onEditService(payload: { serviceName?: string; serviceType?: string }) {
    const serviceName = payload?.serviceName ?? this.service?.service_name;
    const serviceType = payload?.serviceType ?? this.service?.service_type;

    this.router.navigate([
      '/services',
      {
        outlets: {
          modal: [URLVerbs.EDIT, serviceType, serviceName]
        }
      }
    ]);
  }

  private applyService(service: CephServiceSpec | null): void {
    if (!service) {
      this.notFound = true;
      this.service = undefined;
      this.overviewFields = [];
      return;
    }

    this.service = service;
    this.notFound = false;
    this.overviewFields = this.buildOverviewFields(service);
  }

  private buildOverviewFields(service: CephServiceSpec): OverviewField[] {
    return [
      {
        label: $localize`Service`,
        value: service.service_name
      },
      {
        label: $localize`Placement`,
        value: this.placementPipe.transform(service)
      },
      {
        label: $localize`Running`,
        value: `${service.status?.running ?? 0} / ${service.status?.size ?? 0}`
      },
      {
        label: $localize`Last Refreshed`,
        value: this.relativeDatePipe.transform(service.status?.last_refresh)
      },
      {
        label: $localize`Ports`,
        value: this.formatPorts((service as any)?.status?.ports)
      },
      {
        label: $localize`Certificate Status`,
        value: this.certStatusPipe.transform(service.certificate)
      }
    ];
  }

  private formatPorts(value: unknown): string {
    if (Array.isArray(value)) {
      return value.length ? value.join(', ') : '-';
    }

    if (value === undefined || value === null || value === '') {
      return '-';
    }

    return String(value);
  }
}
