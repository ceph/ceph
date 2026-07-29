import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';

import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { RbdConfigurationService } from '~/app/shared/services/rbd-configuration.service';
import { RbdFormModel } from '../rbd-form/rbd-form.model';
import { RbdConfigurationEntry } from '~/app/shared/models/configuration';
import { RbdImageResourceStateService } from '../../../shared/services/rbd-image-resource-state.service';

@Component({
  selector: 'cd-rbd-image-resource-page',
  templateUrl: './rbd-image-resource-page.component.html',
  styleUrls: ['./rbd-image-resource-page.component.scss'],
  standalone: false
})
export class RbdImageResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  section = '';
  selection: RbdFormModel;
  notFound = false;
  overviewFields: OverviewField[] = [];
  rbdDashboardUrl = '';

  constructor(
    private route: ActivatedRoute,
    private rbdImageResourceStateService: RbdImageResourceStateService,
    private rbdConfigurationService: RbdConfigurationService,
    private dimlessBinary: DimlessBinaryPipe,
    private dimless: DimlessPipe,
    private cdDate: CdDatePipe
  ) {}

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? 'overview';

    this.sub.add(
      this.rbdImageResourceStateService.image$.subscribe((image: RbdFormModel | null) => {
        this.applyImage(image);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private applyImage(image: RbdFormModel): void {
    this.notFound = !image;
    if (!image) {
      this.selection = undefined;
      this.overviewFields = [];
      this.rbdDashboardUrl = '';
      return;
    }

    this.selection = image;
    this.enrichConfiguration(this.selection);
    this.overviewFields = this.buildOverviewFields(image);
    this.rbdDashboardUrl = `rbd-details?var-pool=${image.pool_name}&var-image=${image.name}`;
  }

  private enrichConfiguration(image: RbdFormModel): void {
    if (!image?.configuration?.length) {
      return;
    }

    image.configuration = image.configuration.map((option: RbdConfigurationEntry) =>
      Object.assign(option, this.rbdConfigurationService.getOptionByName(option.name))
    );
  }

  private buildOverviewFields(image: RbdFormModel): OverviewField[] {
    return [
      { label: $localize`Name`, value: image.name },
      { label: $localize`Pool`, value: image.pool_name },
      { label: $localize`Namespace`, value: image.namespace },
      { label: $localize`Data Pool`, value: image.data_pool },
      { label: $localize`Created`, value: this.cdDate.transform(image['timestamp']) },
      { label: $localize`Size`, value: this.dimlessBinary.transform(image.size) },
      { label: $localize`Usage`, value: this.getUsageValue(image) },
      { label: $localize`Objects`, value: this.dimless.transform(image['num_objs']) },
      { label: $localize`Object size`, value: this.dimlessBinary.transform(image.obj_size) },
      {
        label: $localize`Mirroring`,
        value: this.getMirroringValue(image)
      },
      {
        label: $localize`Next Scheduled Snapshot`,
        value: this.getNextScheduledSnapshot(image)
      },
      {
        label: $localize`Features`,
        values: image['features_name'],
        type: 'tags'
      },
      {
        label: $localize`Provisioned`,
        value: this.getProvisionedValue(image['disk_usage'], image['features_name'])
      },
      {
        label: $localize`Total provisioned`,
        value: this.getProvisionedValue(image['total_disk_usage'], image['features_name'])
      },
      { label: $localize`Striping unit`, value: this.dimlessBinary.transform(image.stripe_unit) },
      { label: $localize`Striping count`, value: image.stripe_count },
      {
        label: $localize`Parent`,
        value: image['parent']
          ? `${image['parent'].pool_name}${image['parent'].pool_namespace ? `/${image['parent'].pool_namespace}` : ''}/${image['parent'].image_name}@${image['parent'].snap_name}`
          : undefined
      },
      { label: $localize`Block name prefix`, value: image['block_name_prefix'] },
      { label: $localize`Order`, value: image['order'] },
      { label: $localize`Format Version`, value: image['image_format'] }
    ];
  }

  private getProvisionedValue(value: number, features: string[] = []): string {
    return features.indexOf('fast-diff') === -1 ? 'N/A' : this.dimlessBinary.transform(value);
  }

  private getUsageValue(image: RbdFormModel): string {
    if (!image['features_name']?.includes('fast-diff')) return 'N/A';
    return image.size ? `${((image['disk_usage'] / image.size) * 100).toFixed(2)}%` : '0%';
  }

  private getMirroringValue(image: RbdFormModel): string {
    const values = [];
    if (Array.isArray(image.mirror_mode)) {
      values.push(image.mirror_mode[0]);
      values.push(image.mirror_mode[1]);
    } else {
      values.push(image.mirror_mode);
    }

    if (image['primary'] === true) {
      values.push($localize`primary`);
    } else if (image['primary'] === false) {
      values.push($localize`secondary`);
    }

    return values.filter(Boolean).join(' / ');
  }

  private getNextScheduledSnapshot(image: RbdFormModel): string | undefined {
    const time =
      (Array.isArray(image.mirror_mode) && image.mirror_mode[2]) ||
      image.schedule_info?.schedule_time;
    return time ? this.cdDate.transform(time) : undefined;
  }
}
