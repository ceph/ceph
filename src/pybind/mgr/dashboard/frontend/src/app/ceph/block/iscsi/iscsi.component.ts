import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { IscsiService } from '../../../shared/api/iscsi.service';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { IscsiBackstorePipe } from '../../../shared/pipes/iscsi-backstore.pipe';

@Component({
  selector: 'cd-iscsi',
  templateUrl: './iscsi.component.html',
  styleUrls: ['./iscsi.component.scss']
})
export class IscsiComponent implements OnInit {
  @ViewChild('iscsiSparklineTpl', { static: true })
  iscsiSparklineTpl: TemplateRef<any>;
  @ViewChild('iscsiPerSecondTpl', { static: true })
  iscsiPerSecondTpl: TemplateRef<any>;
  @ViewChild('iscsiRelativeDateTpl', { static: true })
  iscsiRelativeDateTpl: TemplateRef<any>;

  gateways: any[] = [];
  gatewaysColumns: any;
  images: any[] = [];
  imagesColumns: any;

  constructor(
    private iscsiService: IscsiService,
    private dimlessPipe: DimlessPipe,
    private iscsiBackstorePipe: IscsiBackstorePipe
  ) {}

  ngOnInit() {
    this.gatewaysColumns = [
      {
        name: $localize`Name`,
        prop: 'name'
      },
      {
        name: $localize`State`,
        prop: 'state',
        flexGrow: 1,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            up: { class: 'badge-success' },
            down: { class: 'badge-danger' }
          }
        }
      },
      {
        name: $localize`# Targets`,
        prop: 'num_targets'
      },
      {
        name: $localize`# Sessions`,
        prop: 'num_sessions'
      }
    ];
    this.imagesColumns = [
      {
        name: $localize`Pool`,
        prop: 'pool'
      },
      {
        name: $localize`Image`,
        prop: 'image'
      },
      {
        name: $localize`Backstore`,
        prop: 'backstore',
        pipe: this.iscsiBackstorePipe
      },
      {
        name: $localize`Read Bytes`,
        prop: 'stats_history.rd_bytes',
        cellTemplate: this.iscsiSparklineTpl
      },
      {
        name: $localize`Write Bytes`,
        prop: 'stats_history.wr_bytes',
        cellTemplate: this.iscsiSparklineTpl
      },
      {
        name: $localize`Read Ops`,
        prop: 'stats.rd',
        pipe: this.dimlessPipe,
        cellTemplate: this.iscsiPerSecondTpl
      },
      {
        name: $localize`Write Ops`,
        prop: 'stats.wr',
        pipe: this.dimlessPipe,
        cellTemplate: this.iscsiPerSecondTpl
      },
      {
        name: $localize`A/O Since`,
        prop: 'optimized_since',
        cellTemplate: this.iscsiRelativeDateTpl
      }
    ];
  }

  refresh() {
    this.iscsiService.overview().subscribe((overview: object) => {
      this.gateways = overview['gateways'];
      this.images = overview['images'];
      this.images.map((image) => {
        if (image.stats_history) {
          image.stats_history.rd_bytes = image.stats_history.rd_bytes.map((i: any) => i[1]);
          image.stats_history.wr_bytes = image.stats_history.wr_bytes.map((i: any) => i[1]);
        }
        image.cdIsBinary = true;
        return image;
      });
    });
  }
}
