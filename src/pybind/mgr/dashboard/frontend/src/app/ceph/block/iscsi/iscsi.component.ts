import { Component } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { TcmuIscsiService } from '../../../shared/api/tcmu-iscsi.service';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { ListPipe } from '../../../shared/pipes/list.pipe';
import { RelativeDatePipe } from '../../../shared/pipes/relative-date.pipe';

@Component({
  selector: 'cd-iscsi',
  templateUrl: './iscsi.component.html',
  styleUrls: ['./iscsi.component.scss']
})
export class IscsiComponent {
  daemons = [];
  daemonsColumns: any;
  images = [];
  imagesColumns: any;

  constructor(
    private tcmuIscsiService: TcmuIscsiService,
    cephShortVersionPipe: CephShortVersionPipe,
    dimlessPipe: DimlessPipe,
    relativeDatePipe: RelativeDatePipe,
    listPipe: ListPipe,
    private i18n: I18n
  ) {
    this.daemonsColumns = [
      {
        name: this.i18n('Hostname'),
        prop: 'server_hostname'
      },
      {
        name: this.i18n('# Active/Optimized'),
        prop: 'optimized_paths'
      },
      {
        name: this.i18n('# Active/Non-Optimized'),
        prop: 'non_optimized_paths'
      },
      {
        name: this.i18n('Version'),
        prop: 'version',
        pipe: cephShortVersionPipe
      }
    ];
    this.imagesColumns = [
      {
        name: this.i18n('Pool'),
        prop: 'pool_name'
      },
      {
        name: this.i18n('Image'),
        prop: 'name'
      },
      {
        name: this.i18n('Active/Optimized'),
        prop: 'optimized_paths',
        pipe: listPipe
      },
      {
        name: this.i18n('Active/Non-Optimized'),
        prop: 'non_optimized_paths',
        pipe: listPipe
      },
      {
        name: this.i18n('Read Bytes'),
        prop: 'stats_history.rd_bytes',
        cellTransformation: CellTemplate.sparkline
      },
      {
        name: this.i18n('Write Bytes'),
        prop: 'stats_history.wr_bytes',
        cellTransformation: CellTemplate.sparkline
      },
      {
        name: this.i18n('Read Ops'),
        prop: 'stats.rd',
        pipe: dimlessPipe,
        cellTransformation: CellTemplate.perSecond
      },
      {
        name: this.i18n('Write Ops'),
        prop: 'stats.wr',
        pipe: dimlessPipe,
        cellTransformation: CellTemplate.perSecond
      },
      {
        name: this.i18n('A/O Since'),
        prop: 'optimized_since',
        pipe: relativeDatePipe
      }
    ];
  }

  refresh() {
    this.tcmuIscsiService.tcmuiscsi().subscribe((resp: any) => {
      this.daemons = resp.daemons;
      this.images = resp.images;
      this.images.map((image) => {
        if (image.stats_history) {
          image.stats_history.rd_bytes = image.stats_history.rd_bytes.map((i) => i[1]);
          image.stats_history.wr_bytes = image.stats_history.wr_bytes.map((i) => i[1]);
        }
        image.cdIsBinary = true;
        return image;
      });
    });
  }
}
