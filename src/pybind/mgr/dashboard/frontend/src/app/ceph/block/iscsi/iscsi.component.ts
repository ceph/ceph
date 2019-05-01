import { Component } from '@angular/core';

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

  constructor(private tcmuIscsiService: TcmuIscsiService,
              cephShortVersionPipe: CephShortVersionPipe,
              dimlessPipe: DimlessPipe,
              relativeDatePipe: RelativeDatePipe,
              listPipe: ListPipe) {
    this.daemonsColumns = [
      {
        name: 'Hostname',
        prop: 'server_hostname'
      },
      {
        name: '# Active/Optimized',
        prop: 'optimized_paths',
      },
      {
        name: '# Active/Non-Optimized',
        prop: 'non_optimized_paths'
      },
      {
        name: 'Version',
        prop: 'version',
        pipe: cephShortVersionPipe
      }
    ];
    this.imagesColumns = [
      {
        name: 'Pool',
        prop: 'pool_name'
      },
      {
        name: 'Image',
        prop: 'name'
      },
      {
        name: 'Active/Optimized',
        prop: 'optimized_paths',
        pipe: listPipe
      },
      {
        name: 'Active/Non-Optimized',
        prop: 'non_optimized_paths',
        pipe: listPipe
      },
      {
        name: 'Read Bytes',
        prop: 'stats_history.rd_bytes',
        cellTransformation: CellTemplate.sparkline
      },
      {
        name: 'Write Bytes',
        prop: 'stats_history.wr_bytes',
        cellTransformation: CellTemplate.sparkline
      },
      {
        name: 'Read Ops',
        prop: 'stats.rd',
        pipe: dimlessPipe,
        cellTransformation: CellTemplate.perSecond
      },
      {
        name: 'Write Ops',
        prop: 'stats.wr',
        pipe: dimlessPipe,
        cellTransformation: CellTemplate.perSecond
      },
      {
        name: 'A/O Since',
        prop: 'optimized_since',
        pipe: relativeDatePipe
      },
    ];

  }

  refresh() {
    this.tcmuIscsiService.tcmuiscsi().then((resp) => {
      this.daemons = resp.daemons;
      this.images = resp.images;
      this.images.map((image) => {
        image.stats_history.rd_bytes = image.stats_history.rd_bytes.map(i => i[1]);
        image.stats_history.wr_bytes = image.stats_history.wr_bytes.map(i => i[1]);
        image.cdIsBinary = true;
        return image;
      });
    });
  }

}
