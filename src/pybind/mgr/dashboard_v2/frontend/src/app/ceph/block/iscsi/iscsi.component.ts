import { Component } from '@angular/core';

import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { ListPipe } from '../../../shared/pipes/list.pipe';
import { RelativeDatePipe } from '../../../shared/pipes/relative-date.pipe';
import { TcmuIscsiService } from '../../../shared/services/tcmu-iscsi.service';

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
              dimlessBinaryPipe: DimlessBinaryPipe,
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
        prop: 'stats.rd_bytes',
        pipe: dimlessBinaryPipe
      },
      {
        name: 'Write Bytes',
        prop: 'stats.wr_bytes',
        pipe: dimlessBinaryPipe
      },
      {
        name: 'Read Ops',
        prop: 'stats.rd',
        pipe: dimlessPipe
      },
      {
        name: 'Write Ops',
        prop: 'stats.wr',
        pipe: dimlessPipe
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
    });
  }

}
