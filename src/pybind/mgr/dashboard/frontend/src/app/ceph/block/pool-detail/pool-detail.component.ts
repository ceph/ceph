import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { PoolService } from '../../../shared/api/pool.service';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';

@Component({
  selector: 'cd-pool-detail',
  templateUrl: './pool-detail.component.html',
  styleUrls: ['./pool-detail.component.scss']
})
export class PoolDetailComponent implements OnInit, OnDestroy {
  @ViewChild('parentTpl') parentTpl: TemplateRef<any>;

  name: string;
  images: any;
  columns: CdTableColumn[];
  retries: number;
  routeParamsSubscribe: any;
  viewCacheStatus: ViewCacheStatus;

  constructor(
    private route: ActivatedRoute,
    private poolService: PoolService,
    dimlessBinaryPipe: DimlessBinaryPipe,
    dimlessPipe: DimlessPipe
  ) {
    this.columns = [
      {
        name: 'Name',
        prop: 'name',
        cellTemplate: this.parentTpl,
        flexGrow: 2
      },
      {
        name: 'Size',
        prop: 'size',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: dimlessBinaryPipe
      },
      {
        name: 'Objects',
        prop: 'num_objs',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: dimlessPipe
      },
      {
        name: 'Object size',
        prop: 'obj_size',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: dimlessBinaryPipe
      },
      {
        name: 'Features',
        prop: 'features_name',
        flexGrow: 3
      },
      {
        name: 'Parent',
        prop: 'parent',
        cellTemplate: this.parentTpl,
        flexGrow: 2
      }
    ];
  }

  ngOnInit() {
    this.routeParamsSubscribe = this.route.params.subscribe((params: { name: string }) => {
      this.name = params.name;
      this.images = [];
      this.retries = 0;
    });
  }

  ngOnDestroy() {
    this.routeParamsSubscribe.unsubscribe();
  }

  loadImages() {
    this.poolService.rbdPoolImages(this.name).then(
      resp => {
        this.viewCacheStatus = resp[0].status;
        this.images = resp[0].value;
      },
      () => {
        this.viewCacheStatus = ViewCacheStatus.ValueException;
      }
    );
  }
}
