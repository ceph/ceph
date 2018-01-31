import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { PoolService } from '../../../shared/services/pool.service';

@Component({
  selector: 'cd-pool-detail',
  templateUrl: './pool-detail.component.html',
  styleUrls: ['./pool-detail.component.scss']
})
export class PoolDetailComponent implements OnInit, OnDestroy {

  name: string;
  images: any;
  columns: any;
  retries: number;
  maxRetries = 5;
  routeParamsSubscribe: any;

  constructor(private route: ActivatedRoute,
              private poolService: PoolService,
              dimlessBinaryPipe: DimlessBinaryPipe,
              dimlessPipe: DimlessPipe) {
    this.columns = [
      {
        name: 'Name',
        prop: 'name',
        width: 100
      },
      {
        name: 'Size',
        prop: 'size',
        width: 50,
        cellClass: 'text-right',
        pipe: dimlessBinaryPipe
      },
      {
        name: 'Objects',
        prop: 'num_objs',
        width: 50,
        cellClass: 'text-right',
        pipe: dimlessPipe
      },
      {
        name: 'Object size',
        prop: 'obj_size',
        width: 50,
        cellClass: 'text-right',
        pipe: dimlessBinaryPipe
      },
      {
        name: 'Features',
        prop: 'features_name',
        width: 150},
      {
        name: 'Parent',
        prop: 'parent',
        width: 100
      }
    ];
  }

  ngOnInit() {
    this.routeParamsSubscribe = this.route.params.subscribe((params: { name: string }) => {
      this.name = params.name;
      this.images = null;
      this.retries = 0;
      this.loadImages();
    });
  }

  ngOnDestroy() {
    this.routeParamsSubscribe.unsubscribe();
  }

  loadImages() {
    this.poolService.rbdPoolImages(this.name).then((resp) => {
      if (resp.status === ViewCacheStatus.ValueNone) {
        setTimeout(() => {
          this.retries++;
          if (this.retries <= this.maxRetries) {
            this.loadImages();
          }
        }, 1000);
      } else {
        this.retries = 0;
        this.images = resp.value;
      }
    });
  }
}
