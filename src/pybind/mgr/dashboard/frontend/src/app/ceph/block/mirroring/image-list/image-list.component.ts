import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { Subscription } from 'rxjs';

import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { TableStatusViewCache } from '~/app/shared/classes/table-status-view-cache';

@Component({
  selector: 'cd-mirroring-images',
  templateUrl: './image-list.component.html',
  styleUrls: ['./image-list.component.scss']
})
export class ImageListComponent implements OnInit, OnDestroy {
  @ViewChild('stateTmpl', { static: true })
  stateTmpl: TemplateRef<any>;
  @ViewChild('syncTmpl', { static: true })
  syncTmpl: TemplateRef<any>;
  @ViewChild('progressTmpl', { static: true })
  progressTmpl: TemplateRef<any>;
  @ViewChild('entriesBehindPrimaryTpl', { static: true })
  entriesBehindPrimaryTpl: TemplateRef<any>;

  subs: Subscription;

  image_error: Record<string, any> = {
    data: [],
    columns: {}
  };
  image_syncing: Record<string, any> = {
    data: [],
    columns: {}
  };
  image_ready: Record<string, any> = {
    data: [],
    columns: {}
  };

  tableStatus = new TableStatusViewCache();

  constructor(private rbdMirroringService: RbdMirroringService) {}

  ngOnInit() {
    this.image_error.columns = [
      { prop: 'pool_name', name: $localize`Pool`, flexGrow: 2 },
      { prop: 'name', name: $localize`Image`, flexGrow: 2 },
      {
        prop: 'state',
        name: $localize`State`,
        cellTemplate: this.stateTmpl,
        flexGrow: 1
      },
      { prop: 'description', name: $localize`Issue`, flexGrow: 4 }
    ];

    this.image_syncing.columns = [
      { prop: 'pool_name', name: $localize`Pool`, flexGrow: 2 },
      { prop: 'name', name: $localize`Image`, flexGrow: 2 },
      {
        prop: 'state',
        name: $localize`State`,
        cellTemplate: this.stateTmpl,
        flexGrow: 1
      },
      {
        prop: 'syncing_percent',
        name: $localize`Progress`,
        cellTemplate: this.progressTmpl,
        flexGrow: 2
      },
      { prop: 'bytes_per_second', name: $localize`Bytes per second`, flexGrow: 2 },
      {
        prop: 'entries_behind_primary',
        name: $localize`Entries behind primary`,
        cellTemplate: this.entriesBehindPrimaryTpl,
        flexGrow: 2
      }
    ];

    this.image_ready.columns = [
      { prop: 'pool_name', name: $localize`Pool`, flexGrow: 2 },
      { prop: 'name', name: $localize`Image`, flexGrow: 2 },
      {
        prop: 'state',
        name: $localize`State`,
        cellTemplate: this.stateTmpl,
        flexGrow: 1
      },
      { prop: 'description', name: $localize`Description`, flexGrow: 4 }
    ];

    this.subs = this.rbdMirroringService.subscribeSummary((data) => {
      this.image_error.data = data.content_data.image_error;
      this.image_syncing.data = data.content_data.image_syncing;
      this.image_ready.data = data.content_data.image_ready;
      this.tableStatus = new TableStatusViewCache(data.status);
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  refresh() {
    this.rbdMirroringService.refresh();
  }
}
