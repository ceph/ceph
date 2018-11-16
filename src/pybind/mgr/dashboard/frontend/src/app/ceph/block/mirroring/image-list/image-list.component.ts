import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { Subscription } from 'rxjs';

import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';

@Component({
  selector: 'cd-mirroring-images',
  templateUrl: './image-list.component.html',
  styleUrls: ['./image-list.component.scss']
})
export class ImageListComponent implements OnInit, OnDestroy {
  @ViewChild('stateTmpl')
  stateTmpl: TemplateRef<any>;
  @ViewChild('syncTmpl')
  syncTmpl: TemplateRef<any>;
  @ViewChild('progressTmpl')
  progressTmpl: TemplateRef<any>;

  subs: Subscription;

  image_error = {
    data: [],
    columns: {}
  };
  image_syncing = {
    data: [],
    columns: {}
  };
  image_ready = {
    data: [],
    columns: {}
  };

  constructor(private rbdMirroringService: RbdMirroringService, private i18n: I18n) {}

  ngOnInit() {
    this.image_error.columns = [
      { prop: 'pool_name', name: this.i18n('Pool'), flexGrow: 2 },
      { prop: 'name', name: this.i18n('Image'), flexGrow: 2 },
      { prop: 'description', name: this.i18n('Issue'), flexGrow: 4 },
      {
        prop: 'state',
        name: this.i18n('State'),
        cellTemplate: this.stateTmpl,
        flexGrow: 1
      }
    ];

    this.image_syncing.columns = [
      { prop: 'pool_name', name: this.i18n('Pool'), flexGrow: 2 },
      { prop: 'name', name: this.i18n('Image'), flexGrow: 2 },
      {
        prop: 'progress',
        name: this.i18n('Progress'),
        cellTemplate: this.progressTmpl,
        flexGrow: 2
      },
      {
        prop: 'state',
        name: this.i18n('State'),
        cellTemplate: this.syncTmpl,
        flexGrow: 1
      }
    ];

    this.image_ready.columns = [
      { prop: 'pool_name', name: this.i18n('Pool'), flexGrow: 2 },
      { prop: 'name', name: this.i18n('Image'), flexGrow: 2 },
      { prop: 'description', name: this.i18n('Description'), flexGrow: 4 },
      {
        prop: 'state',
        name: this.i18n('State'),
        cellTemplate: this.stateTmpl,
        flexGrow: 1
      }
    ];

    this.subs = this.rbdMirroringService.subscribeSummary((data: any) => {
      if (!data) {
        return;
      }
      this.image_error.data = data.content_data.image_error;
      this.image_syncing.data = data.content_data.image_syncing;
      this.image_ready.data = data.content_data.image_ready;
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  refresh() {
    this.rbdMirroringService.refresh();
  }
}
