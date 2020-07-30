import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';

@Component({
  selector: 'cd-nfs-501',
  templateUrl: './nfs-501.component.html',
  styleUrls: ['./nfs-501.component.scss']
})
export class Nfs501Component implements OnInit, OnDestroy {
  message = this.i18n('The NFS Ganesha service is not configured.');
  routeParamsSubscribe: any;

  constructor(private route: ActivatedRoute, private i18n: I18n) {}

  ngOnInit() {
    this.routeParamsSubscribe = this.route.params.subscribe((params: { message: string }) => {
      this.message = params.message;
    });
  }

  ngOnDestroy() {
    this.routeParamsSubscribe.unsubscribe();
  }
}
