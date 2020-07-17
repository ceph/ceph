import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-nfs-501',
  templateUrl: './nfs-501.component.html',
  styleUrls: ['./nfs-501.component.scss']
})
export class Nfs501Component implements OnInit, OnDestroy {
  docsUrl: string;
  message = $localize`The NFS Ganesha service is not configured.`;
  routeParamsSubscribe: any;

  constructor(
    private route: ActivatedRoute,
    private summaryService: SummaryService,
    private cephReleaseNamePipe: CephReleaseNamePipe
  ) {}

  ngOnInit() {
    this.summaryService.subscribeOnce((summary) => {
      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl =
        `http://docs.ceph.com/docs/${releaseName}/mgr/dashboard/` +
        `#configuring-nfs-ganesha-in-the-dashboard`;
    });

    this.routeParamsSubscribe = this.route.params.subscribe((params: { message: string }) => {
      this.message = params.message;
    });
  }

  ngOnDestroy() {
    this.routeParamsSubscribe.unsubscribe();
  }
}
