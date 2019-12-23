import { Component, OnInit } from '@angular/core';

import { CephReleaseNamePipe } from '../../pipes/ceph-release-name.pipe';
import { SummaryService } from '../../services/summary.service';

@Component({
  selector: 'cd-orchestrator-doc-panel',
  templateUrl: './orchestrator-doc-panel.component.html',
  styleUrls: ['./orchestrator-doc-panel.component.scss']
})
export class OrchestratorDocPanelComponent implements OnInit {
  docsUrl: string;

  constructor(
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private summaryService: SummaryService
  ) {}

  ngOnInit() {
    const subs = this.summaryService.subscribe((summary: any) => {
      if (!summary) {
        return;
      }

      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl = `http://docs.ceph.com/docs/${releaseName}/mgr/orchestrator_cli/`;

      setTimeout(() => {
        subs.unsubscribe();
      }, 0);
    });
  }
}
