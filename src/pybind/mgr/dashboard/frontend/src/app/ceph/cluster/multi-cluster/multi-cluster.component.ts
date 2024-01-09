import { Component, OnInit, ViewChild } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { Subscription } from 'rxjs';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalService } from '~/app/shared/services/modal.service';
import { MultiClusterFormComponent } from './multi-cluster-form/multi-cluster-form.component';

@Component({
  selector: 'cd-multi-cluster',
  templateUrl: './multi-cluster.component.html',
  styleUrls: ['./multi-cluster.component.scss']
})
export class MultiClusterComponent implements OnInit {
  @ViewChild('nameTpl', { static: true })
  nameTpl: any;

  private subs = new Subscription();
  dashboardClustersMap: Map<string, string> = new Map<string, string>();
  icons = Icons;
  loading = true;
  bsModalRef: NgbModalRef;

  constructor(
    private multiClusterService: MultiClusterService,
    private modalService: ModalService
  ) {}

  ngOnInit(): void {
    this.subs.add(
      this.multiClusterService.subscribe((resp: any) => {
        const clustersConfig = resp['config'];
        if (clustersConfig) {
          Object.keys(clustersConfig).forEach((clusterKey: string) => {
            const clusterDetailsList = clustersConfig[clusterKey];

            clusterDetailsList.forEach((clusterDetails: any) => {
              const clusterUrl = clusterDetails['url'];
              const clusterName = clusterDetails['name'];
              this.dashboardClustersMap.set(clusterUrl, clusterName);
            });
          });

          if (this.dashboardClustersMap.size >= 1) {
            this.loading = false;
          }
        }
      })
    );
  }

  openRemoteClusterInfoModal() {
    this.bsModalRef = this.modalService.show(MultiClusterFormComponent, {
      size: 'xl'
    });
  }
}
