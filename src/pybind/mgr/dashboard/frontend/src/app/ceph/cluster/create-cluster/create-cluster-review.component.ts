import { Component, OnInit } from '@angular/core';

import _ from 'lodash';

import { HostService } from '~/app/shared/api/host.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';

@Component({
  selector: 'cd-create-cluster-review',
  templateUrl: './create-cluster-review.component.html',
  styleUrls: ['./create-cluster-review.component.scss']
})
export class CreateClusterReviewComponent implements OnInit {
  hosts: object[] = [];
  hostsDetails: object;
  hostsByLabel: object;
  hostsCount: number;
  labelOccurrences = {};
  hostsCountPerLabel: object[] = [];
  uniqueLabels: Set<string> = new Set();

  constructor(private hostService: HostService) {}

  ngOnInit() {
    this.hostsDetails = {
      columns: [
        {
          prop: 'hostname',
          name: $localize`Host Name`,
          flexGrow: 2
        },
        {
          name: $localize`Labels`,
          prop: 'labels',
          flexGrow: 1,
          cellTransformation: CellTemplate.badge,
          customTemplateConfig: {
            class: 'badge-dark'
          }
        }
      ]
    };

    this.hostsByLabel = {
      columns: [
        {
          prop: 'label',
          name: $localize`Labels`,
          flexGrow: 1,
          cellTransformation: CellTemplate.badge,
          customTemplateConfig: {
            class: 'badge-dark'
          }
        },
        {
          name: $localize`Number of Hosts`,
          prop: 'hosts_per_label',
          flexGrow: 1
        }
      ]
    };

    this.hostService.list().subscribe((resp: object[]) => {
      this.hosts = resp;
      this.hostsCount = this.hosts.length;

      _.forEach(this.hosts, (hostKey) => {
        const labels = hostKey['labels'];
        _.forEach(labels, (label) => {
          this.labelOccurrences[label] = (this.labelOccurrences[label] || 0) + 1;
          this.uniqueLabels.add(label);
        });
      });

      this.uniqueLabels.forEach((label) => {
        this.hostsCountPerLabel.push({
          label: label,
          hosts_per_label: this.labelOccurrences[label]
        });
      });

      this.hostsByLabel['data'] = [...this.hostsCountPerLabel];
      this.hostsDetails['data'] = [...this.hosts];
    });
  }
}
