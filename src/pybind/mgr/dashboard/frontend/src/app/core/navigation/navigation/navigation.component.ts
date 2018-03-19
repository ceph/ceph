import { Component, OnInit } from '@angular/core';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {
  summaryData: any;
  rbdPools: Array<any> = [];

  constructor(private summaryService: SummaryService) {}

  ngOnInit() {
    this.summaryService.summaryData$.subscribe((data: any) => {
      this.summaryData = data;
      this.rbdPools = data.rbd_pools;
    });
  }

  blockHealthColor() {
    if (this.summaryData && this.summaryData.rbd_mirroring) {
      if (this.summaryData.rbd_mirroring.errors > 0) {
        return { color: '#d9534f' };
      } else if (this.summaryData.rbd_mirroring.warnings > 0) {
        return { color: '#f0ad4e' };
      }
    }
  }
}
