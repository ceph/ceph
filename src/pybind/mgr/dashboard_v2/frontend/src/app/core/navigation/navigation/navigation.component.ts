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
}
