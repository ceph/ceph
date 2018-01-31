import { Component, OnInit } from '@angular/core';
import { TopLevelService } from '../../../shared/services/top-level.service';

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {
  topLevelData: any;
  rbdPools: Array<any> = [];

  constructor(private topLevelService: TopLevelService) {}

  ngOnInit() {
    this.topLevelService.topLevelData$.subscribe((data: any) => {
      this.topLevelData = data;
      this.rbdPools = data.rbd_pools;
    });
  }
}
