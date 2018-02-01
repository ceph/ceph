import { Component, OnInit } from '@angular/core';
import { TopLevelService } from '../../../shared/services/top-level.service';

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {
  topLevelData: any;

  constructor(topLevelService: TopLevelService) {
    topLevelService.topLevelData$.subscribe(data => {
      this.topLevelData = data;
    });
  }

  ngOnInit() {}
}
