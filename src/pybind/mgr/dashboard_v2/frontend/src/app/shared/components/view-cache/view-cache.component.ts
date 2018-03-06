import { Component, Input, OnInit } from '@angular/core';

import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';

@Component({
  selector: 'cd-view-cache',
  templateUrl: './view-cache.component.html',
  styleUrls: ['./view-cache.component.scss']
})
export class ViewCacheComponent implements OnInit {
  @Input() status: ViewCacheStatus;
  vcs = ViewCacheStatus;

  constructor() {}

  ngOnInit() {}
}
