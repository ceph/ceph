import { Component, OnDestroy, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { Subscription } from 'rxjs';

import { SummaryService } from '../../../shared/services/summary.service';
import { TaskManagerService } from '../../../shared/services/task-manager.service';

@Component({
  selector: 'cd-workbench-layout',
  templateUrl: './workbench-layout.component.html',
  styleUrls: ['./workbench-layout.component.scss']
})
export class WorkbenchLayoutComponent implements OnInit, OnDestroy {
  private subs = new Subscription();

  constructor(
    private router: Router,
    private summaryService: SummaryService,
    private taskManagerService: TaskManagerService
  ) {}

  ngOnInit() {
    this.subs.add(this.summaryService.startPolling());
    this.subs.add(this.taskManagerService.init(this.summaryService));
  }

  ngOnDestroy() {
    this.subs.unsubscribe();
  }

  isDashboardPage() {
    return this.router.url === '/dashboard';
  }
}
