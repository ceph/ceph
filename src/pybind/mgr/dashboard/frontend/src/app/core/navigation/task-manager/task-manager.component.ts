import { Component, OnInit } from '@angular/core';

import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskManagerMessageService } from '../../../shared/services/task-manager-message.service';

@Component({
  selector: 'cd-task-manager',
  templateUrl: './task-manager.component.html',
  styleUrls: ['./task-manager.component.scss']
})
export class TaskManagerComponent implements OnInit {
  executingTasks: ExecutingTask[] = [];
  finishedTasks: FinishedTask[] = [];

  icon = 'fa-hourglass-o';

  constructor(
    private summaryService: SummaryService,
    private taskMessageManager: TaskManagerMessageService
  ) {}

  ngOnInit() {
    this.summaryService.subscribe((data: any) => {
      if (!data) {
        return;
      }
      this._handleTasks(data.executing_tasks, data.finished_tasks);
      this._setIcon(data.executing_tasks.length);
    });
  }

  _handleTasks(executingTasks: ExecutingTask[], finishedTasks: FinishedTask[]) {
    for (const excutingTask of executingTasks) {
      excutingTask.description = this.taskMessageManager.getRunningMessage(excutingTask);
    }
    for (const finishedTask of finishedTasks) {
      finishedTask.description = this.taskMessageManager.getDescription(finishedTask);
      if (finishedTask.success === false) {
        finishedTask.errorMessage = this.taskMessageManager.getErrorMessage(finishedTask);
      }
    }
    this.executingTasks = executingTasks;
    this.finishedTasks = finishedTasks;
  }

  _setIcon(executingTasks: number) {
    const iconSuffix = ['o', 'start', 'half', 'end']; // TODO: Use all suffixes
    const iconIndex = executingTasks > 0 ? 1 : 0;
    this.icon = 'fa-hourglass-' + iconSuffix[iconIndex];
  }
}
