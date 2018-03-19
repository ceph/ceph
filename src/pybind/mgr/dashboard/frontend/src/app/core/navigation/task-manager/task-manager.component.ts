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

  executingTasks: Array<ExecutingTask> = [];
  finishedTasks: Array<FinishedTask> = [];

  icon = 'fa-hourglass-o';

  constructor(private summaryService: SummaryService,
              private taskManagerMessageService: TaskManagerMessageService) {
  }

  ngOnInit() {
    const icons = ['fa-hourglass-o', 'fa-hourglass-start', 'fa-hourglass-half', 'fa-hourglass-end'];
    let iconIndex = 0;
    this.summaryService.summaryData$.subscribe((data: any) => {
      this.executingTasks = data.executing_tasks;
      this.finishedTasks = data.finished_tasks;
      for (const excutingTask of this.executingTasks) {
        excutingTask.description = this.taskManagerMessageService.getDescription(excutingTask);
      }
      for (const finishedTask of this.finishedTasks) {
        finishedTask.description = this.taskManagerMessageService.getDescription(finishedTask);
        if (finishedTask.success === false) {
          finishedTask.errorMessage = this.taskManagerMessageService.getErrorMessage(finishedTask);
        }
      }
      if (this.executingTasks.length > 0) {
        iconIndex = (iconIndex + 1) % icons.length;
      } else {
        iconIndex = 0;
      }
      this.icon = icons[iconIndex];
    });
  }

}
