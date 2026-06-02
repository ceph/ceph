import { Injectable, OnDestroy } from '@angular/core';

import { merge, Subject, Subscription } from 'rxjs';

import { FinishedTask } from '~/app/shared/models/finished-task';
import { Summary } from '~/app/shared/models/summary.model';
import { SummaryService } from '~/app/shared/services/summary.service';

/**
 * Provides a unified refresh$ stream for NVMe-oF UI components to react to
 * task completions and explicit refresh requests.
 *
 * A dedicated service is used instead of reusing TaskListService because
 * TaskListService is tightly coupled to the task-list UI component and does
 * not expose a clean observable suitable for driving side-effects such as
 * setup-card state reloads.
 *
 * This service is provided at the NvmeofTabsComponent level to ensure
 * subscriptions are properly destroyed when navigating away from nvmeof pages.
 */
@Injectable()
export class NvmeofStateService implements OnDestroy {
  private readonly explicitRefreshSource = new Subject<void>();
  private readonly taskRefreshSource = new Subject<void>();
  private summarySubscription: Subscription;
  private seenTaskSignatures = new Set<string>();
  private summaryInitialized = false;

  readonly refresh$ = merge(
    this.explicitRefreshSource.asObservable(),
    this.taskRefreshSource.asObservable()
  );

  constructor(private summaryService: SummaryService) {
    this.summarySubscription = this.summaryService.subscribe((summary: Summary) => {
      const nvmeTasks = (summary?.finished_tasks ?? []).filter((task: FinishedTask) =>
        this.isNvmeTask(task)
      );
      const currentSignatures = new Set(
        nvmeTasks.map((task: FinishedTask) => this.getTaskSignature(task))
      );

      if (!this.summaryInitialized) {
        this.summaryInitialized = true;
        this.seenTaskSignatures = currentSignatures;
        return;
      }

      const hasNewTask = [...currentSignatures].some(
        (taskSignature) => !this.seenTaskSignatures.has(taskSignature)
      );
      this.seenTaskSignatures = currentSignatures;

      if (hasNewTask) {
        this.taskRefreshSource.next();
      }
    });
  }

  ngOnDestroy(): void {
    this.summarySubscription?.unsubscribe();
  }

  requestRefresh(): void {
    this.explicitRefreshSource.next();
  }

  private isNvmeTask(task: FinishedTask): boolean {
    if (!task?.name) {
      return false;
    }

    if (task.name === 'service/create' || task.name === 'service/delete') {
      const serviceName = task.metadata?.['service_name'];
      return typeof serviceName === 'string' && serviceName.startsWith('nvmeof.');
    }

    return [
      'nvmeof/gateway/create',
      'nvmeof/gateway/delete',
      'nvmeof/subsystem/create',
      'nvmeof/subsystem/delete',
      'nvmeof/namespace/create',
      'nvmeof/namespace/delete'
    ].includes(task.name);
  }

  private getTaskSignature(task: FinishedTask): string {
    return JSON.stringify({
      name: task.name,
      begin_time: task.begin_time,
      metadata: task.metadata
    });
  }
}
