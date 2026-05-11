import { Injectable, TemplateRef } from '@angular/core';

import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FormModalComponent } from '~/app/shared/components/form-modal/form-modal.component';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { HostService, HostModalRef } from '~/app/shared/api/host.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { Host } from '~/app/shared/models/host.interface';
import { HostStatus } from '~/app/shared/enum/host-status.enum';
import { FinishedTask } from '../models/finished-task';

interface HostLabelOption {
  content: string;
  selected: boolean;
}

@Injectable({
  providedIn: 'root'
})
export class HostActionService {
  predefinedLabels = ['mon', 'mgr', 'osd', 'mds', 'rgw', 'nfs', 'iscsi', 'rbd', 'grafana'];

  constructor(
    private hostService: HostService,
    private notificationService: NotificationService,
    private cdsModalService: ModalCdsService,
    private taskWrapper: TaskWrapperService
  ) {}

  /**
   * Opens the host edit modal and updates host labels after submit.
   */
  openEditModal(host: Host, onSuccess: (updatedLabels: string[]) => void): void {
    this.hostService.getLabels().subscribe((resp) => {
      const hostLabels = this.getHostLabels(host);
      const allLabels = this.buildHostLabelOptions(resp, hostLabels);
      const labelsField = this.createHostLabelsField(hostLabels, allLabels);

      this.cdsModalService.show(FormModalComponent, {
        titleText: $localize`Edit Host: ${host.hostname}`,
        fields: [labelsField],
        submitButtonText: $localize`Save changes`,
        onSubmit: (values: { labels: string[] }) => {
          this.submitHostLabelUpdate(host.hostname, values.labels, onSuccess);
        }
      });
    });
  }

  /**
   * Moves a host into or out of maintenance mode.
   */
  hostMaintenance(
    host: Host,
    maintenanceConfirmTpl: TemplateRef<any>,
    onExecutingChange: (executing: boolean) => void,
    onErrorMessage: (errorMessage: string[]) => void,
    onSuccess: () => void,
    onWarningSuccess: () => void = () => undefined,
    onWarningModalOpen: (modalRef: HostModalRef) => void = () => undefined
  ): void {
    onExecutingChange(true);

    if (this.isHostInMaintenance(host)) {
      this.exitHostMaintenance(host.hostname, onExecutingChange, onSuccess);
      return;
    }

    this.enterHostMaintenance(
      host.hostname,
      maintenanceConfirmTpl,
      onExecutingChange,
      onErrorMessage,
      onSuccess,
      onWarningSuccess,
      onWarningModalOpen
    );
  }

  /**
   * Starts or stops host drain and updates labels if required.
   */
  hostDrain(host: Host, stop: boolean, onSuccess: () => void): void {
    if (stop) {
      const labels = Array.isArray(host.labels) ? [...host.labels] : [];
      const index = labels.indexOf('_no_schedule', 0);
      if (index > -1) {
        labels.splice(index, 1);
      }

      this.hostService.update(host.hostname, true, labels).subscribe(() => {
        this.notificationService.show(
          NotificationType.info,
          $localize`"${host.hostname}" stopped draining`
        );
        onSuccess();
      });
      return;
    }

    this.hostService.update(host.hostname, false, [], false, false, true).subscribe(() => {
      this.notificationService.show(
        NotificationType.info,
        $localize`"${host.hostname}" started draining`
      );
      onSuccess();
    });
  }

  /**
   * Opens a delete-confirmation modal for removing a host.
   */
  deleteAction(hostname: string): HostModalRef {
    return this.cdsModalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.high,
      itemDescription: $localize`Host`,
      itemNames: [hostname],
      actionDescription: $localize`remove`,
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('host/remove', { hostname: hostname }),
          call: this.hostService.delete(hostname)
        })
    });
  }

  /**
   * Returns a cloned labels array from a host.
   */
  private getHostLabels(host: Host): string[] {
    return Array.isArray(host.labels) ? [...host.labels] : [];
  }

  /**
   * Builds selectable label options from server labels, predefined labels, and host labels.
   */
  private buildHostLabelOptions(labels: string[], hostLabels: string[]): HostLabelOption[] {
    const allLabels = new Set(labels.concat(this.predefinedLabels).concat(hostLabels));
    return Array.from(allLabels).map((label) => ({
      content: label,
      selected: hostLabels.includes(label)
    }));
  }

  /**
   * Creates the labels field config used by the edit-host form modal.
   */
  private createHostLabelsField(hostLabels: string[], allLabels: HostLabelOption[]) {
    return {
      type: 'select-badges',
      name: 'labels',
      value: hostLabels,
      label: $localize`Labels`,
      typeConfig: {
        customBadges: true,
        options: allLabels,
        messages: new SelectMessages({
          empty: $localize`There are no labels.`,
          filter: $localize`Filter or add labels`,
          add: $localize`Add label`
        })
      }
    };
  }

  /**
   * Submits host label changes and emits success callback/notification.
   */
  private submitHostLabelUpdate(
    hostname: string,
    labels: string[],
    onSuccess: (updatedLabels: string[]) => void
  ): void {
    this.hostService.update(hostname, true, labels).subscribe(() => {
      onSuccess(labels);
      this.notificationService.show(
        NotificationType.success,
        $localize`Updated Host "${hostname}"`
      );
    });
  }

  /**
   * Checks whether a host is currently in maintenance mode.
   */
  private isHostInMaintenance(host: Host): boolean {
    return host.status === HostStatus.MAINTENANCE.toLowerCase();
  }

  /**
   * Sends the request to enter maintenance and delegates success/error handling.
   */
  private enterHostMaintenance(
    hostname: string,
    maintenanceConfirmTpl: TemplateRef<any>,
    onExecutingChange: (executing: boolean) => void,
    onErrorMessage: (errorMessage: string[]) => void,
    onSuccess: () => void,
    onWarningSuccess: () => void,
    onWarningModalOpen: (modalRef: HostModalRef) => void
  ): void {
    this.hostService.update(hostname, false, [], true).subscribe(
      () => this.handleEnterMaintenanceSuccess(hostname, onExecutingChange, onSuccess),
      (error) =>
        this.handleEnterMaintenanceError(
          hostname,
          error,
          maintenanceConfirmTpl,
          onExecutingChange,
          onErrorMessage,
          onWarningSuccess,
          onWarningModalOpen
        )
    );
  }

  /**
   * Handles successful transition into maintenance.
   */
  private handleEnterMaintenanceSuccess(
    hostname: string,
    onExecutingChange: (executing: boolean) => void,
    onSuccess: () => void
  ): void {
    onExecutingChange(false);
    this.notificationService.show(
      NotificationType.success,
      $localize`"${hostname}" moved to maintenance`
    );
    onSuccess();
  }

  /**
   * Handles maintenance entry failures and optional warning confirmation flow.
   */
  private handleEnterMaintenanceError(
    hostname: string,
    error: any,
    maintenanceConfirmTpl: TemplateRef<any>,
    onExecutingChange: (executing: boolean) => void,
    onErrorMessage: (errorMessage: string[]) => void,
    onWarningSuccess: () => void,
    onWarningModalOpen: (modalRef: HostModalRef) => void
  ): void {
    onExecutingChange(false);

    const errorDetail = this.getHostMaintenanceErrorDetail(error);
    onErrorMessage(errorDetail.split(/\n/));

    if (typeof error?.preventDefault === 'function') {
      error.preventDefault();
    }

    if (this.shouldShowMaintenanceWarning(errorDetail)) {
      const modalRef = this.openHostMaintenanceWarningModal(
        hostname,
        maintenanceConfirmTpl,
        onWarningSuccess
      );
      onWarningModalOpen(modalRef);
      return;
    }

    this.notificationService.show(
      NotificationType.error,
      $localize`"${hostname}" cannot be put into maintenance`,
      $localize`${errorDetail}`
    );
  }

  /**
   * Extracts maintenance error detail from backend responses.
   */
  private getHostMaintenanceErrorDetail(error: any): string {
    return error?.error?.detail ?? '';
  }

  /**
   * Returns true when warning flow should offer a forced maintenance action.
   */
  private shouldShowMaintenanceWarning(errorDetail: string): boolean {
    return (
      errorDetail.includes('WARNING') &&
      !errorDetail.includes('It is NOT safe to stop') &&
      !errorDetail.includes('ALERT') &&
      !errorDetail.includes('unsafe to stop')
    );
  }

  /**
   * Opens warning confirmation modal and handles the forced maintenance request.
   */
  private openHostMaintenanceWarningModal(
    hostname: string,
    maintenanceConfirmTpl: TemplateRef<any>,
    onWarningSuccess: () => void
  ): HostModalRef {
    return this.cdsModalService.show(ConfirmationModalComponent, {
      titleText: $localize`Warning`,
      buttonText: $localize`Continue`,
      warning: true,
      bodyTpl: maintenanceConfirmTpl,
      showSubmit: true,
      onSubmit: () => this.forceHostMaintenance(hostname, onWarningSuccess)
    });
  }

  /**
   * Retries maintenance entry with force and closes any modal afterward.
   */
  private forceHostMaintenance(hostname: string, onWarningSuccess: () => void): void {
    this.hostService.update(hostname, false, [], true, true).subscribe(
      () => {
        this.cdsModalService.dismissAll();
        onWarningSuccess();
      },
      () => this.cdsModalService.dismissAll()
    );
  }

  /**
   * Exits maintenance mode and notifies the caller.
   */
  private exitHostMaintenance(
    hostname: string,
    onExecutingChange: (executing: boolean) => void,
    onSuccess: () => void
  ): void {
    this.hostService.update(hostname, false, [], true).subscribe(() => {
      onExecutingChange(false);
      this.notificationService.show(
        NotificationType.success,
        $localize`"${hostname}" has exited maintenance`
      );
      onSuccess();
    });
  }
}
