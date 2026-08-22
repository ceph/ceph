import { Injectable, TemplateRef } from '@angular/core';
import { of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

import { CephfsAuthModalComponent } from '~/app/ceph/cephfs/cephfs-auth-modal/cephfs-auth-modal.component';
import { CephfsMountDetailsComponent } from '~/app/ceph/cephfs/cephfs-mount-details/cephfs-mount-details.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { HealthService } from '~/app/shared/api/health.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { CephfsDetail } from '~/app/shared/models/cephfs.model';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Injectable({
  providedIn: 'root'
})
export class CephfsActionService {
  constructor(
    private cephfsService: CephfsService,
    private configurationService: ConfigurationService,
    private healthService: HealthService,
    private modalService: ModalCdsService,
    private taskWrapper: TaskWrapperService
  ) {}

  getMonAllowPoolDelete() {
    return this.configurationService.get('mon_allow_pool_delete').pipe(
      map((data: any) => {
        const monSection = (data?.value || []).find((value: any) => value.section === 'mon');
        return monSection?.value === 'true';
      }),
      catchError(() => of(false))
    );
  }

  getDeleteDisableDesc(hasSelection: boolean, monAllowPoolDelete: boolean): boolean | string {
    if (hasSelection) {
      if (!monAllowPoolDelete) {
        return $localize`File System deletion is disabled by the mon_allow_pool_delete configuration setting.`;
      }

      return false;
    }

    return true;
  }

  showAttachInfo(selectedFileSystem: CephfsDetail | undefined | null): void {
    if (!selectedFileSystem?.id) {
      return;
    }

    this.cephfsService
      .getFsRootDirectory(String(selectedFileSystem.id))
      .pipe(
        switchMap((fsData) =>
          this.healthService.getClusterFsid().pipe(map((data) => ({ clusterId: data, fs: fsData })))
        )
      )
      .subscribe({
        next: (val) => {
          let modalRef: any;
          modalRef = this.modalService.show(CephfsMountDetailsComponent, {
            onSubmit: () => modalRef?.close?.(),
            mountData: {
              clusterFSID: val.clusterId,
              fsName: selectedFileSystem?.mdsmap?.fs_name,
              path: val.fs['path']
            }
          });
        }
      });
  }

  removeVolume(volName: string, bodyTemplate?: TemplateRef<any>): void {
    if (!volName) {
      return;
    }

    this.modalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.high,
      itemDescription: 'File System',
      itemNames: [volName],
      actionDescription: 'remove',
      bodyTemplate,
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/remove', { volumeName: volName }),
          call: this.cephfsService.remove(volName)
        })
    });
  }

  authorize(selectedFileSystem: CephfsDetail | undefined | null): void {
    if (!selectedFileSystem?.mdsmap?.fs_name || !selectedFileSystem?.id) {
      return;
    }

    this.modalService.show(CephfsAuthModalComponent, {
      fsName: selectedFileSystem.mdsmap['fs_name'],
      id: selectedFileSystem.id
    });
  }
}
