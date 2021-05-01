import { Component } from '@angular/core';

import { ClusterService } from '~/app/shared/api/cluster.service';
import { AppConstants } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-create-cluster',
  templateUrl: './create-cluster.component.html',
  styleUrls: ['./create-cluster.component.scss']
})
export class CreateClusterComponent {
  permission: Permission;
  orchStatus = false;
  featureAvailable = false;
  projectConstants: typeof AppConstants = AppConstants;

  constructor(
    private authStorageService: AuthStorageService,
    private clusterService: ClusterService,
    private notificationService: NotificationService
  ) {
    this.permission = this.authStorageService.getPermissions().configOpt;
  }

  createCluster() {
    this.notificationService.show(
      NotificationType.error,
      $localize`Cluster creation feature not implemented`
    );
  }

  skipClusterCreation() {
    this.clusterService.updateStatus('POST_INSTALLED').subscribe(() => {
      this.notificationService.show(
        NotificationType.info,
        $localize`Cluster creation skipped by user`
      );
    });
  }
}
