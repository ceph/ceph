import { Component, Input } from '@angular/core';
import { Icons } from '~/app/shared/enum/icons.enum';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permissions } from '~/app/shared/models/permissions';

@Component({
  selector: 'cd-health-checks',
  templateUrl: './health-checks.component.html',
  styleUrls: ['./health-checks.component.scss']
})
export class HealthChecksComponent {
  @Input()
  healthData: any;

  icons = Icons;

  permissions: Permissions;

  constructor(private authStorageService: AuthStorageService) {
    this.permissions = this.authStorageService.getPermissions();
  }

  getFailedDaemons(detail: any[]): string[] {
    return detail.map(
      (failedDaemons) => failedDaemons.message.split('daemon ')?.[1].split(' on ')[0]
    );
  }
}
