import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  HostBinding,
  OnInit
} from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { distinctUntilChanged, filter, map, pluck } from 'rxjs/operators';

import { Icons } from '../../../shared/enum/icons.enum';
import { Permissions } from '../../../shared/models/permissions';
import { HealthColorPipe } from '../../../shared/pipes/health-color.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { IMenuItem } from '../menu/menu.component';

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class NavigationComponent implements OnInit {
  @HostBinding('class.isPwdDisplayed') isPwdDisplayed = false;

  permissions: Permissions;
  isCollapsed = true;
  showMenuSidebar = true;

  menuItems: IMenuItem[] = [
    {
      label: this.i18n('Dashboard'),
      link: '/dashboard',
      badges: [
        {
          onChange: this.summaryService.summaryData$.pipe(
            filter(Boolean),
            pluck('health_status'),
            distinctUntilChanged(),
            map((s) => ({ class: Icons.health, style: this.healthColorPipe.transform(s) }))
          )
        }
      ]
    },
    {
      label: this.i18n('Cluster'),
      children: [
        { label: this.i18n('Hosts'), link: '/hosts', permissions: 'hosts' },
        { label: this.i18n('Inventory'), link: '/inventory', permissions: 'hosts' },
        { label: this.i18n('Services'), link: '/services', permissions: 'hosts' },
        { label: this.i18n('Monitors'), link: '/monitor', permissions: 'monitor' },
        { label: this.i18n('OSDs'), link: '/osd', permissions: 'osd' },
        { label: this.i18n('CRUSH map'), link: '/crush-map', permissions: 'osd' },
        { label: this.i18n('Configuration'), link: '/configuration', permissions: 'configOpt' },
        { label: this.i18n('Manager modules'), link: '/mgr-modules', permissions: 'configOpt' },
        { label: this.i18n('Monitoring'), link: '/monitoring', permissions: 'prometheus' },
        { label: this.i18n('Logs'), link: '/logs', permissions: 'log' }
      ]
    },
    { label: this.i18n('Pools'), link: '/pool', permissions: 'pool' },
    {
      label: this.i18n('Block'),
      children: [
        { label: this.i18n('Images'), link: '/block/rbd', permissions: 'rbdImage' },
        { label: this.i18n('Mirroring'), link: '/block/mirroring', permissions: 'rbdMirroring' },
        {
          label: this.i18n('iSCSI'),
          link: '/block/iscsi',
          permissions: 'iscsi',
          badges: [
            {
              onChange: this.summaryService.summaryData$.pipe(
                map((s) => _.get(s, 'rbd_mirroring.warnings')),
                distinctUntilChanged(),
                map((s) => ({ class: 'badge badge-warning', label: s, hidden: !s }))
              )
            },
            {
              onChange: this.summaryService.summaryData$.pipe(
                map((s) => _.get(s, 'rbd_mirroring.errors')),
                distinctUntilChanged(),
                map((s) => ({ class: 'badge badge-danger', label: s, hidden: !s }))
              )
            }
          ]
        }
      ]
    },
    { label: this.i18n('NFS'), link: '/nfs', permissions: 'nfs' },
    { label: this.i18n('Filesystems'), link: '/cephfs', permissions: 'cephfs' },
    {
      label: this.i18n('Object Gateway'),
      permissions: 'rgw',
      children: [
        { label: this.i18n('Daemons'), link: '/rgw/daemon' },
        { label: this.i18n('Users'), link: '/rgw/user' },
        { label: this.i18n('Buckets'), link: '/rgw/bucket' }
      ]
    }
  ];

  constructor(
    private authStorageService: AuthStorageService,
    private summaryService: SummaryService,
    private i18n: I18n,
    private healthColorPipe: HealthColorPipe,
    private cdr: ChangeDetectorRef
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.authStorageService.isPwdDisplayed$.subscribe((isDisplayed) => {
      this.isPwdDisplayed = isDisplayed;
      this.cdr.markForCheck();
    });
  }
}
