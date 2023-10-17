import { Component, Input, OnInit } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { MulticlusterFormComponent } from '../multicluster-form/multicluster-form.component';
import { MulticlusterService } from '~/app/shared/api/multicluster.service';

@Component({
  selector: 'cd-multicluster-dashboard',
  templateUrl: './multicluster-dashboard.component.html',
  styleUrls: ['./multicluster-dashboard.component.scss']
})
export class MulticlusterDashboardComponent implements OnInit{
  @Input() selectedValue: string;
  permission: Permission;
  selection = new CdTableSelection();
  bsModalRef: NgbModalRef;

  addRemoteClusterAction: CdTableAction[];
  capacity: any = {};

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    public modalService: ModalService,
    public multiClusterService: MulticlusterService
  ) { 
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit() {
    const addRemoteAction: CdTableAction = {
      permission: 'read',
      icon: Icons.download,
      name: this.actionLabels.ADD + ' Remote Cluster',
      click: () => this.openRemoteClusterInfoModal()
    };
    this.addRemoteClusterAction = [addRemoteAction];
  }

  openRemoteClusterInfoModal() {
    this.bsModalRef = this.modalService.show(MulticlusterFormComponent, {
      size: 'lg'
    });
  }
}
