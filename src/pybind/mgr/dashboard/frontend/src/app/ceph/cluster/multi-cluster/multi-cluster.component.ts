import { Component, Input } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { MultiClusterFormComponent } from './multi-cluster-form/multi-cluster-form.component';

@Component({
  selector: 'cd-multi-cluster',
  templateUrl: './multi-cluster.component.html',
  styleUrls: ['./multi-cluster.component.scss']
})
export class MultiClusterComponent {
  @Input() selectedValue: string;
  permission: Permission;
  selection = new CdTableSelection();
  bsModalRef: NgbModalRef;

  addRemoteClusterAction: CdTableAction[];
  capacity: any = {};

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    public modalService: ModalService // public multiClusterService: MulticlusterService
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit() {
    const addRemoteAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.ADD + ' Cluster',
      click: () => this.openRemoteClusterInfoModal()
    };
    this.addRemoteClusterAction = [addRemoteAction];
  }

  openRemoteClusterInfoModal() {
    this.bsModalRef = this.modalService.show(MultiClusterFormComponent, {
      size: 'lg'
    });
  }
}
