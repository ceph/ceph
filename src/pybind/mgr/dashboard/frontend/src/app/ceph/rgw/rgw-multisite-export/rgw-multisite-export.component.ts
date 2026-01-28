import { AfterViewChecked, ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealmToken } from '../models/rgw-multisite';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';

@Component({
  selector: 'cd-rgw-multisite-export',
  templateUrl: './rgw-multisite-export.component.html',
  styleUrls: ['./rgw-multisite-export.component.scss']
})
export class RgwMultisiteExportComponent extends CdForm implements OnInit, AfterViewChecked {
  exportTokenForm: CdFormGroup;
  realms: RgwRealmToken[];
  tokenValid = false;
  icons = Icons;

  constructor(
    public activeModal: NgbActiveModal,
    public rgwRealmService: RgwRealmService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private readonly changeDetectorRef: ChangeDetectorRef
  ) {
    super();
    this.createForm();
  }

  createForm() {
    this.exportTokenForm = new CdFormGroup({});
  }

  onSubmit() {
    this.closeModal();
  }

  ngOnInit(): void {
    this.rgwRealmService.getRealmTokens().subscribe((data: RgwRealmToken[]) => {
      this.loadingReady();
      this.realms = data;
      var base64Matcher = new RegExp(
        '^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})$'
      );
      this.realms.forEach((realmInfo: RgwRealmToken) => {
        if (base64Matcher.test(realmInfo.token)) {
          this.tokenValid = true;
        } else {
          this.tokenValid = false;
        }
      });
    });
  }

  ngAfterViewChecked(): void {
    this.changeDetectorRef.detectChanges();
  }
}
