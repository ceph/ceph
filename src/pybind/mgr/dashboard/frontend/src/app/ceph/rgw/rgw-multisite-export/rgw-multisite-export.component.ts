import { AfterViewChecked, ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RgwRealm } from '../models/rgw-multisite';
import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-rgw-multisite-export',
  templateUrl: './rgw-multisite-export.component.html',
  styleUrls: ['./rgw-multisite-export.component.scss']
})
export class RgwMultisiteExportComponent implements OnInit, AfterViewChecked {
  exportTokenForm: CdFormGroup;
  realms: any;
  realmList: RgwRealm[];
  multisiteInfo: any;
  tokenValid = false;
  loading = true;
  icons = Icons;

  constructor(
    public activeModal: NgbActiveModal,
    public rgwRealmService: RgwRealmService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private readonly changeDetectorRef: ChangeDetectorRef
  ) {
    this.createForm();
  }

  createForm() {
    this.exportTokenForm = new CdFormGroup({});
  }

  onSubmit() {
    this.activeModal.close();
  }

  ngOnInit(): void {
    this.rgwRealmService.getRealmTokens().subscribe((data: object[]) => {
      this.loading = false;
      this.realms = data;
      var base64Matcher = new RegExp(
        '^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})$'
      );
      this.realms.forEach((realmInfo: any) => {
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
