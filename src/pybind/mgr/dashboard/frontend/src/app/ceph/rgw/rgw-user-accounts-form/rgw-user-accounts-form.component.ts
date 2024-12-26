import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { Account } from '../models/rgw-user-accounts';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdValidators } from '~/app/shared/forms/cd-validators';

@Component({
  selector: 'cd-rgw-user-accounts-form',
  templateUrl: './rgw-user-accounts-form.component.html',
  styleUrls: ['./rgw-user-accounts-form.component.scss']
})
export class RgwUserAccountsFormComponent extends CdForm implements OnInit {
  open: boolean = false;
  accountForm: CdFormGroup;
  action: string;
  resource: string;
  status: string;
  allLabels: string[];
  editing: boolean = false;
  accountData: Account;

  constructor(
    private router: Router,
    private actionLabels: ActionLabelsI18n,
    private route: ActivatedRoute,
    private rgwUserAccountsService: RgwUserAccountsService,
    private notificationService: NotificationService
  ) {
    super();
    this.editing = this.router.url.includes('(modal:edit');
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = $localize`Account`;
    this.createForm();
    this.loadingReady();
  }

  ngOnInit(): void {
    this.open = this.route.outlet === 'modal';
    if (this.editing) {
      this.route.paramMap.subscribe((params: any) => {
        const account_id = params.get('accountId');
        this.rgwUserAccountsService.get(account_id).subscribe((accountData: Account) => {
          this.accountData = accountData;
        });
      });
    }
  }

  private createForm() {
    this.accountForm = new CdFormGroup({
      account_id: new UntypedFormControl('', {
        validators: [Validators.pattern(/^RGW\d{17}$/)]
      }),
      account_name: new UntypedFormControl(''),
      email: new UntypedFormControl('', {
        validators: [CdValidators.email]
      })
    });
  }

  submit() {
    if (this.accountForm.invalid) {
      return;
    }

    if (this.accountForm.pending) {
      this.accountForm.setErrors({ cdSubmitButton: true });
      return;
    }

    if (!this.editing) {
      this.rgwUserAccountsService.create(this.accountForm.value).subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Account ${this.accountForm.getValue('account_name')} created successfully`
          );
          this.closeModal();
        },
        () => {
          this.accountForm.setErrors({ cdSubmitButton: true });
        }
      );
    }
  }

  closeModal(): void {
    this.router.navigate(['rgw/accounts', { outlets: { modal: null } }]);
  }
}
