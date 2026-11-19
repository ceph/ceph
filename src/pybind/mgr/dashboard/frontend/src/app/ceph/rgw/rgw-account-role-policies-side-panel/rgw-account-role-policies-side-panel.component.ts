import {
  ChangeDetectionStrategy,
  Component,
  computed,
  DestroyRef,
  inject,
  input,
  output,
  signal,
  ViewEncapsulation
} from '@angular/core';
import { takeUntilDestroyed, toObservable } from '@angular/core/rxjs-interop';
import { Validators } from '@angular/forms';
import { combineLatest, Observable, of, Subscriber } from 'rxjs';
import { catchError, distinctUntilChanged, map, startWith, switchMap } from 'rxjs/operators';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

export interface PolicyPanelState {
  loading: boolean;
  document: string;
  policyName: string;
}

@Component({
  selector: 'cd-rgw-account-role-policies-side-panel',
  templateUrl: './rgw-account-role-policies-side-panel.component.html',
  styleUrls: ['./rgw-account-role-policies-side-panel.component.scss'],
  encapsulation: ViewEncapsulation.None,
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: false
})
export class RgwAccountRolePoliciesSidePanelComponent {
  readonly expanded = input(false);
  readonly accountId = input('');
  readonly roleName = input('');
  readonly policyName = input('');

  readonly closed = output<void>();
  readonly changed = output<void>();

  icons = Icons;
  form: CdFormGroup;

  readonly isEditing = signal(false);
  readonly isSubmitLoading = signal(false);
  private readonly reloadToken = signal(0);

  readonly panelHeaderText = computed(() => {
    const name = this.policyName();
    if (!name) {
      return '';
    }
    return this.isEditing() ? $localize`Edit ${name}` : name;
  });

  readonly panelHeaderDescription = computed(() => {
    const role = this.roleName();
    return role ? $localize`Permission policy - ${role}` : '';
  });

  private readonly destroyRef = inject(DestroyRef);

  readonly policyState$: Observable<PolicyPanelState> = combineLatest([
    toObservable(this.expanded),
    toObservable(this.accountId),
    toObservable(this.roleName),
    toObservable(this.policyName),
    toObservable(this.reloadToken)
  ]).pipe(
    distinctUntilChanged(
      (prev, curr) =>
        prev[0] === curr[0] &&
        prev[1] === curr[1] &&
        prev[2] === curr[2] &&
        prev[3] === curr[3] &&
        prev[4] === curr[4]
    ),
    switchMap(([expanded, accountId, roleName, policyName]) => {
      if (!expanded || !accountId || !roleName || !policyName) {
        this.isEditing.set(false);
        this.isSubmitLoading.set(false);
        return of({ loading: false, document: '', policyName: '' });
      }
      this.isEditing.set(false);
      return this.rgwRoleService.getPolicy(roleName, policyName, accountId).pipe(
        map((document) => ({ loading: false, document, policyName })),
        startWith({ loading: true, document: '', policyName }),
        catchError(() => of({ loading: false, document: '', policyName }))
      );
    }),
    takeUntilDestroyed(this.destroyRef)
  );

  constructor(
    private formBuilder: CdFormBuilder,
    private rgwRoleService: RgwRoleService,
    private modalService: ModalCdsService,
    private notificationService: NotificationService,
    public actionLabels: ActionLabelsI18n
  ) {
    this.form = this.formBuilder.group({
      policy_name: [{ value: '', disabled: true }, [Validators.required]],
      policy_doc: ['', [Validators.required, CdValidators.json()]]
    });
  }

  close(): void {
    this.closed.emit();
  }

  startEdit(policyName: string, policyDocument: string): void {
    if (!policyName) {
      return;
    }
    this.form.patchValue({
      policy_name: policyName,
      policy_doc: policyDocument
    });
    this.isEditing.set(true);
  }

  cancelEdit(): void {
    this.isEditing.set(false);
  }

  saveChanges(): void {
    if (this.form.invalid) {
      this.form.markAllAsTouched();
      return;
    }
    const { policy_name, policy_doc } = this.form.getRawValue();
    const roleName = this.roleName();
    const accountId = this.accountId();
    this.isSubmitLoading.set(true);
    this.rgwRoleService.attachPolicy(roleName, policy_name, policy_doc, accountId).subscribe({
      next: () => {
        this.isSubmitLoading.set(false);
        this.notificationService.show(
          NotificationType.success,
          $localize`Permission updated`,
          $localize`User role '${roleName}' permission '${policy_name}' updated.`
        );
        this.isEditing.set(false);
        this.changed.emit();
        this.reloadToken.update((token) => token + 1);
      },
      error: () => {
        this.isSubmitLoading.set(false);
        this.form.setErrors({ cdSubmitButton: true });
      }
    });
  }

  deleteSelectedPolicy(): void {
    const policyName = this.policyName();
    const roleName = this.roleName();
    const accountId = this.accountId();
    if (!policyName || !roleName) {
      return;
    }

    this.modalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.medium,
      itemDescription: $localize`policy`,
      itemNames: [policyName],
      actionDescription: $localize`remove`,
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.rgwRoleService.deletePolicy(roleName, policyName, accountId).subscribe({
            next: () => {
              this.notificationService.show(
                NotificationType.success,
                $localize`Policy detached successfully`,
                $localize`Policy "${policyName}" detached from role "${roleName}".`
              );
              observer.next();
              observer.complete();
              this.changed.emit();
              this.close();
            },
            error: (err) => observer.error(err)
          });
        });
      }
    });
  }
}
