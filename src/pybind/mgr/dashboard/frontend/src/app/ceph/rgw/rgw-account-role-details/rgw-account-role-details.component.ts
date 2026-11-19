import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  Input,
  OnChanges,
  Output,
  SimpleChanges
} from '@angular/core';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { Observable, of } from 'rxjs';
import { map } from 'rxjs/operators';
import { RgwRole, RgwRolePolicy } from '../models/rgw-role';

@Component({
  selector: 'cd-rgw-account-role-details',
  templateUrl: './rgw-account-role-details.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: false
})
export class RgwAccountRoleDetailsComponent implements OnChanges {
  @Input()
  selection: RgwRole;

  @Input()
  accountId: string;

  @Output()
  policySelected = new EventEmitter<{ roleName: string; policyName: string }>();

  policies$: Observable<RgwRolePolicy[]>;

  constructor(private rgwRoleService: RgwRoleService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if ((changes.selection || changes.accountId) && this.selection) {
      this.loadPolicies();
    }
  }

  get roleName(): string {
    return this.selection?.RoleName || '';
  }

  get roleArn(): string {
    return this.selection?.Arn || '';
  }

  get rolePath(): string {
    return this.selection?.Path || '/';
  }

  onPolicyClick(policyName: string): void {
    if (!policyName || !this.roleName) {
      return;
    }
    this.policySelected.emit({ roleName: this.roleName, policyName });
  }

  loadPolicies(): void {
    const roleName = this.roleName;
    if (!roleName || !this.accountId) {
      this.policies$ = of([]);
      return;
    }

    this.policies$ = this.rgwRoleService.listPolicies(roleName, this.accountId).pipe(
      map((policies: string[]) => {
        return (policies || []).map((name) => ({ name }));
      })
    );
  }
}
