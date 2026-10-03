import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  SimpleChanges
} from '@angular/core';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { Observable, of } from 'rxjs';
import { map } from 'rxjs/operators';
import { RgwRole } from '../models/rgw-role';

@Component({
  selector: 'cd-rgw-account-role-details',
  templateUrl: './rgw-account-role-details.component.html',
  styleUrls: ['./rgw-account-role-details.component.scss'],
  standalone: false
})
export class RgwAccountRoleDetailsComponent implements OnInit, OnChanges {
  @Input()
  selection: RgwRole;

  @Input()
  accountId: string;

  @Output()
  policySelected = new EventEmitter<{ roleName: string; policyName: string }>();

  policies$: Observable<any[]>;

  constructor(private rgwRoleService: RgwRoleService) {}

  ngOnInit(): void {
    this.loadPolicies();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.selection && this.selection) {
      this.loadPolicies();
    }
  }

  get roleName(): string {
    if (!this.selection) {
      return '';
    }
    return (
      this.selection.RoleName ||
      (this.selection as any).role_name ||
      (this.selection as any).row?.RoleName ||
      (this.selection as any).row?.role_name ||
      ''
    );
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
