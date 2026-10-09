import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';

import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwAccountRolePoliciesSidePanelComponent } from './rgw-account-role-policies-side-panel.component';
import { RgwAccountRolePolicyFieldsComponent } from '../rgw-account-role-policy-fields/rgw-account-role-policy-fields.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { SharedModule } from '~/app/shared/shared.module';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import {
  ButtonModule,
  CodeSnippetModule,
  InputModule,
  LoadingModule,
  ModalModule,
  SkeletonModule
} from 'carbon-components-angular';

describe('RgwAccountRolePoliciesSidePanelComponent', () => {
  let component: RgwAccountRolePoliciesSidePanelComponent;
  let fixture: ComponentFixture<RgwAccountRolePoliciesSidePanelComponent>;
  let rgwRoleService: RgwRoleService;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ModalModule,
      ButtonModule,
      InputModule,
      LoadingModule,
      SkeletonModule,
      CodeSnippetModule
    ],
    declarations: [RgwAccountRolePoliciesSidePanelComponent, RgwAccountRolePolicyFieldsComponent]
  });

  beforeEach(fakeAsync(() => {
    rgwRoleService = TestBed.inject(RgwRoleService);
    spyOn(rgwRoleService, 'getPolicy').and.returnValue(
      of(JSON.stringify({ Version: '2012-10-17', Statement: [] }, null, 2))
    );
    spyOn(TestBed.inject(NotificationService), 'show');

    fixture = TestBed.createComponent(RgwAccountRolePoliciesSidePanelComponent);
    component = fixture.componentInstance;
    component.accountId = 'test-account';
    component.roleName = 'test-role';
    component.policyName = 'policy-1';
    component.expanded = true;
    fixture.detectChanges();
    tick();
  }));

  it('should create and load the selected policy', () => {
    expect(component).toBeTruthy();
    expect(rgwRoleService.getPolicy).toHaveBeenCalledWith('test-role', 'policy-1', 'test-account');
    expect(component.selectedPolicyName).toBe('policy-1');
    expect(component.selectedPolicyDocument).toContain('"Version": "2012-10-17"');
  });

  it('should enter and cancel edit mode', () => {
    component.startEdit();
    expect(component.isEditing).toBe(true);
    expect(component.form.getRawValue().policy_name).toBe('policy-1');
    component.cancelEdit();
    expect(component.isEditing).toBe(false);
    expect(component.selectedPolicyDocument).toContain('"Version": "2012-10-17"');
  });

  it('should save policy changes', () => {
    spyOn(rgwRoleService, 'attachPolicy').and.returnValue(of(null));
    component.startEdit();
    component.form.patchValue({
      policy_doc: JSON.stringify(
        { Version: '2012-10-17', Statement: [{ Effect: 'Allow' }] },
        null,
        2
      )
    });
    component.saveChanges();
    expect(rgwRoleService.attachPolicy).toHaveBeenCalled();
    expect(component.isEditing).toBe(false);
  });

  it('should open delete confirmation', () => {
    const modalService = TestBed.inject(ModalCdsService);
    spyOn(modalService, 'show').and.returnValue({
      closeModal: jasmine.createSpy('closeModal'),
      stopLoadingSpinner: jasmine.createSpy('stopLoadingSpinner')
    } as any);
    component.deleteSelectedPolicy();
    expect(modalService.show).toHaveBeenCalled();
  });

  it('should emit closed when close is called', () => {
    spyOn(component.closed, 'emit');
    component.close();
    expect(component.closed.emit).toHaveBeenCalled();
  });
});
