import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';

import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwAccountRolePoliciesModalComponent } from './rgw-account-role-policies-modal.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { SharedModule } from '~/app/shared/shared.module';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import {
  ButtonModule,
  CodeSnippetModule,
  InputModule,
  LoadingModule,
  ModalModule
} from 'carbon-components-angular';

describe('RgwAccountRolePoliciesModalComponent', () => {
  let component: RgwAccountRolePoliciesModalComponent;
  let fixture: ComponentFixture<RgwAccountRolePoliciesModalComponent>;
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
      CodeSnippetModule
    ],
    declarations: [RgwAccountRolePoliciesModalComponent]
  });

  beforeEach(fakeAsync(() => {
    rgwRoleService = TestBed.inject(RgwRoleService);
    spyOn(rgwRoleService, 'getPolicy').and.returnValue(
      of({ Version: '2012-10-17', Statement: [] })
    );
    spyOn(TestBed.inject(NotificationService), 'show');

    fixture = TestBed.createComponent(RgwAccountRolePoliciesModalComponent);
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
    expect(component.showPolicySnippet).toBe(true);
  });

  it('should enter and cancel edit mode', fakeAsync(() => {
    component.startEdit();
    expect(component.isEditing).toBe(true);
    expect(component.form.getRawValue().policy_name).toBe('policy-1');
    component.cancelEdit();
    expect(component.isEditing).toBe(false);
    tick();
    expect(component.showPolicySnippet).toBe(true);
  }));

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
