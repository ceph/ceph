import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of, throwError } from 'rxjs';

import { CephfsSubvolumeFormComponent } from './cephfs-subvolume-form.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { FormHelper, configureTestBed } from '~/testing/unit-test-helper';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { CheckboxModule, InputModule, ModalModule, SelectModule } from 'carbon-components-angular';
import {
  SNAPSHOT_VISIBILITY_CONFIG_NAME,
  SNAPSHOT_VISIBILITY_CONFIG_SECTION
} from '~/app/shared/models/cephfs-subvolume.model';

describe('CephfsSubvolumeFormComponent', () => {
  let component: CephfsSubvolumeFormComponent;
  let fixture: ComponentFixture<CephfsSubvolumeFormComponent>;
  let formHelper: FormHelper;
  let createSubVolumeSpy: jasmine.Spy;
  let editSubVolumeSpy: jasmine.Spy;
  let getSnapshotVisibilitySpy: jasmine.Spy;
  let configFilterSpy: jasmine.Spy;
  let subvolumeService: CephfsSubvolumeService;
  let configurationService: ConfigurationService;

  configureTestBed({
    declarations: [CephfsSubvolumeFormComponent],
    providers: [NgbActiveModal],
    imports: [
      SharedModule,
      ToastrModule.forRoot(),
      ReactiveFormsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ModalModule,
      InputModule,
      SelectModule,
      CheckboxModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSubvolumeFormComponent);
    component = fixture.componentInstance;
    component.fsName = 'test_volume';
    component.pools = [];
    subvolumeService = TestBed.inject(CephfsSubvolumeService);
    configurationService = TestBed.inject(ConfigurationService);
    createSubVolumeSpy = spyOn(subvolumeService, 'create').and.returnValue(of({ status: 200 }));
    editSubVolumeSpy = spyOn(subvolumeService, 'update').and.returnValue(of({ status: 200 }));
    spyOn(subvolumeService, 'info').and.returnValue(
      of({ bytes_quota: 'infinite', uid: 0, gid: 0, pool_namespace: false, mode: 755 } as any)
    );
    getSnapshotVisibilitySpy = spyOn(subvolumeService, 'getSnapshotVisibility').and.returnValue(
      of('1')
    );
    configFilterSpy = spyOn(configurationService, 'filter').and.returnValue(of([]));
    component.ngOnInit();
    formHelper = new FormHelper(component.subvolumeForm);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a form open in modal', () => {
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cds-modal')).not.toBe(null);
  });

  it('should have the volume name prefilled', () => {
    component.ngOnInit();
    expect(component.subvolumeForm.get('volumeName').value).toBe('test_volume');
  });

  it('should submit the form', () => {
    formHelper.setValue('subvolumeName', 'test_subvolume');
    formHelper.setValue('size', 10);
    component.submit();

    expect(createSubVolumeSpy).toHaveBeenCalled();
    expect(editSubVolumeSpy).not.toHaveBeenCalled();
  });

  it('should edit the subvolume', () => {
    component.isEdit = true;
    component.ngOnInit();
    formHelper.setValue('subvolumeName', 'test_subvolume');
    formHelper.setValue('size', 10);
    component.submit();

    expect(editSubVolumeSpy).toHaveBeenCalled();
    expect(createSubVolumeSpy).not.toHaveBeenCalled();
  });

  describe('Snapshot Visibility', () => {
    it('should show snapshot visibility when config is enabled', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      component.ngOnInit();
      fixture.detectChanges();

      expect(component.showSnapshotVisibility).toBe(true);
    });

    it('should hide snapshot visibility when config is disabled', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'false' }]
          }
        ])
      );
      component.ngOnInit();
      fixture.detectChanges();

      expect(component.showSnapshotVisibility).toBe(false);
    });

    it('should hide snapshot visibility on config error', () => {
      configFilterSpy.and.returnValue(throwError(() => new Error('Config error')));
      component.ngOnInit();
      fixture.detectChanges();

      expect(component.showSnapshotVisibility).toBe(false);
    });

    it('should pass snapshotVisibility to create when visibility config is enabled', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      component.ngOnInit();
      formHelper = new FormHelper(component.subvolumeForm);
      formHelper.setValue('subvolumeName', 'test_subvolume');
      formHelper.setValue('snapshotVisibility', true);
      component.submit();

      expect(createSubVolumeSpy).toHaveBeenCalled();
      const createCallArgs = createSubVolumeSpy.calls.mostRecent().args;
      expect(createCallArgs[9]).toBe(true);
    });

    it('should not pass snapshotVisibility to create when visibility config is disabled', () => {
      configFilterSpy.and.returnValue(of([]));
      component.ngOnInit();
      formHelper = new FormHelper(component.subvolumeForm);
      formHelper.setValue('subvolumeName', 'test_subvolume');
      component.submit();

      expect(createSubVolumeSpy).toHaveBeenCalled();
      const createCallArgs = createSubVolumeSpy.calls.mostRecent().args;
      expect(createCallArgs[9]).toBeUndefined();
    });

    it('should pass snapshotVisibility to update when visibility config is enabled', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      component.isEdit = true;
      component.subVolumeName = 'test_subvolume';
      component.ngOnInit();
      formHelper = new FormHelper(component.subvolumeForm);
      formHelper.setValue('size', 10);
      formHelper.setValue('snapshotVisibility', false);
      component.submit();

      expect(editSubVolumeSpy).toHaveBeenCalled();
      const updateCallArgs = editSubVolumeSpy.calls.mostRecent().args;
      expect(updateCallArgs[4]).toBe(false);
    });

    it('should set snapshotVisibility to true when getSnapshotVisibility returns 1', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      getSnapshotVisibilitySpy.and.returnValue(of('1'));
      component.isEdit = true;
      component.subVolumeName = 'test_subvolume';
      component.ngOnInit();
      fixture.detectChanges();

      expect(component.subvolumeForm.get('snapshotVisibility').value).toBe(true);
    });

    it('should set snapshotVisibility to false when getSnapshotVisibility returns 0', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      getSnapshotVisibilitySpy.and.returnValue(of('0'));
      component.isEdit = true;
      component.subVolumeName = 'test_subvolume';
      component.ngOnInit();
      fixture.detectChanges();

      expect(component.subvolumeForm.get('snapshotVisibility').value).toBe(false);
    });

    it('should disable snapshotVisibility control when config is disabled', () => {
      configFilterSpy.and.returnValue(of([]));
      component.ngOnInit();
      fixture.detectChanges();

      expect(component.subvolumeForm.get('snapshotVisibility').disabled).toBe(true);
    });

    it('should set snapshotVisibility value to false when config is disabled', () => {
      configFilterSpy.and.returnValue(of([]));
      component.ngOnInit();
      fixture.detectChanges();

      expect(component.subvolumeForm.get('snapshotVisibility').value).toBe(false);
    });

    it('should default snapshotVisibility to true in create mode when config is enabled', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      component.isEdit = false;
      component.ngOnInit();
      fixture.detectChanges();

      expect(component.subvolumeForm.get('snapshotVisibility').value).toBe(true);
    });

    it('should not call getSnapshotVisibility in create mode', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      component.isEdit = false;
      component.ngOnInit();
      fixture.detectChanges();

      expect(getSnapshotVisibilitySpy).not.toHaveBeenCalled();
    });

    it('should call getSnapshotVisibility only in edit mode when config is enabled', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      component.isEdit = true;
      component.subVolumeName = 'test_subvolume';
      component.ngOnInit();
      fixture.detectChanges();

      expect(getSnapshotVisibilitySpy).toHaveBeenCalled();
    });

    it('should pass snapshotVisibility as true to update when checkbox is checked', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      getSnapshotVisibilitySpy.and.returnValue(of('1'));
      component.isEdit = true;
      component.subVolumeName = 'test_subvolume';
      component.ngOnInit();
      fixture.detectChanges();
      formHelper = new FormHelper(component.subvolumeForm);
      formHelper.setValue('snapshotVisibility', true);
      component.submit();

      expect(editSubVolumeSpy).toHaveBeenCalled();
      const updateCallArgs = editSubVolumeSpy.calls.mostRecent().args;
      expect(updateCallArgs[4]).toBe(true);
    });

    it('should pass snapshotVisibility as true to create when checkbox is checked', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      component.ngOnInit();
      formHelper = new FormHelper(component.subvolumeForm);
      formHelper.setValue('subvolumeName', 'new_subvolume');
      formHelper.setValue('snapshotVisibility', true);
      component.submit();

      expect(createSubVolumeSpy).toHaveBeenCalled();
      const createCallArgs = createSubVolumeSpy.calls.mostRecent().args;
      expect(createCallArgs[9]).toBe(true);
    });

    it('should pass snapshotVisibility as false to create when checkbox is unchecked', () => {
      configFilterSpy.and.returnValue(
        of([
          {
            name: SNAPSHOT_VISIBILITY_CONFIG_NAME,
            value: [{ section: SNAPSHOT_VISIBILITY_CONFIG_SECTION, value: 'true' }]
          }
        ])
      );
      component.ngOnInit();
      formHelper = new FormHelper(component.subvolumeForm);
      formHelper.setValue('subvolumeName', 'new_subvolume');
      formHelper.setValue('snapshotVisibility', false);
      component.submit();

      expect(createSubVolumeSpy).toHaveBeenCalled();
      const createCallArgs = createSubVolumeSpy.calls.mostRecent().args;
      expect(createCallArgs[9]).toBe(false);
    });
  });
});
