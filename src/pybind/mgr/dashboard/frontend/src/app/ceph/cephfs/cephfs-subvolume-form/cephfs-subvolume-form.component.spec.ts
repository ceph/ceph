import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumeFormComponent } from './cephfs-subvolume-form.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { FormHelper, configureTestBed } from '~/testing/unit-test-helper';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';

describe('CephfsSubvolumeFormComponent', () => {
  let component: CephfsSubvolumeFormComponent;
  let fixture: ComponentFixture<CephfsSubvolumeFormComponent>;
  let formHelper: FormHelper;
  let createSubVolumeSpy: jasmine.Spy;
  let editSubVolumeSpy: jasmine.Spy;

  configureTestBed({
    declarations: [CephfsSubvolumeFormComponent],
    providers: [NgbActiveModal],
    imports: [
      SharedModule,
      ToastrModule.forRoot(),
      ReactiveFormsModule,
      HttpClientTestingModule,
      RouterTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSubvolumeFormComponent);
    component = fixture.componentInstance;
    component.fsName = 'test_volume';
    component.pools = [];
    component.ngOnInit();
    formHelper = new FormHelper(component.subvolumeForm);
    createSubVolumeSpy = spyOn(TestBed.inject(CephfsSubvolumeService), 'create').and.stub();
    editSubVolumeSpy = spyOn(TestBed.inject(CephfsSubvolumeService), 'update').and.stub();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a form open in modal', () => {
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cd-modal')).not.toBe(null);
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
});
