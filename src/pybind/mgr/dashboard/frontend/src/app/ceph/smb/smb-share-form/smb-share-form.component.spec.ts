import { ComponentFixture, TestBed } from '@angular/core/testing';
import { SmbShareFormComponent } from './smb-share-form.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule, Validators } from '@angular/forms';
import { ToastrModule } from 'ngx-toastr';
import {
  CheckboxModule,
  ComboBoxModule,
  GridModule,
  InputModule,
  SelectModule
} from 'carbon-components-angular';
import { SmbService } from '~/app/shared/api/smb.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

describe('SmbShareFormComponent', () => {
  let component: SmbShareFormComponent;
  let fixture: ComponentFixture<SmbShareFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        ToastrModule.forRoot(),
        GridModule,
        InputModule,
        SelectModule,
        ComboBoxModule,
        CheckboxModule
      ],
      declarations: [SmbShareFormComponent],
      providers: [SmbService, TaskWrapperService]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbShareFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should create the form', () => {
    component.ngOnInit();
    expect(component.smbShareForm).toBeDefined();
    expect(component.smbShareForm.get('share_id')).toBeTruthy();
    expect(component.smbShareForm.get('volume')).toBeTruthy();
    expect(component.smbShareForm.get('subvolume_group')).toBeTruthy();
    expect(component.smbShareForm.get('prefixedPath')).toBeTruthy();
  });

  it('should update subvolume group when volume changes', () => {
    component.smbShareForm.get('volume').setValue('fs1');
    component.smbShareForm.get('subvolume').setValue('subvol1');
    component.volumeChangeHandler();
    expect(component.smbShareForm.get('subvolume_group').value).toBe('');
    expect(component.smbShareForm.get('subvolume').value).toBe('');
  });

  it('should call getSubVolGrp when volume is selected', () => {
    const fsName = 'fs1';
    component.smbShareForm.get('volume').setValue(fsName);
    component.volumeChangeHandler();
    expect(component).toBeTruthy();
  });

  it('should set the correct subvolume validation', () => {
    component.smbShareForm.get('subvolume_group').setValue('');
    expect(component.smbShareForm.get('subvolume').hasValidator(Validators.required)).toBe(false);
    component.smbShareForm.get('subvolume_group').setValue('otherGroup');
    expect(component.smbShareForm.get('subvolume').hasValidator(Validators.required)).toBe(false);
  });

  it('should call submitAction', () => {
    component.smbShareForm.setValue({
      share_id: 'share1',
      volume: 'fs1',
      subvolume_group: 'group1',
      subvolume: 'subvol1',
      prefixedPath: '/volumes/fs1/group1/subvol1',
      inputPath: '/',
      browseable: true,
      readonly: false
    });
    component.submitAction();
    expect(component).toBeTruthy();
  });
});
