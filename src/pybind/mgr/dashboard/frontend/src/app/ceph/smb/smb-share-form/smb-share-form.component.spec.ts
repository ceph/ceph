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
  NumberModule,
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
        NumberModule,
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
      name: 'My Share',
      subvolume_group: 'group1',
      subvolume: 'subvol1',
      prefixedPath: '/volumes/fs1/group1/subvol1',
      inputPath: '/',
      browseable: true,
      readonly: false,
      read_iops_limit: 0,
      write_iops_limit: 0,
      read_bw_limit: 0,
      read_bw_limit_unit: 'MiB',
      write_bw_limit: 0,
      write_bw_limit_unit: 'MiB',
      read_delay_max: 30,
      write_delay_max: 30
    });
    component.submitAction();
    expect(component).toBeTruthy();
  });

  describe('QoS', () => {
    it('should have QoS form controls with default values', () => {
      component.ngOnInit();
      expect(component.smbShareForm.get('read_iops_limit')).toBeTruthy();
      expect(component.smbShareForm.get('write_iops_limit')).toBeTruthy();
      expect(component.smbShareForm.get('read_bw_limit')).toBeTruthy();
      expect(component.smbShareForm.get('read_bw_limit_unit')).toBeTruthy();
      expect(component.smbShareForm.get('write_bw_limit')).toBeTruthy();
      expect(component.smbShareForm.get('write_bw_limit_unit')).toBeTruthy();
      expect(component.smbShareForm.get('read_delay_max')).toBeTruthy();
      expect(component.smbShareForm.get('write_delay_max')).toBeTruthy();
      expect(component.smbShareForm.get('read_iops_limit').value).toBe(0);
      expect(component.smbShareForm.get('write_delay_max').value).toBe(30);
    });

    it('should include QoS fields in buildRequest when set', () => {
      component.clusterId = 'cluster1';
      component.smbShareForm.patchValue({
        share_id: 'share1',
        name: 'My Share',
        volume: 'fs1',
        inputPath: '/',
        read_iops_limit: 1000,
        write_iops_limit: 500,
        read_bw_limit: 100,
        read_bw_limit_unit: 'MiB',
        write_delay_max: 60
      });
      const request = component.buildRequest();
      expect(request.share_resource.cephfs.qos.read_iops_limit).toBe(1000);
      expect(request.share_resource.cephfs.qos.write_iops_limit).toBe(500);
      expect(request.share_resource.cephfs.qos.read_bw_limit).toBe(100 * 1024 * 1024);
      expect(request.share_resource.cephfs.qos.write_delay_max).toBe(60);
    });
  });
});
