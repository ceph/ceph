import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ToastrModule } from 'ngx-toastr';
import {
  CheckboxModule,
  ComboBoxModule,
  GridModule,
  InputModule,
  SelectModule
} from 'carbon-components-angular';
import { CoreModule } from '~/app/core/core.module';
import { RgwStorageClassFormComponent } from './rgw-storage-class-form.component';

describe('RgwStorageClassFormComponent', () => {
  let component: RgwStorageClassFormComponent;
  let fixture: ComponentFixture<RgwStorageClassFormComponent>;

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
        CoreModule,
        SelectModule,
        ComboBoxModule,
        CheckboxModule
      ],
      declarations: [RgwStorageClassFormComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwStorageClassFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    component.goToListView();
    expect(component).toBeTruthy();
  });

  it('should initialize the form with empty values', () => {
    const storageClassForm = component.storageClassForm;
    expect(storageClassForm).toBeTruthy();
    expect(storageClassForm.get('zonegroup')).toBeTruthy();
    expect(storageClassForm.get('placement_target')).toBeTruthy();
  });

  it('on zonegroup changes', () => {
    component.zoneGroupDetails = {
      default_zonegroup: 'zonegroup1',
      name: 'zonegrp1',
      zonegroups: [
        {
          name: 'zonegroup1',
          id: 'zonegroup-id-1',
          placement_targets: [
            {
              name: 'default-placement',
              tier_targets: [
                {
                  val: {
                    storage_class: 'CLOUDIBM',
                    tier_type: 'cloud-s3',
                    retain_head_object: true,
                    s3: {
                      endpoint: 'https://s3.amazonaws.com',
                      access_key: 'ACCESSKEY',
                      storage_class: 'STANDARD',
                      target_path: '/path/to/storage',
                      target_storage_class: 'STANDARD',
                      region: 'useastr1',
                      secret: 'SECRETKEY',
                      multipart_min_part_size: 87877,
                      multipart_sync_threshold: 987877,
                      host_style: true
                    }
                  }
                }
              ]
            },
            {
              name: 'placement1',
              tier_targets: [
                {
                  val: {
                    storage_class: 'CloudIBM',
                    tier_type: 'cloud-s3',
                    retain_head_object: true,
                    s3: {
                      endpoint: 'https://s3.amazonaws.com',
                      access_key: 'ACCESSKEY',
                      storage_class: 'GLACIER',
                      target_path: '/pathStorage',
                      target_storage_class: 'CloudIBM',
                      region: 'useast1',
                      secret: 'SECRETKEY',
                      multipart_min_part_size: 187988787,
                      multipart_sync_threshold: 878787878,
                      host_style: false
                    }
                  }
                }
              ]
            }
          ]
        }
      ]
    };
    component.storageClassForm.get('zonegroup').setValue('zonegroup1');
    component.onZonegroupChange();
    expect(component.placementTargets).toEqual(['default-placement', 'placement1']);
    expect(component.storageClassForm.get('placement_target').value).toBe('default-placement');
  });

  it('should set form values on submit', () => {
    const storageClassName = 'storageClass1';
    component.storageClassForm.get('storage_class').setValue(storageClassName);
    component.storageClassForm.get('zonegroup').setValue('zonegroup1');
    component.storageClassForm.get('placement_target').setValue('placement1');
    component.storageClassForm.get('endpoint').setValue('http://ams03.com');
    component.storageClassForm.get('access_key').setValue('accesskey');
    component.storageClassForm.get('secret_key').setValue('secretkey');
    component.storageClassForm.get('target_path').setValue('/target');
    component.storageClassForm.get('retain_head_object').setValue(true);
    component.storageClassForm.get('region').setValue('useast1');
    component.storageClassForm.get('multipart_sync_threshold').setValue(1024);
    component.storageClassForm.get('multipart_min_part_size').setValue(256);
    component.goToListView();
    component.submitAction();
    expect(component).toBeTruthy();
  });
});
