import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwStorageClassDetailsComponent } from './rgw-storage-class-details.component';
import { StorageClassDetails } from '../models/rgw-storage-class.model';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { SimpleChange } from '@angular/core';

describe('RgwStorageClassDetailsComponent', () => {
  let component: RgwStorageClassDetailsComponent;
  let fixture: ComponentFixture<RgwStorageClassDetailsComponent>;

  const mockSelection: StorageClassDetails = {
    access_key: 'TestAccessKey',
    secret: 'TestSecret',
    target_path: '/test/path',
    multipart_min_part_size: 100,
    multipart_sync_threshold: 200,
    host_style: 'path',
    retain_head_object: true,
    allow_read_through: true,
    tier_type: 'local',
    acl_mappings: []
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule
      ],
      declarations: [RgwStorageClassDetailsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwStorageClassDetailsComponent);
    component = fixture.componentInstance;
    component.selection = mockSelection;

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should update storageDetails when selection input changes', () => {
    const newSelection: StorageClassDetails = {
      access_key: 'NewAccessKey',
      secret: 'NewSecret',
      target_path: '/new/path',
      multipart_min_part_size: 500,
      multipart_sync_threshold: 1000,
      host_style: 'virtual',
      retain_head_object: false,
      allow_read_through: false,
      tier_type: 'archive',
      glacier_restore_days: 1,
      glacier_restore_tier_type: 'standard',
      placement_targets: '',
      read_through_restore_days: 7,
      restore_storage_class: 'restored',
      zonegroup_name: 'zone1'
    };

    component.selection = newSelection;
    component.ngOnChanges({
      selection: new SimpleChange(null, newSelection, false)
    });
    expect(component.storageDetails).toEqual(newSelection);
  });
});
