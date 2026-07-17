import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwStorageClassResourceSidebarComponent } from './rgw-storage-class-resource-sidebar.component';
import { StorageClassDetails } from '../models/rgw-storage-class.model';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

describe('RgwStorageClassResourceSidebarComponent', () => {
  let component: RgwStorageClassResourceSidebarComponent;
  let fixture: ComponentFixture<RgwStorageClassResourceSidebarComponent>;

  const mockSelection: StorageClassDetails = {
    storage_class: 'TestStorageClass',
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
      declarations: [RgwStorageClassResourceSidebarComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwStorageClassResourceSidebarComponent);
    component = fixture.componentInstance;
    component.selection = mockSelection;

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
