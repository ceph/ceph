import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwStorageClassDetailsComponent } from './rgw-storage-class-details.component';
import { StorageClassDetails } from '../models/rgw-storage-class.model';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

describe('RgwStorageClassDetailsComponent', () => {
  let component: RgwStorageClassDetailsComponent;
  let fixture: ComponentFixture<RgwStorageClassDetailsComponent>;

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
});
