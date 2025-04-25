import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RgwBucketNotificationListComponent } from './rgw-bucket-notification-list.component';
import { configureTestBed, PermissionHelper } from '~/testing/unit-test-helper';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { of } from 'rxjs';
import { ToastrModule } from 'ngx-toastr';

class MockRgwBucketService {
  listNotification = jest.fn((bucket: string) => of([{ bucket, notifications: [] }]));
}

describe('RgwBucketNotificationListComponent', () => {
  let component: RgwBucketNotificationListComponent;
  let fixture: ComponentFixture<RgwBucketNotificationListComponent>;
  let rgwtbucketService: RgwBucketService;
  let rgwnotificationListSpy: jasmine.Spy;

  configureTestBed({
    declarations: [RgwBucketNotificationListComponent],
    imports: [ComponentsModule, HttpClientTestingModule, ToastrModule.forRoot()],
    providers: [
      { provide: 'bucket', useValue: { bucket: 'bucket1', owner: 'dashboard' } },
      { provide: RgwBucketService, useClass: MockRgwBucketService }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketNotificationListComponent);
    component = fixture.componentInstance;
    rgwtbucketService = TestBed.inject(RgwBucketService);
    rgwnotificationListSpy = spyOn(rgwtbucketService, 'listNotification').and.callThrough();

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call list', () => {
    rgwtbucketService.listNotification('testbucket').subscribe((response) => {
      expect(response).toEqual([{ bucket: 'testbucket', notifications: [] }]);
    });
    expect(rgwnotificationListSpy).toHaveBeenCalledWith('testbucket');
  });

  it('should test all TableActions combinations', () => {
    const permissionHelper = new PermissionHelper(component.permission);
    const tableActions = permissionHelper.setPermissionsAndGetActions(component.tableActions);
    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: ['Create', 'Edit', 'Delete'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      'create,update': {
        actions: ['Create', 'Edit'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      'create,delete': {
        actions: ['Create', 'Delete'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      create: {
        actions: ['Create'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      'update,delete': {
        actions: ['Edit', 'Delete'],
        primary: {
          multiple: '',
          executing: '',
          single: '',
          no: ''
        }
      },
      update: {
        actions: ['Edit'],
        primary: {
          multiple: 'Edit',
          executing: 'Edit',
          single: 'Edit',
          no: 'Edit'
        }
      },
      delete: {
        actions: ['Delete'],
        primary: {
          multiple: 'Delete',
          executing: 'Delete',
          single: 'Delete',
          no: 'Delete'
        }
      },
      'no-permissions': {
        actions: [],
        primary: {
          multiple: '',
          executing: '',
          single: '',
          no: ''
        }
      }
    });
  });
});
