import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwTopicListComponent } from './rgw-topic-list.component';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, PermissionHelper } from '~/testing/unit-test-helper';
import { RgwTopicDetailsComponent } from '../rgw-topic-details/rgw-topic-details.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

describe('RgwTopicListComponent', () => {
  let component: RgwTopicListComponent;
  let fixture: ComponentFixture<RgwTopicListComponent>;
  let rgwtTopicService: RgwTopicService;
  let rgwTopicServiceListSpy: jasmine.Spy;

  configureTestBed({
    declarations: [RgwTopicListComponent, RgwTopicDetailsComponent],
    imports: [BrowserAnimationsModule, RouterTestingModule, HttpClientTestingModule, SharedModule]
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        ToastrModule.forRoot(),
        RouterTestingModule
      ],
      declarations: [RgwTopicListComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwTopicListComponent);
    component = fixture.componentInstance;
    rgwtTopicService = TestBed.inject(RgwTopicService);

    // Stub external methods before ngOnInit triggers
    spyOn(component, 'setTableRefreshTimeout').and.stub();

    // Spy on the service method
    rgwTopicServiceListSpy = spyOn(rgwtTopicService, 'listTopic').and.returnValue(of([]));

    fixture.detectChanges(); // Triggers ngOnInit
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(rgwTopicServiceListSpy).toHaveBeenCalledTimes(2);
  });

  it('should test all TableActions combinations', () => {
    const permissionHelper: PermissionHelper = new PermissionHelper(component.permission);
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
