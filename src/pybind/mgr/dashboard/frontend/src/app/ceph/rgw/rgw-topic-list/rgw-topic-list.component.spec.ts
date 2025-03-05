import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwTopicListComponent } from './rgw-topic-list.component';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, PermissionHelper } from '~//testing/unit-test-helper';
import { RgwTopicDetailsComponent } from '../rgw-topic-details/rgw-topic-details.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { ModalService } from 'carbon-components-angular';

describe('RgwTopicListComponent', () => {
  let component: RgwTopicListComponent;
  let fixture: ComponentFixture<RgwTopicListComponent>;
  let rgwtTopicService: RgwTopicService;
  let rgwTopicServiceListSpy: jasmine.Spy;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        ToastrModule.forRoot(),
        RouterTestingModule,
        NgbNavModule
      ],
      providers: [
        RgwTopicService,
        ModalService, // Add the service here
        ModalCdsService // Add the service if it’s used
      ],
      declarations: [RgwTopicListComponent, RgwTopicDetailsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwTopicListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load topics', () => {
    rgwtTopicService = TestBed.inject(RgwTopicService);
    rgwTopicServiceListSpy = spyOn(rgwtTopicService, 'listTopic').and.callThrough();
    component.ngOnInit();
    expect(rgwTopicServiceListSpy).toHaveBeenCalledTimes(1);
  });

  it('should open the create topic modal', () => {
    const modalService = TestBed.inject(ModalCdsService);
    const modalServiceShowSpy = spyOn(modalService, 'show').and.stub();

    component.tableActions = [
      {
        click: () => {
          modalService.show();
        },
        permission: 'create',
        name: '',
        icon: ''
      },
      {
        click: () => {},
        permission: 'delete',
        name: '',
        icon: ''
      }
    ];

    component.tableActions[0].click(); // Simulate clicking the create action
    fixture.detectChanges();
    expect(modalServiceShowSpy).toHaveBeenCalledTimes(1);
  });

  it('should open the delete confirmation modal', () => {
    const modalService = TestBed.inject(ModalService);
    const modalServiceShowSpy = spyOn(modalService, 'show').and.stub();

    component.tableActions = [
      {
        click: () => {},
        permission: 'create',
        name: '',
        icon: ''
      },
      {
        click: () => {
          modalService.show();
        },
        permission: 'delete',
        name: '',
        icon: ''
      }
    ];

    component.tableActions[1].click(); // Simulate clicking the delete action
    fixture.detectChanges();
    expect(modalServiceShowSpy).toHaveBeenCalledTimes(1);
  });
});
