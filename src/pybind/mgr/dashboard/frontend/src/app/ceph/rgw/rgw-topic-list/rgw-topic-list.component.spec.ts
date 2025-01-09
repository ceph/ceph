import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwTopicListComponent } from './rgw-topic-list.component';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, PermissionHelper } from '~/testing/unit-test-helper';
import { RgwTopicDetailsComponent } from '../rgw-topic-details/rgw-topic-details.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
describe('RgwTopicListComponent', () => {
  let component: RgwTopicListComponent;
  let fixture: ComponentFixture<RgwTopicListComponent>;
  let rgwtTopicService: RgwTopicService;
  let rgwTopicServiceListSpy: jasmine.Spy;

  configureTestBed({
    declarations: [RgwTopicListComponent, RgwTopicDetailsComponent],
    imports: [
      BrowserAnimationsModule,
      RouterTestingModule,
      SharedModule,
      NgbNavModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ]
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
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
