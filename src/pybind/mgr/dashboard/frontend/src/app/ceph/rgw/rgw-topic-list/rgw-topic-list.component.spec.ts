import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwTopicListComponent } from './rgw-topic-list.component';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwTopicDetailsComponent } from '../rgw-topic-details/rgw-topic-details.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
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
    rgwTopicServiceListSpy = spyOn(rgwtTopicService, 'listTopic').and.callThrough();
    fixture = TestBed.createComponent(RgwTopicListComponent);
    component = fixture.componentInstance;
    spyOn(component, 'setTableRefreshTimeout').and.stub();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(rgwTopicServiceListSpy).toHaveBeenCalledTimes(1);
  });

  it('should call listTopic on ngOnInit', () => {
    component.ngOnInit();
    expect(rgwTopicServiceListSpy).toHaveBeenCalled();
  });
});
