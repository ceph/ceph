import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwTopicListComponent } from './rgw-topic-list.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwTopicDetailsComponent } from '../rgw-topic-details/rgw-topic-details.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
describe('RgwTopicListComponent', () => {
  let component: RgwTopicListComponent;
  let fixture: ComponentFixture<RgwTopicListComponent>;

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
