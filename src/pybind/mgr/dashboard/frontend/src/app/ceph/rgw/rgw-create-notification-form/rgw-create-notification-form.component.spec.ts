import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwCreateNotificationFormComponent } from './rgw-create-notification-form.component';
import { CdLabelComponent } from '~/app/shared/components/cd-label/cd-label.component';
import { ReactiveFormsModule } from '@angular/forms';
import { ToastrModule } from 'ngx-toastr';
import { ComponentsModule } from '~/app/shared/components/components.module';
import {
  InputModule,
  ModalModule,
  ModalService,
  NumberModule,
  RadioModule,
  SelectModule,
  ComboBoxModule
} from 'carbon-components-angular';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { of } from 'rxjs';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { NO_ERRORS_SCHEMA } from '@angular/core';

class MockRgwBucketService {
  setNotification = jest.fn().mockReturnValue(of(null));
  getBucketNotificationList = jest.fn().mockReturnValue(of(null));
}

describe('RgwCreateNotificationFormComponent', () => {
  let component: RgwCreateNotificationFormComponent;
  let fixture: ComponentFixture<RgwCreateNotificationFormComponent>;
  let rgwBucketService: MockRgwBucketService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwCreateNotificationFormComponent, CdLabelComponent],
      imports: [
        ReactiveFormsModule,
        HttpClientTestingModule,
        RadioModule,
        SelectModule,
        NumberModule,
        InputModule,
        ToastrModule.forRoot(),
        ComponentsModule,
        ModalModule,
        ComboBoxModule,
        ReactiveFormsModule,
        ComponentsModule,
        InputModule,
        RouterTestingModule
      ],
      schemas: [NO_ERRORS_SCHEMA],
      providers: [
        ModalService,
        { provide: 'bucket', useValue: { bucket: 'bucket1', owner: 'dashboard' } },
        { provide: RgwBucketService, useClass: MockRgwBucketService }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwCreateNotificationFormComponent);
    component = fixture.componentInstance;
    rgwBucketService = TestBed.inject(RgwBucketService) as unknown as MockRgwBucketService;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

 
});