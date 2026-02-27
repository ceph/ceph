import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwNotificationFormComponent } from './rgw-notification-form.component';
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
  listNotification = jest.fn().mockReturnValue(of([]));
}

describe('RgwNotificationFormComponent', () => {
  let component: RgwNotificationFormComponent;
  let fixture: ComponentFixture<RgwNotificationFormComponent>;
  let rgwBucketService: MockRgwBucketService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwNotificationFormComponent, CdLabelComponent],
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

    fixture = TestBed.createComponent(RgwNotificationFormComponent);
    component = fixture.componentInstance;
    rgwBucketService = (TestBed.inject(RgwBucketService) as unknown) as MockRgwBucketService;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
  it('should call setNotification when submitting form', () => {
    rgwBucketService = (TestBed.inject(RgwBucketService) as unknown) as MockRgwBucketService;
    component['notificationList'] = [];
    component.notificationForm.patchValue({
      id: 'notif-1',
      topic: 'arn:aws:sns:us-east-1:123456789012:MyTopic',
      event: ['PutObject']
    });

    component.notificationForm.get('filter.s3Key')?.setValue([{ Name: 'prefix', Value: 'logs/' }]);
    component.notificationForm.get('filter.s3Metadata')?.setValue([{ Name: '', Value: '' }]);
    component.notificationForm.get('filter.s3Tags')?.setValue([{ Name: '', Value: '' }]);
    component.onSubmit();
    expect(rgwBucketService.setNotification).toHaveBeenCalled();
  });
});
