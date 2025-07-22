import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RgwBucketNotificationListComponent } from './rgw-bucket-notification-list.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { of } from 'rxjs';

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
    imports: [ComponentsModule, HttpClientTestingModule],
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

    fixture = TestBed.createComponent(RgwBucketNotificationListComponent);
    component = fixture.componentInstance;
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
});
