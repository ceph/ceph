import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwBucketLifecycleListComponent } from './rgw-bucket-lifecycle-list.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { of } from 'rxjs';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { ReactiveFormsModule } from '@angular/forms';
import { ToastrModule } from 'ngx-toastr';
import { ComponentsModule } from '~/app/shared/components/components.module';
import {
  InputModule,
  ModalModule,
  ModalService,
  NumberModule,
  RadioModule,
  SelectModule
} from 'carbon-components-angular';
import { CdLabelComponent } from '~/app/shared/components/cd-label/cd-label.component';

class MockRgwBucketService {
  setLifecycle = jest.fn().mockReturnValue(of(null));
  getLifecycle = jest.fn().mockReturnValue(of(null));
}

describe('RgwBucketLifecycleListComponent', () => {
  let component: RgwBucketLifecycleListComponent;
  let fixture: ComponentFixture<RgwBucketLifecycleListComponent>;

  configureTestBed({
    declarations: [RgwBucketLifecycleListComponent, CdLabelComponent],
    imports: [
      ReactiveFormsModule,
      RadioModule,
      SelectModule,
      NumberModule,
      InputModule,
      ToastrModule.forRoot(),
      ComponentsModule,
      ModalModule
    ],
    providers: [
      ModalService,
      { provide: 'bucket', useValue: { bucket: 'bucket1', owner: 'dashboard' } },
      { provide: RgwBucketService, useClass: MockRgwBucketService }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketLifecycleListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
