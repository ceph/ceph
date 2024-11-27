import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RgwBucketTieringFormComponent } from './rgw-bucket-tiering-form.component';
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
import { RouterTestingModule } from '@angular/router/testing';

class MockRgwBucketService {
  setLifecycle = jest.fn().mockReturnValue(of(null));
  getLifecycle = jest.fn().mockReturnValue(of(null));
}

describe('RgwBucketTieringFormComponent', () => {
  let component: RgwBucketTieringFormComponent;
  let fixture: ComponentFixture<RgwBucketTieringFormComponent>;
  let rgwBucketService: MockRgwBucketService;

  configureTestBed({
    declarations: [RgwBucketTieringFormComponent, CdLabelComponent],
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
      RouterTestingModule
    ],
    providers: [
      ModalService,
      { provide: 'bucket', useValue: { bucket: 'bucket1', owner: 'dashboard' } },
      { provide: RgwBucketService, useClass: MockRgwBucketService }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketTieringFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    rgwBucketService = (TestBed.inject(RgwBucketService) as unknown) as MockRgwBucketService;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call setLifecyclePolicy function', () => {
    component.ngOnInit();
    component.tieringForm.setValue({
      name: 'test',
      storageClass: 'CLOUD',
      hasPrefix: false,
      prefix: '',
      tags: [],
      status: 'Enabled',
      days: 60
    });
    const createTieringSpy = jest.spyOn(component, 'submitTieringConfig');
    const setLifecycleSpy = jest.spyOn(rgwBucketService, 'setLifecycle').mockReturnValue(of(null));
    component.submitTieringConfig();
    expect(createTieringSpy).toHaveBeenCalled();
    expect(component.tieringForm.valid).toBe(true);
    expect(setLifecycleSpy).toHaveBeenCalled();
    expect(setLifecycleSpy).toHaveBeenCalledWith(
      'bucket1',
      JSON.stringify(component.configuredLifecycle.LifecycleConfiguration),
      'dashboard'
    );
  });
});
