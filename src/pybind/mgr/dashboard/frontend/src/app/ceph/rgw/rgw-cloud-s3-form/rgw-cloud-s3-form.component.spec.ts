import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwCloudS3FormComponent } from './rgw-cloud-s3-form.component';

describe('RgwCloudS3FormComponent', () => {
  let component: RgwCloudS3FormComponent;
  let fixture: ComponentFixture<RgwCloudS3FormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RgwCloudS3FormComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(RgwCloudS3FormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
