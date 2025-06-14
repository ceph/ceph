import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwGlacierStorageclassFormComponent } from './rgw-glacier-storageclass-form.component';

describe('RgwGlacierStorageclassFormComponent', () => {
  let component: RgwGlacierStorageclassFormComponent;
  let fixture: ComponentFixture<RgwGlacierStorageclassFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RgwGlacierStorageclassFormComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(RgwGlacierStorageclassFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
