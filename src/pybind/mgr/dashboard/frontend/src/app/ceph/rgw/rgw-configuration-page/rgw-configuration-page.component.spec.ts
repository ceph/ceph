import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwConfigurationPageComponent } from './rgw-configuration-page.component';

describe('RgwConfigurationPageComponent', () => {
  let component: RgwConfigurationPageComponent;
  let fixture: ComponentFixture<RgwConfigurationPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwConfigurationPageComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwConfigurationPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
