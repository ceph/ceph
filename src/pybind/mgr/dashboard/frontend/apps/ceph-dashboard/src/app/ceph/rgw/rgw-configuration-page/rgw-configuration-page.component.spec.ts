import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwConfigurationPageComponent } from './rgw-configuration-page.component';
import { NgbActiveModal, NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { RgwModule } from '../rgw.module';
import { RouterTestingModule } from '@angular/router/testing';

describe('RgwConfigurationPageComponent', () => {
  let component: RgwConfigurationPageComponent;
  let fixture: ComponentFixture<RgwConfigurationPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwConfigurationPageComponent],
      providers: [NgbActiveModal],
      imports: [HttpClientTestingModule, SharedModule, NgbNavModule, RgwModule, RouterTestingModule]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwConfigurationPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
