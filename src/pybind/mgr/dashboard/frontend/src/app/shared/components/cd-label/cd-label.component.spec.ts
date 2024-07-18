import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CdLabelComponent } from './cd-label.component';
import { ColorClassFromTextPipe } from './color-class-from-text.pipe';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CdLabelComponent', () => {
  let component: CdLabelComponent;
  let fixture: ComponentFixture<CdLabelComponent>;

  configureTestBed({
    declarations: [CdLabelComponent, ColorClassFromTextPipe]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CdLabelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
