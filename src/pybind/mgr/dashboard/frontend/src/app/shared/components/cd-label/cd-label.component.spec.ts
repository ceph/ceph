import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CdLabelComponent } from './cd-label.component';
import { ColorClassFromTextPipe } from './color-class-from-text.pipe';

describe('CdLabelComponent', () => {
  let component: CdLabelComponent;
  let fixture: ComponentFixture<CdLabelComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CdLabelComponent, ColorClassFromTextPipe]
    }).compileComponents();
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
