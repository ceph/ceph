import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';

import { HelperComponent } from './helper.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('HelperComponent', () => {
  let component: HelperComponent;
  let fixture: ComponentFixture<HelperComponent>;

  configureTestBed({
    imports: [NgbPopoverModule],
    declarations: [HelperComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HelperComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
