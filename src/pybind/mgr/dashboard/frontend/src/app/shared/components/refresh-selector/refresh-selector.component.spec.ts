import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { configureTestBed } from '../../../../testing/unit-test-helper';

import { RefreshIntervalService } from '../../services/refresh-interval.service';
import { RefreshSelectorComponent } from './refresh-selector.component';

describe('RefreshSelectorComponent', () => {
  let component: RefreshSelectorComponent;
  let fixture: ComponentFixture<RefreshSelectorComponent>;

  configureTestBed({
    imports: [FormsModule],
    declarations: [RefreshSelectorComponent],
    providers: [RefreshIntervalService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RefreshSelectorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
