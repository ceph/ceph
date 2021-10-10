import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { RefreshSelectorComponent } from './refresh-selector.component';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { configureTestBed } from '~/testing/unit-test-helper';

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
