import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';

import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { UsageBarComponent } from './usage-bar.component';

describe('UsageBarComponent', () => {
  let component: UsageBarComponent;
  let fixture: ComponentFixture<UsageBarComponent>;

  configureTestBed({
    imports: [PipesModule, NgbTooltipModule],
    declarations: [UsageBarComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UsageBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
