import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TooltipModule } from 'ngx-bootstrap/tooltip';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { PipesModule } from '../../pipes/pipes.module';
import { UsageBarComponent } from './usage-bar.component';

describe('UsageBarComponent', () => {
  let component: UsageBarComponent;
  let fixture: ComponentFixture<UsageBarComponent>;

  configureTestBed({
    imports: [PipesModule, TooltipModule.forRoot()],
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
