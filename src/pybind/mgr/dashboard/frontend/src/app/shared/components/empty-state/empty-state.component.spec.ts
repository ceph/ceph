import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EmptyStateComponent } from './empty-state.component';
import { GridModule, LayerModule, TilesModule } from 'carbon-components-angular';

describe('ProductiveCardComponent', () => {
  let component: EmptyStateComponent;
  let fixture: ComponentFixture<EmptyStateComponent>;

  configureTestBed({
    imports: [EmptyStateComponent, GridModule, LayerModule, TilesModule]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(EmptyStateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
