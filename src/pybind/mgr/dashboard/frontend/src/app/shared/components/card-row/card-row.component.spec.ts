import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CardRowComponent } from './card-row.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CardRowComponent', () => {
  let component: CardRowComponent;
  let fixture: ComponentFixture<CardRowComponent>;

  configureTestBed({
    declarations: [CardRowComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CardRowComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
