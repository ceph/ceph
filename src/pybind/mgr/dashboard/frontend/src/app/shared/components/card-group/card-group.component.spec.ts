import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CardGroupComponent } from './card-group.component';

describe('CardGroupComponent', () => {
  let component: CardGroupComponent;
  let fixture: ComponentFixture<CardGroupComponent>;

  configureTestBed({
    declarations: [CardGroupComponent]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(CardGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
