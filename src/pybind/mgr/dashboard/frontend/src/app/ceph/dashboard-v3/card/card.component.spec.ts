import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { CardComponent } from './card.component';

describe('CardComponent', () => {
  let component: CardComponent;
  let fixture: ComponentFixture<CardComponent>;

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [CardComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CardComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('Setting cards title makes title visible', () => {
    const title = 'Card Title';
    component.cardTitle = title;
    fixture.detectChanges();
    const titleDiv = fixture.debugElement.nativeElement.querySelector('.card-title');

    expect(titleDiv.textContent).toContain(title);
  });
});
