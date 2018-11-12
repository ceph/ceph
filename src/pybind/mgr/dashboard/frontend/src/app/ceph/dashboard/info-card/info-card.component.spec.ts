import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { InfoCardComponent } from './info-card.component';

describe('InfoCardComponent', () => {
  let component: InfoCardComponent;
  let fixture: ComponentFixture<InfoCardComponent>;

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [InfoCardComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InfoCardComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('Setting cardTitle makes title visible', () => {
    const cardTitle = 'Card Title';
    component.cardTitle = cardTitle;
    fixture.detectChanges();
    const titleDiv = fixture.debugElement.nativeElement.querySelector('.card-title');

    expect(titleDiv.textContent).toContain(cardTitle);
  });

  it('Setting link makes anchor visible', () => {
    const cardTitle = 'Card Title';
    const link = '/dashboard';
    component.cardTitle = cardTitle;
    component.link = link;
    fixture.detectChanges();
    const anchor = fixture.debugElement.nativeElement
      .querySelector('.card-title')
      .querySelector('a');

    expect(anchor.textContent).toContain(cardTitle);
    expect(anchor.href).toBe(`http://localhost${link}`);
  });

  it('Setting cardClass makes class set', () => {
    const cardClass = 'my-css-card-class';
    component.cardClass = cardClass;
    fixture.detectChanges();
    const card = fixture.debugElement.nativeElement.querySelector(`.card.${cardClass}`);

    expect(card).toBeTruthy();
  });

  it('Setting contentClass makes class set', () => {
    const contentClass = 'my-css-content-class';
    component.contentClass = contentClass;
    fixture.detectChanges();
    const card = fixture.debugElement.nativeElement.querySelector(`.card-body .${contentClass}`);

    expect(card).toBeTruthy();
  });
});
