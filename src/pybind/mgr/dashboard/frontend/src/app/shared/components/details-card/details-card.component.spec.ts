import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DetailsCardComponent } from './details-card.component';

describe('DetailsCardComponent', () => {
  let component: DetailsCardComponent;
  let fixture: ComponentFixture<DetailsCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [DetailsCardComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(DetailsCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
