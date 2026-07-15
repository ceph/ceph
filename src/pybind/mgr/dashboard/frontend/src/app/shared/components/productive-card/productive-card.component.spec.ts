import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductiveCardComponent } from './productive-card.component';
import { GridModule, LayerModule, TilesModule } from 'carbon-components-angular';

describe('ProductiveCardComponent', () => {
  let component: ProductiveCardComponent;
  let fixture: ComponentFixture<ProductiveCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ProductiveCardComponent, GridModule, LayerModule, TilesModule]
    }).compileComponents();

    fixture = TestBed.createComponent(ProductiveCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
