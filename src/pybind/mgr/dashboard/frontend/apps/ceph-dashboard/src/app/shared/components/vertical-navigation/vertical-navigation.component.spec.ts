import { ComponentFixture, TestBed } from '@angular/core/testing';

import { VerticalNavigationComponent } from './vertical-navigation.component';
import { By } from '@angular/platform-browser';

describe('VerticalNavigationComponent', () => {
  let component: VerticalNavigationComponent;
  let fixture: ComponentFixture<VerticalNavigationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [VerticalNavigationComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(VerticalNavigationComponent);
    component = fixture.componentInstance;
    component.items = ['item1', 'item2', 'item3'];
    component.inputIdentifier = 'filter';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a title', () => {
    component.title = 'testTitle';
    fixture.detectChanges();
    const title = fixture.debugElement.query(By.css('h3'));
    expect(title.nativeElement.textContent).toEqual('testTitle');
  });

  it('should select the first item as active if no item is selected', () => {
    expect(component.activeItem).toEqual('item1');
  });

  it('should filter the items by the keyword in filter input', () => {
    const event = new KeyboardEvent('keyup');
    const filterInput = fixture.debugElement.query(By.css('#filter'));
    filterInput.nativeElement.value = 'item1';
    filterInput.nativeElement.dispatchEvent(event);
    fixture.detectChanges();
    expect(component.filteredItems).toEqual(['item1']);

    filterInput.nativeElement.value = 'item2';
    filterInput.nativeElement.dispatchEvent(event);
    fixture.detectChanges();
    expect(component.filteredItems).toEqual(['item2']);
  });

  it('should select the item when clicked', () => {
    component.activeItem = '';

    // click on the first item in the nav list
    const item = fixture.debugElement.query(By.css('.nav-link'));
    item.nativeElement.click();
    fixture.detectChanges();
    expect(component.activeItem).toEqual('item1');
  });
});
