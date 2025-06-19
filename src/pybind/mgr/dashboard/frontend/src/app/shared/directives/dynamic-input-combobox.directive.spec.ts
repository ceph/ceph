import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { DEBOUNCE_TIMER, DynamicInputComboboxDirective } from './dynamic-input-combobox.directive';
import { Subject } from 'rxjs';
import { Component, EventEmitter } from '@angular/core';
import { ComboBoxItem } from '../models/combo-box.model';

@Component({
  template: `<div cdDynamicInputCombobox [items]="[]"></div>`
})
class MockComponent {
  items: ComboBoxItem[] = [{ content: 'Item1', name: 'Item1' }];
  searchSubject = new Subject<string>();
  selectedItems: ComboBoxItem[] = [];
  updatedItems = new EventEmitter<ComboBoxItem[]>();
}

describe('DynamicInputComboboxDirective', () => {
  let fixture: ComponentFixture<MockComponent>;
  let directive: DynamicInputComboboxDirective;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DynamicInputComboboxDirective, MockComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(MockComponent);

    directive = fixture.debugElement.children[0].injector.get(DynamicInputComboboxDirective);
    fixture.detectChanges();
  });

  afterEach(() => {
    directive.ngOnDestroy();
  });

  it('should create an instance', () => {
    expect(directive).toBeTruthy();
  });

  it('should update items when input is given', fakeAsync(() => {
    const newItem = 'NewItem';
    directive.onInput(newItem);
    tick(DEBOUNCE_TIMER);

    expect(directive.items[0].content).toBe(newItem);
  }));

  it('should not unselect selected items', fakeAsync(() => {
    const selectedItems: ComboBoxItem[] = [
      {
        content: 'selectedItem',
        name: 'selectedItem',
        selected: true
      }
    ];

    directive.items = selectedItems;

    directive.onSelected(selectedItems);
    tick(DEBOUNCE_TIMER);

    directive.onInput(selectedItems[0].content);
    tick(DEBOUNCE_TIMER);

    expect(directive.items[0].content).toBe(selectedItems[0].content);
    expect(directive.items[0].selected).toBeTruthy();
  }));
});
