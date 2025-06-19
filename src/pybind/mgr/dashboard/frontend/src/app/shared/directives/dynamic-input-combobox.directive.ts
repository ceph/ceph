import {
  Directive,
  Input,
  OnDestroy,
  OnInit,
  Output,
  EventEmitter,
  HostListener
} from '@angular/core';
import { ComboBoxItem } from '../models/combo-box.model';
import { ComboBoxService } from '../services/combo-box.service';
import { Subject, Subscription } from 'rxjs';
import { debounceTime, distinctUntilChanged } from 'rxjs/operators';

export const DEBOUNCE_TIMER = 300;

/**
 * Directive to introduce adding custom items to the carbon combo-box component
 * It takes the inputs of the combo-box and then append it with the searched item.
 * Then it emits the updatedItems back to the <cds-combobox> element
 */
@Directive({
  selector: '[cdDynamicInputCombobox]'
})
export class DynamicInputComboboxDirective implements OnInit, OnDestroy {
  @Input() items: ComboBoxItem[];

  /**
   * This will hold the items of the combo-box appended with the searched items.
   */
  @Output() updatedItems: EventEmitter<ComboBoxItem[]> = new EventEmitter();

  private searchSubscription: Subscription;
  private searchSubject: Subject<string> = new Subject();
  private selectedItems: ComboBoxItem[] = [];

  constructor(private combBoxService: ComboBoxService) {}

  ngOnInit() {
    this.searchSubscription = this.searchSubject
      .pipe(debounceTime(DEBOUNCE_TIMER), distinctUntilChanged())
      .subscribe((searchString) => {
        // Already selected items should be selected in the dropdown
        // even if the items are updated again
        this.items = this.items.map((item: ComboBoxItem) => {
          const selected = this.selectedItems.some(
            (selectedItem: ComboBoxItem) => selectedItem.content === item.content
          );
          return { ...item, selected };
        });

        const exists = this.items.some((item: ComboBoxItem) => item.content === searchString);

        if (!exists) {
          this.items = this.items.concat({ content: searchString, name: searchString });
        }
        this.updatedItems.emit(this.items);
        this.combBoxService.emit({ searchString });
      });
  }

  @HostListener('search', ['$event'])
  onInput(event: string) {
    if (event.length > 1) this.searchSubject.next(event);
  }

  @HostListener('selected', ['$event'])
  onSelected(event: ComboBoxItem[]) {
    this.selectedItems = event;
  }

  ngOnDestroy() {
    this.searchSubscription.unsubscribe();
    this.searchSubject.complete();
  }
}
