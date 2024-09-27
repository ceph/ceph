import { Directive, Input, OnDestroy, OnInit, Output, EventEmitter, HostListener } from '@angular/core';
import { ComboBoxItem } from '../models/combo-box.model';
import { ComboBoxService } from '../services/combo-box.service';
import { Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged } from 'rxjs/operators';

@Directive({
  selector: '[cdDynamicInputCombobox]'
})
export class DynamicInputComboboxDirective implements OnInit, OnDestroy{
  @Input() items: ComboBoxItem[];

  @Output() updatedItems: EventEmitter<ComboBoxItem[]> = new EventEmitter();

  private searchSubject: Subject<string> = new Subject();
  private selectedItems: ComboBoxItem[] = [];

  constructor(
    private combBoxService: ComboBoxService
  ) { }

  ngOnInit() {
    this.searchSubject
      .pipe(
        debounceTime(300),
        distinctUntilChanged()
      )
      .subscribe((searchString) => {
        // Already selected items should be selected in the dropdown
        // even if the items are updated again
        this.items = this.items.map((item) => {
          const selected = this.selectedItems.some(
            (selectedItem) => selectedItem.content === item.content
          );
          return { ...item, selected };
        });

        const exists = this.items.some(
          (item) => item.content === searchString
        );
      
        if (!exists) {
          this.items = this.items.concat({ content: searchString, name: searchString });
        }
        this.updatedItems.emit(this.items );
        this.combBoxService.emit({ searchString });
      });
  }

  @HostListener('search', ['$event'])
  onInput(event: any) {
    if (event.length > 1) this.searchSubject.next(event);
  }

  @HostListener('selected', ['$event'])
  onSelected(event: ComboBoxItem[]) {
    this.selectedItems = event;
  }

  ngOnDestroy() {
    this.searchSubject.complete();
  }
}
