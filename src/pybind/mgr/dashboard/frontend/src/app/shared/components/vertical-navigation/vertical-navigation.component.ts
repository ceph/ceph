import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';

@Component({
  selector: 'cd-vertical-navigation',
  templateUrl: './vertical-navigation.component.html',
  styleUrls: ['./vertical-navigation.component.scss']
})
export class VerticalNavigationComponent implements OnInit {
  @Input() items: string[];
  @Input() title: string;
  @Input() inputIdentifier: string;

  @Output() emitFilteredItems: EventEmitter<string[]> = new EventEmitter();
  @Output() emitActiveItem: EventEmitter<string> = new EventEmitter();

  activeItem = '';
  filteredItems: string[];

  ngOnInit(): void {
    this.filteredItems = this.items;
    if (!this.activeItem && this.items.length) this.selectItem(this.items[0]);
  }

  updateFilter() {
    const filterInput = document.getElementById(this.inputIdentifier) as HTMLInputElement;
    this.filteredItems = this.items.filter((item) => item.includes(filterInput.value));
  }

  selectItem(item = '') {
    this.activeItem = item;
    this.emitActiveItem.emit(item);
  }

  trackByFn(item: number) {
    return item;
  }
}
