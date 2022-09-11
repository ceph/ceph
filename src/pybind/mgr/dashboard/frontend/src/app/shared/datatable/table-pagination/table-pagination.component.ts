import { Component, EventEmitter, Input, Output } from '@angular/core';

@Component({
  selector: 'cd-table-pagination',
  templateUrl: './table-pagination.component.html',
  styleUrls: ['./table-pagination.component.scss']
})
export class TablePaginationComponent {
  private _size = 0;
  private _count = 0;
  private _page = 1;
  pages: any;

  @Input()
  set size(value: number) {
    this._size = value;
    this.pages = this.calcPages();
  }

  get size(): number {
    return this._size;
  }

  @Input()
  set page(value: number) {
    this._page = value;
  }

  get page(): number {
    return this._page;
  }

  @Input()
  set count(value: number) {
    this._count = value;
  }

  get count(): number {
    return this._count;
  }

  get totalPages(): number {
    const count = this.size < 1 ? 1 : Math.ceil(this._count / this._size);
    return Math.max(count || 0, 1);
  }

  @Output() pageChange: EventEmitter<any> = new EventEmitter();

  canPrevious(): boolean {
    return this._page > 1;
  }

  canNext(): boolean {
    return this._page < this.totalPages;
  }

  prevPage(): void {
    this.selectPage(this._page - 1);
  }

  nextPage(): void {
    this.selectPage(this._page + 1);
  }

  selectPage(page: number): void {
    if (page > 0 && page <= this.totalPages && page !== this.page) {
      this._page = page;
      this.pageChange.emit({
        page
      });
    } else if (page > 0 && page >= this.totalPages) {
      this._page = this.totalPages;
      this.pageChange.emit({
        page: this.totalPages
      });
    }
  }

  calcPages(page?: number): any[] {
    const pages = [];
    let startPage = 1;
    let endPage = this.totalPages;
    const maxSize = 5;
    const isMaxSized = maxSize < this.totalPages;

    page = page || this.page;

    if (isMaxSized) {
      startPage = page - Math.floor(maxSize / 2);
      endPage = page + Math.floor(maxSize / 2);

      if (startPage < 1) {
        startPage = 1;
        endPage = Math.min(startPage + maxSize - 1, this.totalPages);
      } else if (endPage > this.totalPages) {
        startPage = Math.max(this.totalPages - maxSize + 1, 1);
        endPage = this.totalPages;
      }
    }

    for (let num = startPage; num <= endPage; num++) {
      pages.push({
        number: num,
        text: <string>(<any>num)
      });
    }

    return pages;
  }
}
