import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class ComboBoxService {
  private searchSubject = new Subject<any>();

  constructor() {
  }

  emit(value: any) {
    this.searchSubject.next(value);
  }

  subscribe<T>(handler: (value: T) => void, debounceMs = 300) {
    this.searchSubject
      .pipe(
        debounceTime(debounceMs),
        distinctUntilChanged((prev, curr) => JSON.stringify(prev) === JSON.stringify(curr))
      )
      .subscribe(handler);
  }
}
