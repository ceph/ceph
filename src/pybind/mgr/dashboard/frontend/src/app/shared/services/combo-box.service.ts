import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ComboBoxService {
  private searchSubject = new Subject<{ searchString: string }>();

  constructor() {}

  emit(value: { searchString: string }) {
    this.searchSubject.next(value);
  }
}
