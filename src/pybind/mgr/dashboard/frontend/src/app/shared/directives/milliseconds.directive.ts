import { Directive, EventEmitter, HostListener, Input, OnInit } from '@angular/core';
import { NgControl } from '@angular/forms';

import { FormatterService } from '../services/formatter.service';

@Directive({
  selector: '[cdMilliseconds]'
})
export class MillisecondsDirective implements OnInit {
  @Input()
  ngDataReady: EventEmitter<any>;

  constructor(private control: NgControl, private formatter: FormatterService) {}

  setValue(value: string): void {
    const ms = this.formatter.toMilliseconds(value);
    this.control.control.setValue(`${ms} ms`);
  }

  ngOnInit(): void {
    this.setValue(this.control.value);
    if (this.ngDataReady) {
      this.ngDataReady.subscribe(() => this.setValue(this.control.value));
    }
  }

  @HostListener('blur', ['$event.target.value'])
  onUpdate(value) {
    this.setValue(value);
  }
}
