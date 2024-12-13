import { Directive, EventEmitter, HostListener, Input, OnInit } from '@angular/core';
import { NgControl } from '@angular/forms';

import { FormatterService } from '../services/formatter.service';

@Directive({
  selector: '[cdIopm]'
})
export class IopmDirective implements OnInit {
  @Input()
  ngDataReady: EventEmitter<any>;

  constructor(private formatter: FormatterService, private ngControl: NgControl) {}

  setValue(value: string): void {
    if(value === '' || value === null){
      this.ngControl.control.setValue(value);
      return;
    }
    const iopm = this.formatter.toIopm(value);
    this.ngControl.control.setValue(`${iopm} IOPM`);
  }

  ngOnInit(): void {
    this.setValue(this.ngControl.value);
    if (this.ngDataReady) {
      this.ngDataReady.subscribe(() => this.setValue(this.ngControl.value));
    }
  }

  @HostListener('blur', ['$event.target.value'])
  onUpdate(value: string) {
    this.setValue(value);
  }
}
