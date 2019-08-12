import { Directive, EventEmitter, HostListener, Input, OnInit } from '@angular/core';
import { NgControl } from '@angular/forms';

import { FormatterService } from '../services/formatter.service';

@Directive({
  selector: '[cdIops]'
})
export class IopsDirective implements OnInit {
  @Input()
  ngDataReady: EventEmitter<any>;

  constructor(private formatter: FormatterService, private ngControl: NgControl) {}

  setValue(value: string): void {
    const iops = this.formatter.toIops(value);
    this.ngControl.control.setValue(`${iops} IOPS`);
  }

  ngOnInit(): void {
    this.setValue(this.ngControl.value);
    if (this.ngDataReady) {
      this.ngDataReady.subscribe(() => this.setValue(this.ngControl.value));
    }
  }

  @HostListener('blur', ['$event.target.value'])
  onUpdate(value) {
    this.setValue(value);
  }
}
