import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { DropdownModule, ListItem } from 'carbon-components-angular';

@Component({
  selector: 'cd-time-picker',
  standalone: true,
  templateUrl: './time-picker.component.html',
  styleUrl: './time-picker.component.scss',
  imports: [DropdownModule]
})
export class TimePickerComponent implements OnInit {
  @Output() selectedTime = new EventEmitter<{ start: number; end: number; step: number }>();
  @Input() dropdownSize?: 'sm' | 'md' | 'lg' = 'sm';
  @Input() label?: string = 'Time Span'; // Default to 'Last 1 hour'

  private timeRanges = [
    { label: $localize`Last 5 minutes`, minutes: 5, step: 1 },
    { label: $localize`Last 30 minutes`, minutes: 30, step: 7 },
    { label: $localize`Last 1 hour`, minutes: 60, step: 14 },
    { label: $localize`Last 6 hours`, minutes: 360, step: 86 },
    { label: $localize`Last 24 hours`, minutes: 1440, step: 345 },
    { label: $localize`Last 7 days`, minutes: 10080, step: 2419 },
    { label: $localize`Last 30 days`, minutes: 43200, step: 10368 }
  ];

  timeOptions: ListItem[] = this.timeRanges.map((range, index) => ({
    content: range.label,
    value: index,
    selected: index === 2 // Default to 'Last 1 hour'
  }));

  ngOnInit(): void {
    const defaultOption = this.timeOptions.find((option) => option.selected);
    if (defaultOption) {
      this.emitTime(defaultOption['value']);
    }
  }

  onTimeSelected(event: object): void {
    this.emitTime((event as { item: ListItem }).item['value']);
  }

  private emitTime(index: number): void {
    const now = Math.floor(Date.now() / 1000);
    const selectedRange = this.timeRanges[index];
    const start = now - selectedRange.minutes * 60;

    this.selectedTime.emit({
      start,
      end: now,
      step: selectedRange.step
    });
  }
}
