import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { DropdownModule, ListItem, LayerModule, LayoutModule } from 'carbon-components-angular';

@Component({
  selector: 'app-time-picker',
  standalone: true,
  imports: [DropdownModule, LayerModule, LayoutModule],
  templateUrl: './time-picker.component.html',
  styleUrl: './time-picker.component.scss'
})
export class TimePickerComponent implements OnInit {
  @Output() selectedTime = new EventEmitter<{ start: number; end: number; step: number }>();

  private timeRanges = [
    { label: 'Last 5 minutes', minutes: 5, step: 1 },
    { label: 'Last 30 minutes', minutes: 30, step: 7 },
    { label: 'Last 1 hour', minutes: 60, step: 14 },
    { label: 'Last 6 hours', minutes: 360, step: 86 },
    { label: 'Last 24 hours', minutes: 1440, step: 345 },
    { label: 'Last 7 days', minutes: 10080, step: 2419 },
    { label: 'Last 30 days', minutes: 43200, step: 10368 }
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
