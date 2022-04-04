import { EventEmitter, Injectable, Output } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class TelemetryNotificationService {
  @Output()
  update: EventEmitter<boolean> = new EventEmitter<boolean>();

  visible = false;

  setVisibility(visible: boolean) {
    this.visible = visible;
    this.update.emit(visible);
  }
}
