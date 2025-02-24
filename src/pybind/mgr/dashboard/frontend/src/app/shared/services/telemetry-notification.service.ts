import { EventEmitter, Injectable, Output } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class TelemetryNotificationService {
  visible = false;

  @Output()
  update: EventEmitter<boolean> = new EventEmitter<boolean>();

  setVisibility(visible: boolean) {
    this.visible = visible;
    this.update.emit(visible);
  }
}
