import { Injectable } from '@angular/core';
import { CanDeactivate } from '@angular/router';
import { Observable } from 'rxjs';

export interface ComponentCanDeactivate {
  hasUnsavedChanges: () => boolean;
}

@Injectable({
  providedIn: 'root'
})
export class UnsavedChangesGuard implements CanDeactivate<ComponentCanDeactivate> {
  canDeactivate(
    component: ComponentCanDeactivate
  ): Observable<boolean> | Promise<boolean> | boolean {
    if (component.hasUnsavedChanges()) {
      return confirm($localize`You have unsaved changes. Are you sure you want to leave this page?`);
    }
    return true;
  }
}