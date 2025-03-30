import { Injectable } from '@angular/core';
import { CanDeactivate } from '@angular/router';
import { Observable } from 'rxjs';

export interface ComponentCanDeactivate {
  canDeactivate: () => boolean | Observable<boolean>;
}

@Injectable({
  providedIn: 'root'
})
export class UnsavedChangesGuard implements CanDeactivate<ComponentCanDeactivate> {
  canDeactivate(
    component: ComponentCanDeactivate
  ): boolean | Observable<boolean> {
    if (!component) {
      console.warn('Component is null in UnsavedChangesGuard');
      return true;
    }

    if (typeof component.canDeactivate !== 'function') {
      console.warn('Component does not implement canDeactivate method');
      return true;
    }

    return component.canDeactivate();
  }
}