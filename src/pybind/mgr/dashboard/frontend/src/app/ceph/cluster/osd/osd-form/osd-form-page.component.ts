import { Component, ViewChild } from '@angular/core';

import { Step } from 'carbon-components-angular';

import { OsdFormComponent } from './osd-form.component';

@Component({
  selector: 'cd-osd-form-page',
  templateUrl: './osd-form-page.component.html',
  standalone: false
})
export class OsdFormPageComponent {
  @ViewChild(OsdFormComponent)
  osdForm!: OsdFormComponent;

  title: string = $localize`Create OSDs`;
  description: string = $localize`Configure deployment mode, shared devices, and optional features for new OSD creation.`;
  steps: Step[] = [{ label: $localize`Configuration`, invalid: false }];

  onSubmitRequested() {
    this.osdForm?.submit();
  }
}
