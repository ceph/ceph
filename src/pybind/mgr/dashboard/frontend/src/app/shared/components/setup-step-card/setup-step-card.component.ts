import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ComponentsModule } from '../components.module';

@Component({
  selector: 'cd-setup-step-card',
  standalone: true,
  imports: [CommonModule, ComponentsModule],
  templateUrl: './setup-step-card.component.html',
  styleUrls: ['./setup-step-card.component.scss']
})
export class SetupStepCardComponent {
  @Input() stepNumber!: number;
  @Input() title!: string;
  @Input() description!: string;
  @Input() isConfigured: boolean = false;
  @Input() isLoading: boolean = false;
  @Input() successMessage?: string;
  @Input() infoMessage?: string;

  get statusMessage(): string {
    if (this.isLoading) {
      return $localize`Loading...`;
    }
    if (this.isConfigured && this.successMessage) {
      return this.successMessage;
    }
    if (!this.isConfigured && this.infoMessage) {
      return this.infoMessage;
    }
    return this.isConfigured ? $localize`Configured successfully.` : $localize`Not configured yet.`;
  }
}
