import { Component } from '@angular/core';
import { GridModule, TilesModule } from 'carbon-components-angular';
import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';

@Component({
  selector: 'cd-overview',
  imports: [GridModule, TilesModule, OverviewStorageCardComponent],
  standalone: true,
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss'
})
export class OverviewComponent {}
