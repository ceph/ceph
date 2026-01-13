import { Component } from '@angular/core';
import { GridModule, TilesModule } from 'carbon-components-angular';

@Component({
  selector: 'cd-overview',
  imports: [GridModule, TilesModule],
  standalone: true,
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss'
})
export class OverviewComponent {}
