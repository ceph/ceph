import { Component, Input } from '@angular/core';

import { OrchestratorFeature } from '../../models/orchestrator.enum';

@Component({
  selector: 'cd-orchestrator-doc-panel',
  templateUrl: './orchestrator-doc-panel.component.html',
  styleUrls: ['./orchestrator-doc-panel.component.scss']
})
export class OrchestratorDocPanelComponent {
  @Input()
  missingFeatures: OrchestratorFeature[];
}
