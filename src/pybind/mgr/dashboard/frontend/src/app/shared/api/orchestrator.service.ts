import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { Observable } from 'rxjs';

import { OrchestratorFeature } from '../models/orchestrator.enum';
import { OrchestratorStatus } from '../models/orchestrator.interface';

@Injectable({
  providedIn: 'root'
})
export class OrchestratorService {
  private url = 'ui-api/orchestrator';

  disableMessages = {
    noOrchestrator: $localize`The feature is disabled because Orchestrator is not available.`,
    missingFeature: $localize`The Orchestrator backend doesn't support this feature.`
  };

  constructor(private http: HttpClient) {}

  status(): Observable<OrchestratorStatus> {
    return this.http.get<OrchestratorStatus>(`${this.url}/status`);
  }

  hasFeature(status: OrchestratorStatus, features: OrchestratorFeature[]): boolean {
    return _.every(features, (feature) => _.get(status.features, `${feature}.available`));
  }

  getTableActionDisableDesc(
    status: OrchestratorStatus,
    features: OrchestratorFeature[]
  ): boolean | string {
    if (!status) {
      return false;
    }
    if (!status.available) {
      return this.disableMessages.noOrchestrator;
    }
    if (!this.hasFeature(status, features)) {
      return this.disableMessages.missingFeature;
    }
    return false;
  }

  getName() {
    return this.http.get(`${this.url}/get_name`);
  }
}
