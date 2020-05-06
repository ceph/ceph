export interface OrchestratorStatus {
  available: boolean;
  description: string;
  features: {
    [feature: string]: {
      available: boolean;
    };
  };
}
