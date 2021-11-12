export interface OrchestratorStatus {
  available: boolean;
  message: string;
  features: {
    [feature: string]: {
      available: boolean;
    };
  };
}
