export class PrometheusSilenceMatcher {
  name: string;
  value: any;
  isRegex: boolean;
}

export class PrometheusSilence {
  id: string;
  matchers: PrometheusSilenceMatcher[];
  startsAt: string; // DateStr
  endsAt: string; // DateStr
  updatedAt?: string; // DateStr
  createdBy: string;
  comment: string;
  status?: {
    state: 'expired' | 'active' | 'pending';
  };
}
