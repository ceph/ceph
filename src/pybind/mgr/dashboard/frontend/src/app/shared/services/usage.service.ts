import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class UsageService {

  calculateUsedPercentage(used: number, total: number): number {
    return total > 0 ? (used / total) * 100 : 0;
  }

  getUsageInfo(used: number, total: number, warningThreshold: number, errorThreshold: number) {
    const usedPercentage = this.calculateUsedPercentage(used, total);
    let color = '#0043ce'; // Default color for 'Capacity'

    if (warningThreshold >= 0 && usedPercentage >= warningThreshold * 100) {
      color = '#f1c21b'; // Warning threshold exceeded
    }
    if (errorThreshold >= 0 && usedPercentage >= errorThreshold * 100) {
      color = '#da1e28'; // Error threshold exceeded
    }

    return { usedPercentage, color };
  }
}
