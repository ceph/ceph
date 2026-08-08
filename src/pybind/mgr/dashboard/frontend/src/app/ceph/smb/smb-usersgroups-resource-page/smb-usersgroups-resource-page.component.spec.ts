import { ComponentFixture, TestBed } from '@angular/core/testing';
import { SmbUsersgroupsResourcePageComponent } from './smb-usersgroups-resource-page.component';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { SmbService } from '~/app/shared/api/smb.service';
import { BehaviorSubject, of, throwError } from 'rxjs';
import { SMBUsersGroups } from '../smb.model';
import { Component, Input } from '@angular/core';

// Mock child components to prevent TestBed errors
@Component({ selector: 'cd-resource-overview-card', template: '', standalone: false })
class MockResourceOverviewCardComponent {
  @Input() title: string;
  @Input() columns: number;
  @Input() fields: any[];
}

@Component({ selector: 'cd-table', template: '', standalone: false })
class MockTableComponent {
  @Input() data: any[];
  @Input() columnMode: string;
  @Input() columns: any[];
  @Input() selectionType: string;
  @Input() hasDetails: boolean;
}

@Component({ selector: 'cd-alert-panel', template: '', standalone: false })
class MockAlertPanelComponent {
  @Input() type: string;
}

describe('SmbUsersgroupsResourcePageComponent', () => {
  let component: SmbUsersgroupsResourcePageComponent;
  let fixture: ComponentFixture<SmbUsersgroupsResourcePageComponent>;
  let smbServiceMock: any;
  let paramMapSubject: BehaviorSubject<any>;

  const mockUsersGroupsData: SMBUsersGroups = {
    users_groups_id: 'standalone-1',
    linked_to_cluster: 'true',
    values: {
      users: [
        { name: 'user1', password: 'pass1' },
        { name: 'user2', password: 'pass2' }
      ],
      groups: [{ name: 'groupA' }, { name: 'groupB' }]
    },
    resource_type: 'smb_users_groups'
  };

  beforeEach(async () => {
    smbServiceMock = {
      getUsersGroups: jest.fn().mockReturnValue(of(mockUsersGroupsData))
    };

    paramMapSubject = new BehaviorSubject(convertToParamMap({ users_groups_id: 'standalone-1' }));

    const activatedRouteMock = {
      snapshot: { data: { section: 'overview' } },
      parent: { paramMap: paramMapSubject.asObservable() }
    };

    await TestBed.configureTestingModule({
      declarations: [
        SmbUsersgroupsResourcePageComponent,
        MockResourceOverviewCardComponent,
        MockTableComponent,
        MockAlertPanelComponent
      ],
      providers: [
        { provide: SmbService, useValue: smbServiceMock },
        { provide: ActivatedRoute, useValue: activatedRouteMock }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbUsersgroupsResourcePageComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('ngOnInit', () => {
    it('should setup initial state and columns', () => {
      fixture.detectChanges();

      expect(component.section).toBe('overview');
      expect(component.columns.length).toBe(1);
      expect(component.columns[0].prop).toBe('name');
    });

    it('should fetch users groups data and map overview fields on success', () => {
      fixture.detectChanges(); // Triggers ngOnInit

      expect(smbServiceMock.getUsersGroups).toHaveBeenCalledWith('standalone-1');
      expect(component.notFound).toBe(false);
      expect(component.selection).toEqual(mockUsersGroupsData);

      // Verify OverviewFields mapping
      expect(component.overviewFields.length).toBe(4);
      expect(component.overviewFields[0].value).toBe('standalone-1'); // ID
      expect(component.overviewFields[1].value).toBe(2); // Number of users
      expect(component.overviewFields[2].values).toEqual(['groupA', 'groupB']); // Groups mapped to tags
      expect(component.overviewFields[2].type).toBe('tags');
      expect(component.overviewFields[3].value).toBe('true'); // Linked to cluster
    });

    it('should gracefully handle missing users_groups_id in route parameters', () => {
      // Push empty parameter
      paramMapSubject.next(convertToParamMap({}));
      fixture.detectChanges();

      expect(smbServiceMock.getUsersGroups).not.toHaveBeenCalled();
      expect(component.notFound).toBe(true);
      expect(component.selection).toBeUndefined();
      expect(component.overviewFields).toEqual([]);
    });

    it('should gracefully handle API errors', () => {
      smbServiceMock.getUsersGroups.mockReturnValue(throwError(() => new Error('API Error')));

      fixture.detectChanges();

      expect(smbServiceMock.getUsersGroups).toHaveBeenCalledWith('standalone-1');
      expect(component.notFound).toBe(true);
      expect(component.selection).toBeUndefined();
      expect(component.overviewFields).toEqual([]);
    });
  });

  describe('Edge cases in buildOverviewFields', () => {
    it('should handle null/missing values gracefully', () => {
      const edgeCaseData = {
        users_groups_id: 'empty-standalone',
        linked_to_cluster: false,
        values: null // Values is null
      } as unknown as SMBUsersGroups;

      smbServiceMock.getUsersGroups.mockReturnValue(of(edgeCaseData));
      fixture.detectChanges();

      expect(component.overviewFields[1].value).toBe(0); // Number of users should fallback to 0
      expect(component.overviewFields[2].values).toEqual([]); // Groups should fallback to empty array
    });
  });

  describe('ngOnDestroy', () => {
    it('should unsubscribe from observables', () => {
      const unsubscribeSpy = jest.spyOn((component as any).sub, 'unsubscribe');

      fixture.detectChanges();
      component.ngOnDestroy();

      expect(unsubscribeSpy).toHaveBeenCalled();
    });
  });
});
