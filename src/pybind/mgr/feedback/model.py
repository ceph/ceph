# # -*- coding: utf-8 -*-
from enum import Enum


class Feedback:
    project_id: int
    tracker_id: int
    subject: str
    description: str
    status: int

    class Project(Enum):
        dashboard = 46
        block = 9  # rbd
        object = 10  # rgw
        file_system = 13  # cephfs
        ceph_manager = 46
        orchestrator = 42
        ceph_volume = 39
        core_ceph = 36  # rados

    class TrackerType(Enum):
        bug = 1
        feature = 2

    class Status(Enum):
        new = 1

    def __init__(self, project_id, tracker_id, subject, description):
        self.project_id = int(project_id)
        self.tracker_id = int(tracker_id)
        self.subject = subject
        self.description = description
        self.status = Feedback.Status.new.value

    def as_dict(self):
        return {
            "issue": {
                "project": {
                    "id": self.project_id
                },
                "tracker_id": self.tracker_id,
                "Status": self.status,
                "subject": self.subject,
                "description": self.description
            }
        }
