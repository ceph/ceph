// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <initializer_list>

namespace rgw::dbstore::config::schema {

struct Migration {
  // human-readable description to help with debugging migration errors
  const char* description = nullptr;
  // series of sql statements to apply the schema migration
  const char* up = nullptr;
  // series of sql statements to undo the schema migration
  const char* down = nullptr;
};

static constexpr std::initializer_list<Migration> migrations {{
    .description = "create the initial ConfigStore tables",
    .up = R"(
CREATE TABLE IF NOT EXISTS Realms (
  ID TEXT PRIMARY KEY NOT NULL,
  Name TEXT UNIQUE NOT NULL,
  CurrentPeriod TEXT,
  Epoch INTEGER DEFAULT 0,
  VersionNumber INTEGER,
  VersionTag TEXT
);
CREATE TABLE IF NOT EXISTS Periods (
  ID TEXT NOT NULL,
  Epoch INTEGER DEFAULT 0,
  RealmID TEXT NOT NULL REFERENCES Realms (ID),
  Data TEXT NOT NULL,
  PRIMARY KEY (ID, Epoch)
);
CREATE TABLE IF NOT EXISTS PeriodConfigs (
  RealmID TEXT PRIMARY KEY NOT NULL REFERENCES Realms (ID),
  Data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ZoneGroups (
  ID TEXT PRIMARY KEY NOT NULL,
  Name TEXT UNIQUE NOT NULL,
  RealmID TEXT REFERENCES Realms (ID),
  Data TEXT NOT NULL,
  VersionNumber INTEGER,
  VersionTag TEXT
);
CREATE TABLE IF NOT EXISTS Zones (
  ID TEXT PRIMARY KEY NOT NULL,
  Name TEXT UNIQUE NOT NULL,
  RealmID TEXT REFERENCES Realms (ID),
  Data TEXT NOT NULL,
  VersionNumber INTEGER,
  VersionTag TEXT
);
CREATE TABLE IF NOT EXISTS DefaultRealms (
  ID TEXT,
  Empty TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS DefaultZoneGroups (
  ID TEXT,
  RealmID TEXT PRIMARY KEY REFERENCES Realms (ID)
);
CREATE TABLE IF NOT EXISTS DefaultZones (
  ID TEXT,
  RealmID TEXT PRIMARY KEY REFERENCES Realms (ID)
);
)",
    .down = R"(
DROP TABLE IF EXISTS Realms;
DROP TABLE IF EXISTS Periods;
DROP TABLE IF EXISTS PeriodConfigs;
DROP TABLE IF EXISTS ZoneGroups;
DROP TABLE IF EXISTS Zones;
DROP TABLE IF EXISTS DefaultRealms;
DROP TABLE IF EXISTS DefaultZoneGroups;
DROP TABLE IF EXISTS DefaultZones;
)"
  }
};


// DefaultRealms

static constexpr const char* default_realm_insert1 =
"INSERT INTO DefaultRealms (ID, Empty) VALUES ({}, '')";

static constexpr const char* default_realm_upsert1 =
R"(INSERT INTO DefaultRealms (ID, Empty) VALUES ({0}, '')
ON CONFLICT(Empty) DO UPDATE SET ID = {0})";

static constexpr const char* default_realm_select0 =
"SELECT ID FROM DefaultRealms LIMIT 1";

static constexpr const char* default_realm_delete0 =
"DELETE FROM DefaultRealms";


// Realms

static constexpr const char* realm_update5 =
"UPDATE Realms SET CurrentPeriod = {1}, Epoch = {2}, VersionNumber = {3} + 1 \
WHERE ID = {0} AND VersionNumber = {3} AND VersionTag = {4}";

static constexpr const char* realm_rename4 =
"UPDATE Realms SET Name = {1}, VersionNumber = {2} + 1 \
WHERE ID = {0} AND VersionNumber = {2} AND VersionTag = {3}";

static constexpr const char* realm_delete3 =
"DELETE FROM Realms WHERE ID = {} AND VersionNumber = {} AND VersionTag = {}";

static constexpr const char* realm_insert4 =
"INSERT INTO Realms (ID, Name, VersionNumber, VersionTag) \
VALUES ({}, {}, {}, {})";

static constexpr const char* realm_upsert4 =
"INSERT INTO Realms (ID, Name, VersionNumber, VersionTag) \
VALUES ({0}, {1}, {2}, {3}) \
ON CONFLICT(ID) DO UPDATE SET Name = {1}, \
VersionNumber = {2}, VersionTag = {3}";

static constexpr const char* realm_select_id1 =
"SELECT * FROM Realms WHERE ID = {} LIMIT 1";

static constexpr const char* realm_select_name1 =
"SELECT * FROM Realms WHERE Name = {} LIMIT 1";

static constexpr const char* realm_select_default0 =
"SELECT r.* FROM Realms r \
INNER JOIN DefaultRealms d \
ON d.ID = r.ID LIMIT 1";

static constexpr const char* realm_select_names2 =
"SELECT Name FROM Realms WHERE Name > {} \
ORDER BY Name ASC LIMIT {}";


// Periods

static constexpr const char* period_insert4 =
"INSERT INTO Periods (ID, Epoch, RealmID, Data) \
VALUES ({}, {}, {}, {})";

static constexpr const char* period_upsert4 =
"INSERT INTO Periods (ID, Epoch, RealmID, Data) \
VALUES ({0}, {1}, {2}, {3}) \
ON CONFLICT DO UPDATE SET RealmID = {2}, Data = {3}";

static constexpr const char* period_select_epoch2 =
"SELECT * FROM Periods WHERE ID = {} AND Epoch = {} LIMIT 1";

static constexpr const char* period_select_latest1 =
"SELECT * FROM Periods WHERE ID = {} ORDER BY Epoch DESC LIMIT 1";

static constexpr const char* period_delete1 =
"DELETE FROM Periods WHERE ID = {}";

static constexpr const char* period_select_ids2 =
"SELECT ID FROM Periods WHERE ID > {} ORDER BY ID ASC LIMIT {}";


// DefaultZoneGroups

static constexpr const char* default_zonegroup_insert2 =
"INSERT INTO DefaultZoneGroups (RealmID, ID) VALUES ({}, {})";

static constexpr const char* default_zonegroup_upsert2 =
"INSERT INTO DefaultZoneGroups (RealmID, ID) \
VALUES ({0}, {1}) \
ON CONFLICT(RealmID) DO UPDATE SET ID = {1}";

static constexpr const char* default_zonegroup_select1 =
"SELECT ID FROM DefaultZoneGroups WHERE RealmID = {}";

static constexpr const char* default_zonegroup_delete1 =
"DELETE FROM DefaultZoneGroups WHERE RealmID = {}";


// ZoneGroups

static constexpr const char* zonegroup_update5 =
"UPDATE ZoneGroups SET RealmID = {1}, Data = {2}, VersionNumber = {3} + 1 \
WHERE ID = {0} AND VersionNumber = {3} AND VersionTag = {4}";

static constexpr const char* zonegroup_rename4 =
"UPDATE ZoneGroups SET Name = {1}, VersionNumber = {2} + 1 \
WHERE ID = {0} AND VersionNumber = {2} AND VersionTag = {3}";

static constexpr const char* zonegroup_delete3 =
"DELETE FROM ZoneGroups WHERE ID = {} \
AND VersionNumber = {} AND VersionTag = {}";

static constexpr const char* zonegroup_insert6 =
"INSERT INTO ZoneGroups (ID, Name, RealmID, Data, VersionNumber, VersionTag) \
VALUES ({}, {}, {}, {}, {}, {})";

static constexpr const char* zonegroup_upsert6 =
"INSERT INTO ZoneGroups (ID, Name, RealmID, Data, VersionNumber, VersionTag) \
VALUES ({0}, {1}, {2}, {3}, {4}, {5}) \
ON CONFLICT (ID) DO UPDATE SET Name = {1}, RealmID = {2}, \
Data = {3}, VersionNumber = {4}, VersionTag = {5}";

static constexpr const char* zonegroup_select_id1 =
"SELECT * FROM ZoneGroups WHERE ID = {} LIMIT 1";

static constexpr const char* zonegroup_select_name1 =
"SELECT * FROM ZoneGroups WHERE Name = {} LIMIT 1";

static constexpr const char* zonegroup_select_default0 =
"SELECT z.* FROM ZoneGroups z \
INNER JOIN DefaultZoneGroups d \
ON d.ID = z.ID LIMIT 1";

static constexpr const char* zonegroup_select_names2 =
"SELECT Name FROM ZoneGroups WHERE Name > {} \
ORDER BY Name ASC LIMIT {}";


// DefaultZones

static constexpr const char* default_zone_insert2 =
"INSERT INTO DefaultZones (RealmID, ID) VALUES ({}, {})";

static constexpr const char* default_zone_upsert2 =
"INSERT INTO DefaultZones (RealmID, ID) VALUES ({0}, {1}) \
ON CONFLICT(RealmID) DO UPDATE SET ID = {1}";

static constexpr const char* default_zone_select1 =
"SELECT ID FROM DefaultZones WHERE RealmID = {}";

static constexpr const char* default_zone_delete1 =
"DELETE FROM DefaultZones WHERE RealmID = {}";


// Zones

static constexpr const char* zone_update5 =
"UPDATE Zones SET RealmID = {1}, Data = {2}, VersionNumber = {3} + 1 \
WHERE ID = {0} AND VersionNumber = {3} AND VersionTag = {4}";

static constexpr const char* zone_rename4 =
"UPDATE Zones SET Name = {1}, VersionNumber = {2} + 1 \
WHERE ID = {0} AND VersionNumber = {2} AND VersionTag = {3}";

static constexpr const char* zone_delete3 =
"DELETE FROM Zones WHERE ID = {} AND VersionNumber = {} AND VersionTag = {}";

static constexpr const char* zone_insert6 =
"INSERT INTO Zones (ID, Name, RealmID, Data, VersionNumber, VersionTag) \
VALUES ({}, {}, {}, {}, {}, {})";

static constexpr const char* zone_upsert6 =
"INSERT INTO Zones (ID, Name, RealmID, Data, VersionNumber, VersionTag) \
VALUES ({0}, {1}, {2}, {3}, {4}, {5}) \
ON CONFLICT (ID) DO UPDATE SET Name = {1}, RealmID = {2}, \
Data = {3}, VersionNumber = {4}, VersionTag = {5}";

static constexpr const char* zone_select_id1 =
"SELECT * FROM Zones WHERE ID = {} LIMIT 1";

static constexpr const char* zone_select_name1 =
"SELECT * FROM Zones WHERE Name = {} LIMIT 1";

static constexpr const char* zone_select_default0 =
"SELECT z.* FROM Zones z \
INNER JOIN DefaultZones d \
ON d.ID = z.ID LIMIT 1";

static constexpr const char* zone_select_names2 =
"SELECT Name FROM Zones WHERE Name > {} \
ORDER BY Name ASC LIMIT {}";


// PeriodConfigs

static constexpr const char* period_config_insert2 =
"INSERT INTO PeriodConfigs (RealmID, Data) VALUES ({}, {})";

static constexpr const char* period_config_upsert2 =
"INSERT INTO PeriodConfigs (RealmID, Data) VALUES ({0}, {1}) \
ON CONFLICT (RealmID) DO UPDATE SET Data = {1}";

static constexpr const char* period_config_select1 =
"SELECT Data FROM PeriodConfigs WHERE RealmID = {} LIMIT 1";

} // namespace rgw::dbstore::config::schema
