// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include "include/str_list.h"
#include "rgw_xml.h"


namespace rgw { namespace inv {

  enum class Format : uint8_t
  {
    None = 0,
    CSV,
    ORC,
    Parquet,
  };

  enum class Frequency : uint8_t
  {
    None = 0,
    Daily,
    Weekly,
  };

  enum class ObjectVersions : uint8_t
  {
    None = 0,
    All,
    Current,
  };

  enum class FieldType : uint8_t
  {
    None = 0,
    Size,
    LastModifiedDate,
    StorageClass,
    ETag,
    IsMultipartUploaded,
    ReplicationStatus,
    EncryptionStatus,
    ObjectLockRetainUntilDate,
    ObjectLockMode,
    ObjectLockLegalHoldStatus,
    IntelligentTieringAccessTier,
    BucketKeyStatus,
    Last = BucketKeyStatus,
  };

  class Field {
  public:
    FieldType ord;
    const char* name;

    constexpr Field(FieldType ord, const char* name) : ord(ord), name(name)
      {}
  };

  static constexpr std::array<Field, uint8_t(FieldType::Last)+1> field_table =
  {
    Field(FieldType::None, "None"),
    Field(FieldType::Size, "Size"),
    Field(FieldType::LastModifiedDate, "LastModifiedDate"),
    Field(FieldType::StorageClass, "StorageClass"),
    Field(FieldType::ETag, "ETag"),
    Field(FieldType::IsMultipartUploaded, "IsMultipartUploaded"),
    Field(FieldType::ReplicationStatus, "ReplicationStatus"),
    Field(FieldType::EncryptionStatus, "EncryptionStatus"),
    Field(FieldType::ObjectLockRetainUntilDate, "ObjectLockRetainUntilDate"),
    Field(FieldType::ObjectLockMode, "ObjectLockMode"),
    Field(FieldType::ObjectLockLegalHoldStatus, "ObjectLockLegalHoldStatus"),
    Field(FieldType::IntelligentTieringAccessTier,
	  "IntelligentTieringAccessTier"),
    Field(FieldType::BucketKeyStatus, "BucketKeyStatus"),
  };

  static constexpr uint32_t shift_field(FieldType type) {
    switch (type) {
    case FieldType::None:
      return 0;
      break;
    default:
      return 1 << (uint32_t(type) - 1);
    }
   }

   static inline const Field& find_field(const FieldType type) {
     if (type <= FieldType::Last) {
       return field_table[uint8_t(type)];
     }
     return field_table[0]; // FieldType::None
   }

   static inline const Field& find_field(const std::string& fs) {
     for (const auto& field : field_table) {
       if (fs == field.name) {
	 return field_table[uint8_t(field.ord)];
       }
     }
     // ok, so the None field
     return field_table[0];
   }

  class Configuration
  {
  public:
    std::string id; // unique identifier

    class Filter
    {
    public:
      std::string prefix; // the only defined filter, as yet
    } filter;

    class Destination
    {
    public:
      Format format;
      std::string account_id;
      std::string bucket_arn;
      std::string prefix;

      class Encryption
      {
      public:
	class KMS
	{
	public:
	  std::string key_id; // for SSE-KMS; SSE-S3 exists but is
			      // undefined
	} kms;
      } encryption;

      Destination() : format(Format::None)
	{}

    } destination;

    class Schedule
    {
    public:
      Frequency frequency;
      Schedule() : frequency(Frequency::None)
	{}

    } schedule;

    ObjectVersions versions;
    uint32_t optional_fields; // bitmap

    Configuration() :
      versions(ObjectVersions::None),
      optional_fields(uint32_t(FieldType::None))
      {}

    bool operator<(const Configuration &rhs) const;
    bool operator==(const Configuration &rhs) const;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(id, bl);
      encode(filter.prefix, bl);
      encode(destination.format, bl);
      encode(destination.account_id, bl);
      encode(destination.bucket_arn, bl);
      encode(destination.prefix, bl);
      encode(destination.encryption.kms.key_id, bl);
      encode(schedule.frequency, bl);
      encode(versions, bl);
      encode(optional_fields, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(id, bl);
      decode(filter.prefix, bl);
      decode(destination.format, bl);
      decode(destination.account_id, bl);
      decode(destination.bucket_arn, bl);
      decode(destination.prefix, bl);
      decode(destination.encryption.kms.key_id, bl);
      decode(schedule.frequency, bl);
      decode(versions, bl);
      decode(optional_fields, bl);
      DECODE_FINISH(bl);
    }

    void dump_xml(Formatter* f) const;
    void decode_xml(XMLObj* obj);
  }; /* Configuration */
  WRITE_CLASS_ENCODER(Configuration);

  class InventoryConfigurations
  {
  public:
    std::map<std::string, Configuration> id_mapping;

    void emplace(std::string&& key, Configuration&& config);
    bool operator==(const InventoryConfigurations &rhs) const;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(id_mapping, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(id_mapping, bl);
      DECODE_FINISH(bl);
    }
  };
  WRITE_CLASS_ENCODER(InventoryConfigurations);

}} /* namespace rgw::inv */
