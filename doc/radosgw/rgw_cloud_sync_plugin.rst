==================================
RGW sync plugin: cloud sync module
==================================

..versionadded::Luminous

Function
========
cloud sync module can sync bucket to the third party cloud storage.
Now it support Ufile of UCloud & OSS of alibaba cloud, but cloud module is scalable, 
so we can add the others cloud storge easily without adding new sync module.

Ufile, OSS sync:
rgw is incompatible with UFile&OSS api, so cloud interface only replicate bucket data only.
not support bucket version, acl, attr, and so on.


Configuration
=============

create master zone
--------------------------
  # radosgw-admin realm create --rgw-realm={realm-name} --default
  # radosgw-admin zonegroup create --rgw-zonegroup={name} --endpoints={url} --master --default

  # radosgw-admin zone create --rgw-zonegroup={zg_name} --rgw-zone={zone_name} --endpoints={url} --master --default --access-key={access} --secret={secret}
  # radosgw-admin user create --uid={user_name} --display-name={display_name} --access-key={access} --secret={secret} --system
  # radosgw-admin period update --commit


create cloud zone for ufile
---------------------------
  # radosgw-admin  zone create --rgw-zonegroup={zg_name} --rgw-zone={zone_name} --endpoints={url} --access-key={access} --secret={secret} --tier-type=cloud --tier-config=cloud_type=ufile,domain_name={region}.ufileos.com,public_key={ufile_public_key},private_key={ufile_priovate_key},prefix_bucket={prefix}, bucket_host=api.ucloud.cn

  # radosgw-admin period update --commit

create cloud zone for oss
--------------------------

  # radosgw-admin  zone create --rgw-zonegroup={zg_name} --rgw-zone={zone_name} --endpoints={url} --access-key={access} --secret={secret} --tier-type=cloud --tier-config=cloud_type=oss,domain_name={region}.aliyuncs.com,public_key={oss_public_key},private_key={oss_priovate_key},prefix_bucket={prefix}

  # radosgw-admin period update --commit


ufile tier-config
-----------------
  cloud_type     #[forcible] ufile cloud name: ufile (case insensitive)
  domain_name    #[forcible] address of the ufile cloud data
  public_key     #[forcible] ufile cloud public key
  private_key    #[forcible] ufile cloud private key
  bucket_host    #[forcible] address of the ufile cloud bucket: create the ufile bucket if the bucket no exist.
  prefix_bucket  #[optional] the prefix of ufile bucket name,reduce the ufile bucket names conflict.
  dest_bucket    #[optional] the dest bucket name in ufile.if not specify,the ufile bucket name is rgw bucket name.


oss tier-confg
  cloud_type     #[forcible] oss cloud name: oss (case insensitive)
  domain_name    #[forcible] address of the oss cloud data
  public_key     #[forcible] ufile cloud public key
  private_key    #[forcible] ufile cloud private key
  bucket_host    #[optional] same as domain_name
  prefix_bucket  #[optional] the prefix of ufile bucket name,reduce the ufile bucket names conflict.
  dest_bucket    #[optional] the dest bucket name in ufile.if not specify,the ufile bucket name is rgw bucket name.
