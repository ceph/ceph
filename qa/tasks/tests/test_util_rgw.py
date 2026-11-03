from tasks.util.rgw import (parse_s3_usage_xml, s3_usage_capacity_entries,
                            s3_usage_log_owners, s3_usage_log_users,
                            s3_usage_summary_successful_ops, s3_usage_total_bytes,
                            s3_usage_total_ops)

SAMPLE_USAGE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Usage>
  <Entries>
    <User>
      <Owner>foo</Owner>
      <Buckets>
        <Bucket>
          <Bucket>myfoo</Bucket>
          <categories>
            <Entry>
              <Category>put_obj</Category>
              <Ops>2</Ops>
              <SuccessfulOps>2</SuccessfulOps>
            </Entry>
            <Entry>
              <Category>delete_obj</Category>
              <Ops>1</Ops>
              <SuccessfulOps>1</SuccessfulOps>
            </Entry>
          </categories>
        </Bucket>
      </Buckets>
    </User>
  </Entries>
  <Summary>
    <User>
      <User>foo</User>
      <Total>
        <Ops>3</Ops>
        <SuccessfulOps>3</SuccessfulOps>
      </Total>
    </User>
    <TotalBytes>4096</TotalBytes>
    <TotalBytesRounded>4096</TotalBytesRounded>
    <TotalEntries>2</TotalEntries>
  </Summary>
  <CapacityUsed>
    <User>
      <Buckets>
        <Entry>
          <Bucket>myfoo</Bucket>
          <Bytes>4096</Bytes>
          <Bytes_Rounded>4096</Bytes_Rounded>
        </Entry>
      </Buckets>
    </User>
  </CapacityUsed>
</Usage>
"""


class TestS3UsageXml(object):

    def setup(self):
        self.root = parse_s3_usage_xml(SAMPLE_USAGE_XML)

    def test_capacity_entries(self):
        entries = s3_usage_capacity_entries(self.root)
        assert len(entries) == 1
        assert entries[0].find('Bucket').text == 'myfoo'

    def test_log_users_and_owners(self):
        assert len(s3_usage_log_users(self.root)) == 1
        assert s3_usage_log_owners(self.root) == ['foo']

    def test_total_ops(self):
        assert s3_usage_total_ops(self.root) == 3

    def test_summary_successful_ops(self):
        assert s3_usage_summary_successful_ops(self.root) == 3

    def test_total_bytes(self):
        assert s3_usage_total_bytes(self.root) == 4096
