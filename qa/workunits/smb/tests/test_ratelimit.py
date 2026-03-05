import time
import pytest

import cephutil
import smbutil


def _update_qos(smb_cfg, cluster_id, share_id, **qos_params):
    """Update QoS settings for a share using the CLI command."""
    cmd = [
        "ceph",
        "smb",
        "share",
        "update",
        "cephfs",
        "qos",
        "--cluster-id",
        cluster_id,
        "--share-id",
        share_id,
    ]

    for param, value in qos_params.items():
        if value is not None:
            cmd.extend([f"--{param.replace('_', '-')}", str(value)])

    jres = cephutil.cephadm_shell_cmd(
        smb_cfg,
        cmd,
        load_json=True,
    )

    assert (
        jres.returncode == 0
    ), f"Command failed with return code {jres.returncode}"
    assert jres.obj, "No JSON response from command"
    assert jres.obj.get("success"), f"Command not successful: {jres.obj}"

    assert "resource" in jres.obj, f"Response missing 'resource' key: {jres.obj}"

    time.sleep(60)

    return jres.obj["resource"]


@pytest.mark.rate_limiting
class TestSMBRateLimiting:
    @pytest.fixture(scope="class")
    def config(self, smb_cfg):
        """Setup initial configuration with a test file."""
        orig = smbutil.get_shares(smb_cfg)[0]
        share_name = orig["name"]
        cluster_id = orig["cluster_id"]
        share_id = orig["share_id"]

        original_qos = None
        if "cephfs" in orig and "qos" in orig["cephfs"]:
            original_qos = orig["cephfs"]["qos"]

        test_filename = "test_ratelimit_data.bin"
        with smbutil.connection(smb_cfg, share_name) as sharep:
            test_data = b"X" * (1 * 1024 * 1024)
            file_path = sharep / test_filename
            file_path.write_bytes(test_data)

        yield {
            "share_name": share_name,
            "cluster_id": cluster_id,
            "share_id": share_id,
            "test_filename": test_filename,
            "original_share": orig,
            "original_qos": original_qos,
        }

        smbutil.apply_share_config(smb_cfg, orig)

        with smbutil.connection(smb_cfg, share_name) as sharep:
            (sharep / test_filename).unlink()

    def test_qos_read_iops_limit(self, smb_cfg, config):
        """Test read IOPS rate limiting."""
        updated_share = _update_qos(
            smb_cfg, config["cluster_id"], config["share_id"], read_iops_limit=100
        )

        assert updated_share is not None
        assert updated_share["cluster_id"] == config["cluster_id"]
        assert updated_share["share_id"] == config["share_id"]
        assert "cephfs" in updated_share
        assert "qos" in updated_share["cephfs"]
        assert updated_share["cephfs"]["qos"]["read_iops_limit"] == 100

        show_share = smbutil.get_share_by_id(
            smb_cfg, config["cluster_id"], config["share_id"]
        )
        assert show_share["cephfs"]["qos"]["read_iops_limit"] == 100

    def test_qos_write_iops_limit(self, smb_cfg, config):
        """Test write IOPS rate limiting."""
        updated_share = _update_qos(
            smb_cfg, config["cluster_id"], config["share_id"], write_iops_limit=50
        )

        assert updated_share is not None
        assert updated_share["cephfs"]["qos"]["write_iops_limit"] == 50

        show_share = smbutil.get_share_by_id(
            smb_cfg, config["cluster_id"], config["share_id"]
        )
        assert show_share["cephfs"]["qos"]["write_iops_limit"] == 50

    def test_qos_read_bandwidth_limit(self, smb_cfg, config):
        """Test read bandwidth rate limiting."""
        read_bw_limit = 1048576

        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_bw_limit=read_bw_limit,
        )

        assert updated_share["cephfs"]["qos"]["read_bw_limit"] == read_bw_limit

        show_share = smbutil.get_share_by_id(
            smb_cfg, config["cluster_id"], config["share_id"]
        )
        assert show_share["cephfs"]["qos"]["read_bw_limit"] == read_bw_limit

    def test_qos_write_bandwidth_limit(self, smb_cfg, config):
        """Test write bandwidth rate limiting."""
        write_bw_limit = 2097152

        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            write_bw_limit=write_bw_limit,
        )

        assert updated_share["cephfs"]["qos"]["write_bw_limit"] == write_bw_limit

        show_share = smbutil.get_share_by_id(
            smb_cfg, config["cluster_id"], config["share_id"]
        )
        assert show_share["cephfs"]["qos"]["write_bw_limit"] == write_bw_limit

    def test_qos_delay_max(self, smb_cfg, config):
        """Test delay_max settings."""
        read_delay_max = 100
        write_delay_max = 150
        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_delay_max=read_delay_max,
            write_delay_max=write_delay_max,
        )

        qos = updated_share["cephfs"]["qos"]
        assert qos["read_delay_max"] == read_delay_max
        assert qos["write_delay_max"] == write_delay_max

        show_share = smbutil.get_share_by_id(
            smb_cfg, config["cluster_id"], config["share_id"]
        )
        assert show_share["cephfs"]["qos"]["read_delay_max"] == read_delay_max
        assert show_share["cephfs"]["qos"]["write_delay_max"] == write_delay_max

    def test_qos_multiple_limits(self, smb_cfg, config):
        """Test applying multiple QoS limits simultaneously."""
        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=100,
            write_iops_limit=50,
            read_bw_limit=4194304,
            write_bw_limit=2097152,
            read_delay_max=100,
            write_delay_max=150,
        )

        qos = updated_share["cephfs"]["qos"]
        assert qos["read_iops_limit"] == 100
        assert qos["write_iops_limit"] == 50
        assert qos["read_bw_limit"] == 4194304
        assert qos["write_bw_limit"] == 2097152
        assert qos["read_delay_max"] == 100
        assert qos["write_delay_max"] == 150

    def test_qos_update_existing(self, smb_cfg, config):
        """Test updating existing QoS settings."""
        _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=100,
            read_bw_limit=1048576,
        )

        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=200,
            write_iops_limit=100,
            read_bw_limit=2097152,
        )

        qos = updated_share["cephfs"]["qos"]
        assert qos["read_iops_limit"] == 200
        assert qos["write_iops_limit"] == 100
        assert qos["read_bw_limit"] == 2097152

    def test_qos_limits_clamping(self, smb_cfg, config):
        """Test that QoS limits are properly clamped to maximum values."""
        excessive_iops = 2_000_000  # Above IOPS_LIMIT_MAX = 1,000,000
        excessive_bw = 2 << 40  # Above BYTES_LIMIT_MAX = 1 << 40 (1 TB)
        excessive_delay = 500  # Above DELAY_MAX_LIMIT = 300

        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=excessive_iops,
            write_iops_limit=excessive_iops,
            read_bw_limit=excessive_bw,
            write_bw_limit=excessive_bw,
            read_delay_max=excessive_delay,
            write_delay_max=excessive_delay,
        )

        qos = updated_share["cephfs"]["qos"]
        assert qos["read_iops_limit"] == 1_000_000  # IOPS_LIMIT_MAX
        assert qos["write_iops_limit"] == 1_000_000  # IOPS_LIMIT_MAX
        assert qos["read_bw_limit"] == 1 << 40  # BYTES_LIMIT_MAX (1 TB)
        assert qos["write_bw_limit"] == 1 << 40  # BYTES_LIMIT_MAX (1 TB)
        assert qos["read_delay_max"] == 300  # DELAY_MAX_LIMIT
        assert qos["write_delay_max"] == 300  # DELAY_MAX_LIMIT

    def test_qos_zero_values(self, smb_cfg, config):
        """Test that zero values are handled correctly (should be treated as no limit)."""
        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=0,
            write_iops_limit=0,
            read_bw_limit=0,
            write_bw_limit=0,
            read_delay_max=0,
            write_delay_max=0,
        )

        assert "qos" not in updated_share["cephfs"]

    def test_qos_apply_via_resources(self, smb_cfg, config):
        """Test applying QoS settings via the apply command with resources JSON."""
        current_share = smbutil.get_share_by_id(
            smb_cfg, config["cluster_id"], config["share_id"]
        )
        assert (
            current_share is not None
        ), f"Share {config['cluster_id']}/{config['share_id']} not found"

        share_resource = current_share.copy()

        assert "cephfs" in share_resource, "Share resource missing 'cephfs' key"

        share_resource["cephfs"]["qos"] = {
            "read_iops_limit": 300,
            "write_iops_limit": 150,
            "read_bw_limit": 3145728,  # 3 MB/s
            "write_bw_limit": 1572864,  # 1.5 MB/s
            "read_delay_max": 50,
            "write_delay_max": 75,
        }

        updated_share = smbutil.apply_share_config(smb_cfg, share_resource)

        assert updated_share is not None
        assert "cephfs" in updated_share
        assert "qos" in updated_share["cephfs"]
        qos = updated_share["cephfs"]["qos"]
        assert qos["read_iops_limit"] == 300
        assert qos["write_iops_limit"] == 150
        assert qos["read_bw_limit"] == 3145728
        assert qos["write_bw_limit"] == 1572864
        assert qos["read_delay_max"] == 50
        assert qos["write_delay_max"] == 75
