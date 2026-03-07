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

    def test_qos_read_bandwidth_limit_integer(self, smb_cfg, config):
        """Test read bandwidth rate limiting with integer bytes as string."""
        read_bw_limit = "1048576"

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

    def test_qos_read_bandwidth_limit_human_readable(self, smb_cfg, config):
        """Test read bandwidth rate limiting with human-readable units."""
        test_cases = ["1M", "500K"]

        for human_readable in test_cases:
            updated_share = _update_qos(
                smb_cfg,
                config["cluster_id"],
                config["share_id"],
                read_bw_limit=human_readable,
            )

            assert updated_share["cephfs"]["qos"]["read_bw_limit"] == human_readable

            show_share = smbutil.get_share_by_id(
                smb_cfg, config["cluster_id"], config["share_id"]
            )
            assert show_share["cephfs"]["qos"]["read_bw_limit"] == human_readable

    def test_qos_write_bandwidth_limit_integer(self, smb_cfg, config):
        """Test write bandwidth rate limiting with integer bytes."""
        write_bw_limit = "2097152"  # 2 MiB

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

    def test_qos_write_bandwidth_limit_human_readable(self, smb_cfg, config):
        """Test write bandwidth rate limiting with human-readable units."""
        test_cases = ["1M", "750K"]

        for human_readable in test_cases:
            updated_share = _update_qos(
                smb_cfg,
                config["cluster_id"],
                config["share_id"],
                write_bw_limit=human_readable,
            )

            assert updated_share["cephfs"]["qos"]["write_bw_limit"] == human_readable

            show_share = smbutil.get_share_by_id(
                smb_cfg, config["cluster_id"], config["share_id"]
            )
            assert show_share["cephfs"]["qos"]["write_bw_limit"] == human_readable

    def test_qos_burst_multiplier_default(self, smb_cfg, config):
        """Test default burst multiplier (15 = 1.5x) when QoS is enabled."""
        # Enable QoS with only IOPS limit
        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=100,
        )

        qos = updated_share["cephfs"]["qos"]
        assert qos["read_iops_limit"] == 100
        assert qos.get("read_burst_mult") == 15  # Default value

    def test_qos_read_burst_multiplier(self, smb_cfg, config):
        """Test read burst multiplier."""
        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=100,
            read_burst_mult=20,  # 2x burst
        )

        qos = updated_share["cephfs"]["qos"]
        assert qos["read_iops_limit"] == 100
        assert qos["read_burst_mult"] == 20

        show_share = smbutil.get_share_by_id(
            smb_cfg, config["cluster_id"], config["share_id"]
        )
        assert show_share["cephfs"]["qos"]["read_burst_mult"] == 20

    def test_qos_write_burst_multiplier(self, smb_cfg, config):
        """Test write burst multiplier."""
        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            write_iops_limit=50,
            write_burst_mult=30,  # 3x burst
        )

        qos = updated_share["cephfs"]["qos"]
        assert qos["write_iops_limit"] == 50
        assert qos["write_burst_mult"] == 30

        show_share = smbutil.get_share_by_id(
            smb_cfg, config["cluster_id"], config["share_id"]
        )
        assert show_share["cephfs"]["qos"]["write_burst_mult"] == 30

    def test_qos_multiple_limits_with_burst(self, smb_cfg, config):
        """Test applying multiple QoS limits with burst multipliers."""
        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=100,
            write_iops_limit=50,
            read_bw_limit="10M",
            write_bw_limit="5M",
            read_burst_mult=20,  # 2x burst
            write_burst_mult=15,  # 1.5x burst
        )

        qos = updated_share["cephfs"]["qos"]
        assert qos["read_iops_limit"] == 100
        assert qos["write_iops_limit"] == 50
        assert qos["read_bw_limit"] == "10M"
        assert qos["write_bw_limit"] == "5M"
        assert qos["read_burst_mult"] == 20
        assert qos["write_burst_mult"] == 15

    def test_qos_limits_clamping(self, smb_cfg, config):
        """Test that QoS limits are properly clamped to maximum values."""
        excessive_iops = 2_000_000  # Above IOPS_LIMIT_MAX = 1,000,000
        excessive_bw = str(2 << 40)  # Above BYTES_LIMIT_MAX = 1 << 40 (1 TB)
        excessive_burst = 200  # Above max burst 100

        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=excessive_iops,
            write_iops_limit=excessive_iops,
            read_bw_limit=excessive_bw,
            write_bw_limit=excessive_bw,
            read_burst_mult=excessive_burst,
            write_burst_mult=excessive_burst,
        )

        qos = updated_share["cephfs"]["qos"]
        assert qos["read_iops_limit"] == 1_000_000  # IOPS_LIMIT_MAX
        assert qos["write_iops_limit"] == 1_000_000  # IOPS_LIMIT_MAX
        assert qos["read_bw_limit"] == str(1 << 40)  # BYTES_LIMIT_MAX (1 TB)
        assert qos["write_bw_limit"] == str(1 << 40)  # BYTES_LIMIT_MAX (1 TB)
        assert qos["read_burst_mult"] == 100  # Max burst
        assert qos["write_burst_mult"] == 100  # Max burst

    def test_qos_zero_values(self, smb_cfg, config):
        """Test that zero values are handled correctly."""
        # First set some QoS values
        _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=100,
            write_iops_limit=50,
        )

        # Now set all limits to 0 - should remove QoS entirely
        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=0,
            write_iops_limit=0,
            read_bw_limit="0",
            write_bw_limit="0",
        )

        # QoS should be completely removed
        assert "qos" not in updated_share["cephfs"]

    def test_qos_partial_zero_values(self, smb_cfg, config):
        """Test that setting some limits to zero disables only those limits."""
        # Set QoS with all limits
        _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=100,
            write_iops_limit=50,
            read_bw_limit="10M",
            write_bw_limit="5M",
        )

        # Disable only read limits
        updated_share = _update_qos(
            smb_cfg,
            config["cluster_id"],
            config["share_id"],
            read_iops_limit=0,
            read_bw_limit="0",
        )

        qos = updated_share["cephfs"]["qos"]
        # Read limits should be disabled (None)
        assert qos.get("read_iops_limit") is None
        assert qos.get("read_bw_limit") is None
        # Write limits should remain
        assert qos["write_iops_limit"] == 50
        assert qos["write_bw_limit"] == "5M"
        # Burst multipliers should remain with their defaults
        assert qos.get("read_burst_mult") == 15
        assert qos.get("write_burst_mult") == 15

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
            "read_bw_limit": "10M",
            "write_bw_limit": "5M",
            "read_burst_mult": 20,
            "write_burst_mult": 15,
        }

        updated_share = smbutil.apply_share_config(smb_cfg, share_resource)

        assert updated_share is not None
        assert "cephfs" in updated_share
        assert "qos" in updated_share["cephfs"]
        qos = updated_share["cephfs"]["qos"]
        assert qos["read_iops_limit"] == 300
        assert qos["write_iops_limit"] == 150
        assert qos["read_bw_limit"] == "10M"
        assert qos["write_bw_limit"] == "5M"
        assert qos["read_burst_mult"] == 20
        assert qos["write_burst_mult"] == 15
