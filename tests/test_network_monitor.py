"""
Production-ready test suite for network_monitor.py.
Validates stateful packet telemetry processing, MTU size parsing anomalies,
high-frequency traffic floods (DoS), and kernel filter setups using pytest.
"""

import pytest
from typing import Generator
from unittest.mock import patch, MagicMock

# Import components directly from your network monitor script
from src.network_monitor import (
    StatefulPacketAnalyzer,
    main,
    TARGET_PORT,
    MAX_SAFE_PACKET_SIZE,
    MIN_TIME_DELTA
)


# ------------------------------------------------------------------------------
# Pytest Fixtures
# ------------------------------------------------------------------------------
@pytest.fixture
def mock_logger() -> Generator[MagicMock, None, None]:
    """Fixture patches the module-level logging system to audit runtime telemetry alerts."""
    with patch('src.network_monitor.logger') as mock_log:
        yield mock_log


def create_mock_packet(
    has_ip: bool = True, 
    has_tcp: bool = True, 
    src_ip: str = "192.168.1.50", 
    dst_ip: str = "192.168.1.100", 
    sport: int = 45322, 
    dport: int = 19092, 
    flags_str: str = "S", 
    raw_size: int = 64
) -> MagicMock:
    """
    Helper function to build a structured Scapy Packet mock instance matching
    expected internal layer structural configurations and string conversion behaviors.
    """
    packet = MagicMock()
    
    # Clean logic branch evaluation for haslayer queries
    packet.haslayer.side_effect = lambda layer: True if (layer.__name__ == 'IP' and has_ip) or (layer.__name__ == 'TCP' and has_tcp) else False

    # Mock Layer 3 IP properties
    mock_ip = MagicMock()
    mock_ip.src = src_ip
    mock_ip.dst = dst_ip

    # Mock Layer 4 TCP properties
    mock_tcp = MagicMock()
    mock_tcp.sport = sport
    mock_tcp.dport = dport

    # FIX: Explicitly mock Python's native __str__ casting hook
    # to perfectly handle the production script's str(tcp_layer.flags) extraction
    mock_flags = MagicMock()
    mock_flags.__str__.return_value = flags_str
    mock_tcp.flags = mock_flags

    # Dictionary mapping mock layers to packet array retrievals
    layer_map = {'IP': mock_ip, 'TCP': mock_tcp}
    packet.__getitem__.side_effect = lambda layer: layer_map[layer.__name__]

    # Control structural payload size dimensions evaluated by len()
    packet.__len__.return_value = raw_size

    return packet


# ------------------------------------------------------------------------------
# 1. Tests for StatefulPacketAnalyzer Core Engine
# ------------------------------------------------------------------------------
def test_analyzer_ignores_non_ip_or_tcp_traffic(mock_logger: MagicMock) -> None:
    """Verifies that packets lacking basic IP/TCP layers exit early without state changes or log side-effects."""
    analyzer = StatefulPacketAnalyzer(target_port=TARGET_PORT)

    # 1. Grab whatever timestamp the constructor generated automatically
    initial_saved_time = analyzer.last_packet_time

    # 2. Build a packet completely lacking networking layers
    malformed_packet = create_mock_packet(has_ip=False, has_tcp=False)

    # 3. Process the malformed packet
    analyzer(malformed_packet)

    # 4. Verify that the internal state timer was completely untouched by the escape logic
    assert analyzer.last_packet_time == initial_saved_time
    mock_logger.info.assert_not_called()
    mock_logger.warning.assert_not_called()


def test_analyzer_ingests_normal_flow_packet(mock_logger: MagicMock) -> None:
    """Validates structural extraction ranges and nominal telemetry log streaming outputs."""
    packet = create_mock_packet(raw_size=500, flags_str="A")

    # 1. Instantiate the tracker to capture an baseline startup time
    analyzer = StatefulPacketAnalyzer(target_port=TARGET_PORT)

    # 2. Patch time.monotonic precisely for ONLY the packet execution event pass
    with patch('time.monotonic', return_value=2000.0):
        analyzer(packet)
   
    # 3. Assert the time tracker accurately updated to the execution event timestamp
    assert analyzer.last_packet_time == 2000.0
    mock_logger.info.assert_called_once()

    # Flatten log history to a clean string format to protect against tuple traps
    log_messages_combined = "".join([str(call) for call in mock_logger.info.call_args_list])
    assert "[NetFlow Log] Ingested" in log_messages_combined
    assert "500" in log_messages_combined
    assert "A" in log_messages_combined


def test_analyzer_detects_oversized_payload_anomaly(mock_logger: MagicMock) -> None:
    """Forces an explicit MTU breach context to ensure oversized warning traps trip cleanly."""
    oversized_size = MAX_SAFE_PACKET_SIZE + 100
    packet = create_mock_packet(raw_size=oversized_size)

    with patch('time.monotonic', return_value=100.0):
        analyzer = StatefulPacketAnalyzer(target_port=TARGET_PORT)

    analyzer(packet)

    mock_logger.warning.assert_called_once()

    # Flatten warnings layout explicitly to prevent index mismatch fragility
    warning_logs_combined = "".join([str(call) for call in mock_logger.warning.call_args_list])
    assert "Oversized Payload Detected" in warning_logs_combined
    assert str(oversized_size) in warning_logs_combined


def test_analyzer_detects_high_frequency_dos_flood_anomaly(mock_logger: MagicMock) -> None:
    """Simulates multi-packet ingestion happening faster than thresholds to check DoS alarm triggers."""
    packet_1 = create_mock_packet()
    packet_2 = create_mock_packet()

    # 1. Spawn the analyzer instance using the baseline background system clock
    analyzer = StatefulPacketAnalyzer(target_port=TARGET_PORT)

    # 2. Control time.monotonic precisely for exactly two distinct packet execution events.
    # The difference between event 1 (1000.0) and event 2 (1000.00005) is exactly 0.00005 seconds.
    # This falls perfectly between 0.0 and 0.0001 (MIN_TIME_DELTA), triggering the anomaly path.
    with patch('time.monotonic', side_effect=[1000.0, 1000.00005]):
        analyzer(packet_1)  # Nominal log tracking pass (establishes the last_packet_time marker)
        analyzer(packet_2)  # High-frequency anomaly target check
   
    # 3. Assert the alert framework caught the high-frequency sequence
    mock_logger.warning.assert_called_once()
    warning_logs_combined = "".join([str(call) for call in mock_logger.warning.call_args_list])
    assert "High-Frequency Traffic Flood (Possible DoS)" in warning_logs_combined


# ==============================================================================
# 2. Tests for Main Application Setup
# ==============================================================================
def test_main_configures_kernel_bpf_filter_and_runs() -> None:
    """Ensures sniff targets are mapped directly to hardware optimization layers (BPF filters)."""
    with patch('src.network_monitor.sniff') as mock_sniff, \
         patch('src.network_monitor.setup_production_logging'):

        main()

        mock_sniff.assert_called_once()
        kwargs = mock_sniff.call_args.kwargs

        assert kwargs['filter'] == f"tcp src port {TARGET_PORT} or tcp dst port {TARGET_PORT}"
        assert kwargs['store'] == 0
        assert isinstance(kwargs['prn'], StatefulPacketAnalyzer)


def test_main_handles_permission_fault_gracefully(mock_logger: MagicMock) -> None:
    """Validates behavior when non-root permissions drop executions gracefully without unhandled trace crashes."""
    with patch('src.network_monitor.sniff', side_effect=PermissionError), \
         patch('src.network_monitor.setup_production_logging'):

        main()

        assert mock_logger.error.call_count == 2
        error_logs_combined = "".join([str(call) for call in mock_logger.error.call_args_list])
        assert "Permission Denied" in error_logs_combined
        assert "sudo python network_monitor.py" in error_logs_combined
