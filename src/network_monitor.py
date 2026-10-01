"""
This script focuses on the network layer. In a smart grid environment, cyber threats
often manifest as anomalies in network traffic before they show up in the telemetry
itself.

This optimized build leverages kernel-level Berkeley Packet Filters (BPF) and monotonic
clocks to sustain packet stream ingest with minimal CPU overhead.
"""

import logging
import random
from scapy.all import sniff, IP, IPv6, TCP
from scapy.packet import Packet
import time
from typing import Optional, Any, Dict

# Import the production logging infrastructure
from config.logging_configs.mylogger import setup_production_logging

# ==============================================================================
# Logging Configuration
# ==============================================================================
logger: logging.Logger = logging.getLogger("smartgrid.network_monitor")

# ==============================================================================
# Configuration & Baselines
# ==============================================================================
TARGET_PORT: int = 19092
PACKET_CAPTURE_COUNT: int = 100  # Size of the micro-analysis batch

MAX_SAFE_PACKET_SIZE: int = 1500  # Standard MTU size
MIN_TIME_DELTA: float = 0.0001     # 0.1ms threshold for DoS floods


class StatefulPacketAnalyzer:
    """Handles stateful analysis of packet captures without global variable reliance."""

    def __init__(self, target_port: int) -> None:
        self.target_port: int = target_port
        # Use monotonic baseline time to safeguard against NTP adjustments
        self.last_packet_time: Optional[float] = None

    def __call__(self, packet: Packet) -> None:
        """Executed instantly by Scapy's capture thread for each matched packet."""

        # 1. Defensively verify layer boundaries and extract in one pass
        has_ip = IP in packet or IPv6 in packet
        if not (has_ip and packet.haslayer(TCP)):
            return

        # Make sure to account for IPv4 and IPv6 traffic sent to localhost
        ip_layer: Any = packet[IP] if IP in packet else packet[IPv6]
        tcp_layer: Any = packet[TCP]

        # 2. Performance metrics capture using non-drifting monotonic timers
        current_time: float = time.monotonic()
        packet_size: int = len(packet)

        # Calculate delta if a previous packet exists; otherwise, it's our first packet (0.0)
        time_delta: float = current_time - self.last_packet_time if self.last_packet_time else 0.0

        # Update the state tracker for the next arriving packet
        self.last_packet_time = current_time

        # 3. Extract core features (High-performance string conversion processing)
        # Using string representation of flags explicitly handles custom Scapy internal Flag types
        tcp_flags_str: str = str(tcp_layer.flags) if tcp_layer.flags else "None"

        #Inject anomalies to simulate possible attacks or issues
        inject_anomaly = random.random() < 0.005
        if inject_anomaly:
            if random.random() < 0.5:
                packet_size = MAX_SAFE_PACKET_SIZE + 1000
            else:
                time_delta = MIN_TIME_DELTA - 0.00005

        feature_set: Dict[str, Any] = {
            "src_ip": str(ip_layer.src),
            "dst_ip": str(ip_layer.dst),
            "src_port": int(tcp_layer.sport),
            "dst_port": int(tcp_layer.dport),
            "packet_size_bytes": packet_size,
            "tcp_flags": tcp_flags_str,
            "time_delta_seconds": round(time_delta, 6)
        }

        # 4. Anomaly Detection Engine
        is_anomaly: bool = False
        alert_reason: str = ""

        if packet_size > MAX_SAFE_PACKET_SIZE:
            is_anomaly = True
            alert_reason = "Oversized Payload Detected"
        elif 0 < time_delta < MIN_TIME_DELTA:
            is_anomaly = True
            alert_reason = "High-Frequency Traffic Flood (Possible DoS)"

        # 5. Pipeline Telemetry Logging Output using optimized runtime unpacking
        if is_anomaly:
            logger.warning(
                "[NETWORK ALERT] %s | From %s:%s | Size: %dB | Delta: %ss",
                alert_reason,
                feature_set["src_ip"],
                feature_set["src_port"],
                feature_set["packet_size_bytes"],
                feature_set["time_delta_seconds"]
            )
        else:
            logger.info(
                "[NetFlow Log] Ingested %dB packet | Flags: %s | Latency Delta: %ss",
                feature_set["packet_size_bytes"],
                feature_set["tcp_flags"],
                feature_set["time_delta_seconds"]
            )


def main() -> None:
    """Initializes and runs the kernel-filtered packet sniffer engine."""

    # Configure the main thread's local execution logger instance
    logger = logging.getLogger("smartgrid.network_monitor")

    logger.info("Starting Network Sniffer. Monitoring port %d...", TARGET_PORT)
    logger.info("Make sure your 'simulator.py' is running to generate active streaming traffic!")
    
    # Instantiate state tracker
    analyzer = StatefulPacketAnalyzer(target_port=TARGET_PORT)

    # Move target checking into the kernel BPF layer for speed optimization
    bpf_filter: str = f"tcp src port {TARGET_PORT} or tcp dst port {TARGET_PORT}"

    # explicitly tell Scapy to sniff on the loopback interface (loO on Linux/macOS: verify using ifconfig)
    # so it can intercept the localhost traffic
    try:
        sniff(
            filter=bpf_filter, 
            prn=analyzer, 
            count=PACKET_CAPTURE_COUNT,
            iface='lo0',
            store=0  # Prevents memory footprint allocation expansion
        )
        
        logger.info("Successfully captured and parsed a batch of %d packets.", PACKET_CAPTURE_COUNT)
        
    except PermissionError:
        logger.error("Permission Denied: Network sniffing requires administrative rights.")
        logger.error("Please rerun this script using: 'sudo python network_monitor.py'")
    except KeyboardInterrupt:
        logger.info("Sniffer manually stopped.")


if __name__ == "__main__":
    setup_production_logging()
    main()
