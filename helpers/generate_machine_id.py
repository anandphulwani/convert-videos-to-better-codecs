import socket
import uuid
import zlib
import sys

def generate_machine_id():
    # Get IP address
    try:
        ip = socket.gethostbyname(socket.gethostname())
        ip_parts = ip.split('.')
        if len(ip_parts) >= 2:
            last_two = ip_parts[-2] + ip_parts[-1]  # e.g., 123.45 → 12345
        else:
            print("Failed to parse IP address correctly.")
            sys.exit(1)
    except Exception as e:
        print(f"Failed to get IP address: {e}")
        sys.exit(1)

    # Get MAC address and compute CRC32
    try:
        mac = uuid.getnode()
        if (mac >> 40) % 2:
            raise ValueError("Invalid MAC address retrieved (locally administered bit set).")
        mac_bytes = mac.to_bytes(6, byteorder='big')
        crc32_mac = format(zlib.crc32(mac_bytes) & 0xFFFFFFFF, '08x')
    except Exception as e:
        print(f"Failed to get MAC address: {e}")
        sys.exit(1)

    return f"{last_two}-{crc32_mac}"

