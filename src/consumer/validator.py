from datetime import datetime

def validate(data):

    required_fields = [
        "machine_id",
        "timestamp",
        "power",
        "current",
        "temperature",
        "production_count",
        "status"
    ]

    # Check required fields
    for field in required_fields:
        if field not in data:
            return False

    # Type validation
    if not isinstance(data["machine_id"], str):
        return False

    if not isinstance(data["status"], int):
        return False

    # Range validation
    if data["temperature"] < -10 or data["temperature"] > 120:
        return False

    if data["power"] < 0 or data["power"] > 50:
        return False

    if data["current"] < 0 or data["current"] > 100:
        return False

    # Timestamp validation
    try:
        datetime.fromisoformat(data["timestamp"])
    except:
        return False

    return True