"""Fleet and schema configuration for the PulseGrid sensor simulator."""

DEVICE_TYPES = {
    "hvac_unit": {
        "count": 15,
        "fields": ["temperature_c", "humidity_pct"],
        "ranges": {
            "temperature_c": (18.0, 26.0),
            "humidity_pct": (30.0, 60.0),
        },
    },
    "motor": {
        "count": 15,
        "fields": ["vibration_mm_s", "rpm"],
        "ranges": {
            "vibration_mm_s": (0.5, 4.5),
            "rpm": (800, 3600),
        },
    },
    "cold_storage_unit": {
        "count": 15,
        "fields": ["temperature_c", "door_open_count"],
        "ranges": {
            "temperature_c": (-25.0, -10.0),
            "door_open_count": (0, 5),
        },
    },
    "smart_meter": {
        "count": 15,
        "fields": ["energy_kwh", "voltage"],
        "ranges": {
            "energy_kwh": (0.1, 15.0),
            "voltage": (215.0, 245.0),
        },
    },
}

ZONES = ["zone-a", "zone-b", "zone-c", "zone-d"]
FACILITIES = ["FAC-01", "FAC-02", "FAC-03"]
FIRMWARE_VERSIONS = ["1.0.0", "1.1.0", "1.2.3", "2.0.0"]

STATUS_WEIGHTS = {"OK": 0.90, "WARNING": 0.07, "FAULT": 0.03}

# Fraction of devices that skip reporting in a given hour (simulates offline devices)
DEVICE_DROPOUT_RATE = 0.02

# Fraction of records corrupted with bad data (nulls, out-of-range, duplicates)
BAD_DATA_RATE = 0.04
