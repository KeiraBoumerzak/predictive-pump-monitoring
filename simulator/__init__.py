"""Virtual Asset Simulator and MQTT Communication Layer (Member 1).

Models three industrial motor-pump units (PUMP-01, PUMP-02, PUMP-03) publishing
unified telemetry, retained device status, and Last Will and Testament (LWT)
events over MQTT, with closed-loop maintenance recovery and fault injection.
"""

__version__ = "1.0.0"
