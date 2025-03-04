"""
Rig data simulator that generates realistic oil rig metrics.
"""
import time
import random
import json
from datetime import datetime
import numpy as np
from typing import Dict, Any

class RigSimulator:
    def __init__(self, rig_id: str):
        self.rig_id = rig_id
        self.base_pressure = random.uniform(2000, 3000)  # Base pressure in PSI
        self.base_temperature = random.uniform(150, 200)  # Base temperature in °F
        self.base_flow_rate = random.uniform(100, 200)    # Base flow rate in bbl/day
        self.latitude = random.uniform(-90, 90)
        self.longitude = random.uniform(-180, 180)
        
    def generate_metrics(self) -> Dict[str, Any]:
        """Generate realistic rig metrics with some random variation."""
        timestamp = datetime.utcnow().isoformat()
        
        # Generate base metrics with realistic variations
        pressure = self.base_pressure + random.gauss(0, 50)
        temperature = self.base_temperature + random.gauss(0, 5)
        flow_rate = self.base_flow_rate + random.gauss(0, 10)
        
        # Generate equipment status
        equipment_status = {
            "pump_status": random.choice(["running", "idle", "maintenance"]),
            "safety_system": random.choice(["active", "warning", "critical"]),
            "power_status": random.choice(["normal", "backup", "critical"]),
            "communication_status": random.choice(["excellent", "good", "fair", "poor"])
        }
        
        # Generate sensor readings
        sensor_readings = {
            "vibration": random.uniform(0, 5),
            "noise_level": random.uniform(60, 90),
            "emissions": random.uniform(0, 100),
            "fluid_level": random.uniform(80, 100)
        }
        
        # Generate maintenance metrics
        maintenance_metrics = {
            "hours_since_last_maintenance": random.uniform(0, 168),
            "maintenance_due": random.choice([True, False]),
            "parts_wear_level": random.uniform(0, 100)
        }
        
        # Generate environmental conditions
        environmental = {
            "wind_speed": random.uniform(0, 30),
            "wave_height": random.uniform(0, 5),
            "visibility": random.uniform(0, 10),
            "sea_temperature": random.uniform(40, 80)
        }
        
        # Combine all metrics
        metrics = {
            "rig_id": self.rig_id,
            "timestamp": timestamp,
            "location": {
                "latitude": self.latitude + random.gauss(0, 0.0001),
                "longitude": self.longitude + random.gauss(0, 0.0001),
                "depth": random.uniform(100, 5000)
            },
            "pressure": {
                "value": pressure,
                "unit": "PSI",
                "status": "normal" if abs(pressure - self.base_pressure) < 100 else "warning"
            },
            "temperature": {
                "value": temperature,
                "unit": "°F",
                "status": "normal" if abs(temperature - self.base_temperature) < 20 else "warning"
            },
            "flow_rate": {
                "value": flow_rate,
                "unit": "bbl/day",
                "status": "normal" if abs(flow_rate - self.base_flow_rate) < 20 else "warning"
            },
            "equipment_status": equipment_status,
            "sensor_readings": sensor_readings,
            "maintenance_metrics": maintenance_metrics,
            "environmental": environmental,
            "safety_metrics": {
                "gas_levels": random.uniform(0, 100),
                "pressure_variance": random.uniform(0, 10),
                "emergency_systems_status": "active"
            }
        }
        
        # Add random alerts if conditions are critical
        if any([
            abs(pressure - self.base_pressure) > 200,
            abs(temperature - self.base_temperature) > 40,
            equipment_status["safety_system"] == "critical",
            maintenance_metrics["parts_wear_level"] > 90
        ]):
            metrics["alerts"] = {
                "level": "critical",
                "message": "Critical condition detected",
                "timestamp": timestamp
            }
        
        return metrics
    
    def generate_message(self) -> str:
        """Generate a JSON message with the current metrics."""
        metrics = self.generate_metrics()
        return json.dumps(metrics) 