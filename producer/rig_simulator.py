"""
Simulates oil rig telemetry data for testing and development.
"""
import json
import random
import time
import uuid
from datetime import datetime
from typing import Dict, Any

class RigSimulator:
    """Simulates an oil rig sending telemetry data."""
    
    OPERATIONAL_STATUSES = ["DRILLING", "OPERATIONAL", "STANDBY", "MAINTENANCE"]
    
    def __init__(self, rig_id: str):
        """Initialize a rig simulator with the given ID."""
        self.rig_id = rig_id
        # Generate a semi-stable location for this rig
        self.latitude = random.uniform(25.0, 36.0)  # Gulf of Mexico region
        self.longitude = random.uniform(-100.0, -80.0)
        
    def _generate_measurements(self) -> Dict[str, float]:
        """Generate random measurements for the rig."""
        return {
            "depth": random.uniform(1000, 5000),
            "weight_on_bit": random.uniform(10, 50),
            "rotary_speed": random.uniform(80, 200),
            "rate_of_penetration": random.uniform(10, 50),
            "torque": random.uniform(500, 2000),
            "pressure": random.uniform(2000, 5000),
            "temperature": random.uniform(60, 150),
            "mud_flow_rate": random.uniform(500, 2000)
        }
    
    def _generate_alerts(self) -> Dict[str, Any]:
        """Generate alerts as a dictionary instead of a list."""
        # Usually no alerts (80% of the time)
        if random.random() > 0.2:
            return {"items": []}
            
        severity = random.choice(["WARNING", "CRITICAL"])
        message = random.choice([
            "Pressure exceeding threshold",
            "Temperature too high",
            "Flow rate below minimum",
            "Torque fluctuation detected"
        ])
        
        alert = {
            "alertId": str(uuid.uuid4()),
            "severity": severity,
            "message": message,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        
        # Return alerts as a dictionary with 'items' key containing the list
        return {"items": [alert]}
    
    def generate_message(self) -> str:
        """Generate a complete message from this rig."""
        message = {
            "messageId": str(uuid.uuid4()),
            "rigId": self.rig_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "operationalStatus": random.choice(self.OPERATIONAL_STATUSES),
            "location": {
                "latitude": self.latitude + random.uniform(-0.001, 0.001),
                "longitude": self.longitude + random.uniform(-0.001, 0.001)
            },
            "measurements": self._generate_measurements(),
            "alerts": self._generate_alerts()  # Now returns a dictionary with 'items' key
        }
        return json.dumps(message)