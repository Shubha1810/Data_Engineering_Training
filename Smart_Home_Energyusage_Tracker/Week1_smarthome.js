use SmartHomeEnergyDB;

db.sensor_logs.insertMany([
  {
    device_id: "AC",
    timestamp: ISODate("2025-08-01T08:00:00Z"),
    energy_kwh: 2.5,
    status: "ON"
  },
  {
    device_id: "TV",
    timestamp: ISODate("2025-08-01T09:00:00Z"),
    energy_kwh: 0.3,
    status: "OFF"
  },
  {
    device_id: "Fridge",
    timestamp: ISODate("2025-08-01T10:00:00Z"),
    energy_kwh: 1.2,
    status: "ON"
  },
  {
    device_id: "Oven",
    timestamp: ISODate("2025-08-01T13:00:00Z"),
    energy_kwh: 2.0,
    status: "OFF"
  },
  {
    device_id: "Heater",
    timestamp: ISODate("2025-08-01T11:00:00Z"),
    energy_kwh: 1.8,
    status: "ON"
  },
  {
    device_id: "Light",
    timestamp: ISODate("2025-08-01T17:00:00Z"),
    energy_kwh: 0.1,
    status: "OFF"
  }
]);
