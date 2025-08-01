import pandas as pd
import numpy as np

# Load CSV
df = pd.read_csv("week2_energy_usage.csv")

# Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Ensure energy_kwh is float
df['energy_kwh'] = df['energy_kwh'].astype(float)

# Check for missing values and fill with 0
df.fillna({'energy_kwh': 0}, inplace=True)

# Total and average energy per device
device_summary = df.groupby('device_id')['energy_kwh'].agg(['sum', 'mean']).reset_index()

# Room-level total usage
room_summary = df.groupby('room_id')['energy_kwh'].sum().reset_index()

# Print summaries
print("=== Device Summary (Total & Avg) ===")
print(device_summary)
print("\n=== Room Summary (Total Usage) ===")
print(room_summary)

# Save cleaned dataset and summaries
df.to_csv("week2_cleaned_energy_usage.csv", index=False)
device_summary.to_csv("week2_device_summary.csv", index=False)
room_summary.to_csv("week2_room_summary.csv", index=False)
