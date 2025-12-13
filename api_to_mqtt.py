import requests
import json
import time
import csv
import os
from datetime import datetime
import paho.mqtt.client as mqtt

# ==========================================
# --- USER CONFIGURATION ---
# ==========================================

# 1. TIMING
# How often to run the loop (in seconds)
PUBLISH_INTERVAL = 10

# 2. LOGGING
CSV_FILENAME = "api_mqtt_log.csv"

# 3. API CREDENTIALS (AQI.in)
API_EMAIL = "purchaseacres@bhoomi-group.com"     
API_PASSWORD = "Nimu1015"

# 4. MQTT BROKER DETAILS
MQTT_BROKER = "cloud.pbrresearch.com"
MQTT_PORT = 1883 
MQTT_USER = "317060018"
MQTT_PASS = "317060018"
MQTT_TOPIC = "test/display_1"

# ==========================================
# --- END CONFIGURATION ---
# ==========================================

# API Endpoints
LOGIN_URL = "https://airquality.aqi.in/api/v1/login"
DEVICE_URL = "https://airquality.aqi.in/api/v1/GetAllUserDevices"

def get_api_token():
    """
    Logs in to the API and retrieves the Bearer token.
    """
    try:
        payload = {'email': API_EMAIL, 'password': API_PASSWORD}
        print(f"Authentication: Requesting new token from {LOGIN_URL}...")
        
        response = requests.post(LOGIN_URL, data=payload)
        
        if response.status_code == 200:
            data = response.json()
            if 'token' in data:
                print("Authentication: Success.")
                return data['token']
            else:
                print("Authentication: Token missing in response.")
        else:
            print(f"Authentication Failed: {response.status_code}")
            
    except Exception as e:
        print(f"Authentication Error: {e}")
    return None

def append_to_csv(request_info, raw_response, mqtt_payload):
    """
    Appends a new row to the CSV file.
    """
    file_exists = os.path.isfile(CSV_FILENAME)
    
    try:
        with open(CSV_FILENAME, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            # Write Header if new file
            if not file_exists:
                writer.writerow(["Timestamp", "Request_Details", "Raw_API_Response_JSON", "MQTT_Payload"])
            
            # Create timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Dump dictionary to string for CSV storage
            if isinstance(raw_response, (dict, list)):
                raw_response_str = json.dumps(raw_response)
            else:
                raw_response_str = str(raw_response)

            writer.writerow([timestamp, request_info, raw_response_str, mqtt_payload])
            print(f"Logging: Data saved to {CSV_FILENAME}")
            
    except Exception as e:
        print(f"Logging Error: Could not write to CSV - {e}")

def format_mqtt_string(device_json):
    """
    Formats the JSON data into the specific string format required.
    """
    if not device_json or 'data' not in device_json or not device_json['data']:
        return None

    # Get first device
    try:
        device = device_json['data'][0]
        realtime_sensors = device.get('realtime', [])
        
        data_parts = []
        for sensor in realtime_sensors:
            # Clean name: "Temp(cel)" -> "TEMP"
            name = sensor.get('sensorname', 'Unknown').split('(')[0].strip().upper()
            value = sensor.get('sensorvalue', 0)
            data_parts.append(f"{name}:{value}")

        # Add timestamp: DATE:2025-02-13,20:45:28
        now = datetime.now()
        date_str = now.strftime("DATE:%Y-%m-%d,%H:%M:%S")
        
        return ",".join(data_parts) + "," + date_str
    except Exception as e:
        print(f"Formatting Error: {e}")
        return None

def publish_mqtt(payload):
    """
    Publishes payload to MQTT broker.
    """
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.publish(MQTT_TOPIC, payload)
        client.disconnect()
        print(f"MQTT: Published to {MQTT_TOPIC}")
        return True
    except Exception as e:
        print(f"MQTT Error: {e}")
        return False

def main():
    token = None
    
    print(f"--- Starting Service (Interval: {PUBLISH_INTERVAL}s) ---")
    
    while True:
        loop_start_time = time.time()
        
        # 1. Get Token (Login if we don't have one)
        if not token:
            token = get_api_token()
            if not token:
                print("Error: Could not get token. Retrying in 10s...")
                time.sleep(10)
                continue

        # 2. Prepare Request
        request_details = f"GET {DEVICE_URL}"
        headers = {'Authorization': f'bearer {token}'}

        # 3. Fetch Data
        try:
            print("\nFetching device data...")
            response = requests.get(DEVICE_URL, headers=headers)
            
            # Handle Token Expiry (401 Unauthorized)
            if response.status_code == 401:
                print("Token expired. Re-authenticating...")
                token = None
                continue
            
            # Handle Success
            if response.status_code == 200:
                raw_json = response.json()
                
                # 4. Format Payload
                mqtt_payload = format_mqtt_string(raw_json)
                
                if mqtt_payload:
                    print(f"Payload generated: {mqtt_payload[:50]}...") 
                    
                    # 5. Publish
                    publish_mqtt(mqtt_payload)
                    
                    # 6. Log to CSV
                    append_to_csv(request_details, raw_json, mqtt_payload)
                else:
                    print("Warning: Valid JSON received but no device data found to format.")
                    append_to_csv(request_details, raw_json, "ERROR: No Data to Format")
            else:
                print(f"API Error: {response.status_code} - {response.text}")
                append_to_csv(request_details, response.text, "ERROR: API Request Failed")

        except Exception as e:
            print(f"Loop Error: {e}")
            append_to_csv(request_details, str(e), "ERROR: Exception")

        # 7. Sleep for the remainder of the interval
        elapsed = time.time() - loop_start_time
        sleep_time = max(0, PUBLISH_INTERVAL - elapsed)
        print(f"Sleeping for {sleep_time:.1f} seconds...")
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopping script...")