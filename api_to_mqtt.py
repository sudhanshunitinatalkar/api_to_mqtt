import requests
import json
import time
import os
import threading
from datetime import datetime
import paho.mqtt.client as mqtt

# ==========================================
# --- GLOBAL SETTINGS ---
# ==========================================
CONFIG_FILENAME = "config.json"
LOG_DIR = "logs" 
LOG_LOCK = threading.Lock() 
MAX_LOG_LINES = 50000  # Rolling buffer limit

# API Endpoints
LOGIN_URL = "https://airquality.aqi.in/api/v1/login"
DEVICE_URL = "https://airquality.aqi.in/api/v1/GetAllUserDevices"

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def load_config():
    if not os.path.exists(CONFIG_FILENAME):
        print(f"CRITICAL ERROR: {CONFIG_FILENAME} not found.")
        return []
    try:
        with open(CONFIG_FILENAME, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"CRITICAL ERROR: Could not parse {CONFIG_FILENAME}. {e}")
        return []

# ==========================================
# --- TIERED LOGGING SYSTEM ---
# ==========================================
def log_to_file(job_name, log_type, event_type, details, payload=None):
    """
    log_type: "info" or "error"
    """
    with LOG_LOCK:
        try:
            # Create job-specific folder
            safe_name = "".join([c for c in job_name if c.isalnum() or c in (' ', '.', '_')]).strip().replace(" ", "_")
            job_dir = os.path.join(LOG_DIR, safe_name)
            if not os.path.exists(job_dir):
                os.makedirs(job_dir)

            log_path = os.path.join(job_dir, f"{log_type}.log")

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"[{timestamp}] {event_type} | {details}"
            
            if payload is not None:
                if isinstance(payload, (dict, list)):
                    payload_str = json.dumps(payload, indent=4)
                else:
                    payload_str = str(payload)
                log_entry += f"\n--- PAYLOAD ---\n{payload_str}\n----------------"

            # Read existing lines to manage rolling buffer
            lines = []
            if os.path.exists(log_path):
                with open(log_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

            lines.append(log_entry + "\n")

            # Rolling Cycle: keep only the most recent 50k lines
            if len(lines) > MAX_LOG_LINES:
                lines = lines[-MAX_LOG_LINES:]

            with open(log_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
        except Exception as e:
            print(f"Logging Error for {job_name}: {e}")

# ==========================================
# --- AUTH & API FUNCTIONS ---
# ==========================================
def get_api_token(email, password, job_name):
    try:
        request_payload = {'email': email, 'password': password}
        log_to_file(job_name, "info", "API_LOGIN_REQUEST", f"URL: {LOGIN_URL}", request_payload)

        response = requests.post(LOGIN_URL, data=request_payload, timeout=15)
        
        try:
            response_json = response.json()
        except:
            response_json = f"RAW_RESPONSE: {response.text}"

        if response.status_code == 200:
            log_to_file(job_name, "info", "API_LOGIN_SUCCESS", f"Status: {response.status_code}", response_json)
            return response_json.get('token')
        else:
            log_to_file(job_name, "error", "AUTH_FAILED", f"Status: {response.status_code}", response_json)
            
    except Exception as e:
        log_to_file(job_name, "error", "AUTH_EXCEPTION", str(e))
    return None

def format_mqtt_string(device_data):
    """
    Extracts only PM2.5, PM10, TEMP, and HUM from the device data
    and formats them as KEY:VALUE strings.
    """
    try:
        realtime_sensors = device_data.get('realtime', [])
        found_data = {}

        # 1. Extract and normalize available sensor data
        for sensor in realtime_sensors:
            # API usually returns names like "PM2.5 (ug/m3)", "Temperature (C)"
            # We normalize this to just "PM2.5", "TEMPERATURE", etc.
            raw_name = sensor.get('sensorname', 'Unknown').split('(')[0].strip().upper()
            value = sensor.get('sensorvalue', 0)

            # Map common API names to your specific target keys
            if raw_name in ["PM2.5", "PM25"]:
                found_data["PM25"] = value
            elif raw_name in ["PM10"]:
                found_data["PM10"] = value
            elif raw_name in ["TEMPERATURE", "TEMP"]:
                found_data["TEMP"] = value
            elif raw_name in ["HUMIDITY", "HUM"]:
                found_data["HUM"] = value

        # 2. Construct the data string in the specific order requested
        target_order = ["PM25", "PM10", "TEMP", "HUM"]
        data_parts = []

        for key in target_order:
            if key in found_data:
                data_parts.append(f"{key}:{found_data[key]}")

        # If none of the relevant sensors were found, return None to skip publishing
        if not data_parts:
            return None

        # 3. Add Timestamp
        now = datetime.now()
        date_str = now.strftime("DATE:%Y-%m-%d,%H:%M:%S")
        
        return ",".join(data_parts) + "," + date_str

    except Exception as e:
        # In case of parsing error, return None
        return None

def publish_mqtt(payload, mqtt_config, job_name):
    broker = mqtt_config.get('broker')
    port = mqtt_config.get('port', 1883)
    user = mqtt_config.get('username')
    pw = mqtt_config.get('password')
    topic = mqtt_config.get('topic')

    # Explicitly specify CallbackAPIVersion to remove DeprecationWarning
    try:
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        if user and pw:
            client.username_pw_set(user, pw)
        
        client.connect(broker, port, 60)
        client.publish(topic, payload)
        client.disconnect()
        return True
    except Exception as e:
        log_to_file(job_name, "error", "MQTT_CONNECTION_ERROR", f"Broker: {broker}", str(e))
        return False

def run_job(job_config):
    name = job_config.get('job_name', 'Unnamed')
    interval = job_config.get('interval', 10)
    api_conf = job_config['api']
    mappings = job_config.get('device_mappings', [])

    token = None
    
    while True:
        loop_start = time.time()
        
        if not token:
            token = get_api_token(api_conf['email'], api_conf['password'], name)
            if not token:
                time.sleep(10)
                continue

        try:
            headers = {'Authorization': f'bearer {token}'}
            log_to_file(name, "info", "API_DEVICE_FETCH_REQUEST", f"URL: {DEVICE_URL}")
            
            response = requests.get(DEVICE_URL, headers=headers, timeout=15)
            
            try:
                resp_data = response.json()
            except:
                resp_data = f"RAW_NON_JSON_CONTENT: {response.text}"
            
            if response.status_code == 200:
                log_to_file(name, "info", "API_DEVICE_FETCH_RESPONSE", "Full Device List Received", resp_data)
                
                devices_list = resp_data.get('data', []) if isinstance(resp_data, dict) else []

                for device in devices_list:
                    dev_name = device.get('devicename', '')
                    
                    for map_rule in mappings:
                        target_name = map_rule.get('device_name')
                        mqtt_conf = map_rule.get('mqtt', {})
                        
                        if dev_name == target_name:
                            payload = format_mqtt_string(device)
                            if payload:
                                if publish_mqtt(payload, mqtt_conf, name):
                                    log_to_file(name, "info", "MQTT_PUBLISH_SUCCESS", f"Topic: {mqtt_conf.get('topic')}", payload)
                                else:
                                    log_to_file(name, "error", "MQTT_PUBLISH_FAILURE", f"Failed to send to {mqtt_conf.get('topic')}", payload)
                            else:
                                log_to_file(name, "error", "FORMAT_ERROR", f"No matching sensor data (PM25, PM10, TEMP, HUM) for {dev_name}")
            
            elif response.status_code == 401:
                log_to_file(name, "error", "API_TOKEN_EXPIRED", "401 Unauthorized - Re-authenticating...")
                token = None
                continue
            else:
                log_to_file(name, "error", "API_FETCH_ERROR", f"Status {response.status_code}", resp_data)
                
        except Exception as e:
            log_to_file(name, "error", "CRITICAL_LOOP_EXCEPTION", str(e))
            token = None

        elapsed = time.time() - loop_start
        time.sleep(max(0, interval - elapsed))

def main():
    config = load_config()
    if not config: return

    threads = []
    for job in config:
        if job.get('enabled', True):
            t = threading.Thread(target=run_job, args=(job,))
            t.daemon = True
            t.start()
            threads.append(t)

    print(f"--- Running {len(threads)} jobs. Separate logs in 'logs/[job_name]/' folder. ---")
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    main()
