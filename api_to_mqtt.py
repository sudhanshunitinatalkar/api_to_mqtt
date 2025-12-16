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
LOG_FILENAME = "api_to_mqtt.log"
LOG_LOCK = threading.Lock() # Lock for thread-safe file writing

# --- NEW: CONFIGURATION FOR LOG LIMIT ---
MAX_LOG_LINES = 100 

# API Endpoints
LOGIN_URL = "https://airquality.aqi.in/api/v1/login"
DEVICE_URL = "https://airquality.aqi.in/api/v1/GetAllUserDevices"

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
# --- MODIFIED LOGGING FUNCTION ---
# ==========================================
def log_event(job_name, event_type, details, payload=None):
    with LOG_LOCK:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Build the main log line
            log_entry = f"[{timestamp}] [{job_name}] {event_type} | {details}"
            
            # Append payload details if available
            if payload is not None:
                if isinstance(payload, dict) or isinstance(payload, list):
                    payload_str = json.dumps(payload, indent=4)
                else:
                    payload_str = str(payload)
                
                log_entry += f"\n--- PAYLOAD ---\n{payload_str}\n----------------"

            # 1. Read existing lines
            lines = []
            if os.path.exists(LOG_FILENAME):
                with open(LOG_FILENAME, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

            # 2. Append new entry (ensure it has a newline)
            lines.append(log_entry + "\n")

            # 3. Trim to keep only the last MAX_LOG_LINES
            if len(lines) > MAX_LOG_LINES:
                lines = lines[-MAX_LOG_LINES:]

            # 4. Write back to file (Overwrite mode 'w')
            with open(LOG_FILENAME, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
        except Exception as e:
            # Fallback print to console if file IO fails
            print(f"[{job_name}] Logging Error: {e}")

# ==========================================
# --- AUTH & API FUNCTIONS ---
# ==========================================
def get_api_token(email, password, job_name):
    try:
        # 1. Log Login Request Payload (password is redacted)
        request_payload_log = {'email': email, 'password': '***REDACTED***'}
        log_event(job_name, "API_LOGIN_REQUEST", f"URL: {LOGIN_URL}", request_payload_log)

        # Use original payload for the actual API call
        original_payload = {'email': email, 'password': password}
        response = requests.post(LOGIN_URL, data=original_payload, timeout=15)
        
        # 2. Log Login Response Payload
        try:
            response_json = response.json()
        except:
            response_json = "Non-JSON Response"

        log_event(job_name, "API_LOGIN_RESPONSE", f"Status: {response.status_code}", response_json)
        
        if response.status_code == 200:
            data = response.json()
            if 'token' in data:
                print(f"[{job_name}] Auth Success.")
                return data['token']
        print(f"[{job_name}] Auth Failed: {response.status_code}")
    except Exception as e:
        print(f"[{job_name}] Auth Error: {e}")
        log_event(job_name, "AUTH_ERROR_EXCEPTION", str(e))
    return None

def format_mqtt_string(device_data):
    try:
        realtime_sensors = device_data.get('realtime', [])
        data_parts = []
        for sensor in realtime_sensors:
            name = sensor.get('sensorname', 'Unknown').split('(')[0].strip().upper()
            value = sensor.get('sensorvalue', 0)
            data_parts.append(f"{name}:{value}")

        now = datetime.now()
        date_str = now.strftime("DATE:%Y-%m-%d,%H:%M:%S")
        if not data_parts: return None
        return ",".join(data_parts) + "," + date_str
    except Exception as e:
        return None

def publish_mqtt(payload, mqtt_config, job_name):
    broker = mqtt_config.get('broker')
    port = mqtt_config.get('port', 1883)
    user = mqtt_config.get('username')
    password = mqtt_config.get('password')
    topic = mqtt_config.get('topic')

    client = mqtt.Client()
    if user and password:
        client.username_pw_set(user, password)
    
    try:
        client.connect(broker, port, 60)
        client.publish(topic, payload)
        client.disconnect()
        print(f"[{job_name}] -> Sent to {broker} :: {topic}")
        return True
    except Exception as e:
        print(f"[{job_name}] MQTT Error ({broker}): {e}")
        log_event(job_name, "MQTT_CONNECT_ERROR", str(e), mqtt_config)
        return False

def run_job(job_config):
    name = job_config.get('job_name', 'Unnamed')
    interval = job_config.get('interval', 10)
    api_conf = job_config['api']
    mappings = job_config.get('device_mappings', [])

    print(f"[{name}] Starting worker thread...")
    token = None
    
    while True:
        loop_start = time.time()
        
        # 1. Login
        if not token:
            token = get_api_token(api_conf['email'], api_conf['password'], name)
            if not token:
                time.sleep(10)
                continue

        # 2. Fetch Data
        try:
            headers = {'Authorization': f'bearer {token}'}
            # Log Device Fetch Request
            log_event(name, "API_DEVICE_FETCH_REQUEST", f"URL: {DEVICE_URL}")
            
            response = requests.get(DEVICE_URL, headers=headers, timeout=15)
            
            if response.status_code == 401:
                print(f"[{name}] Token expired. Refreshing...")
                log_event(name, "API_TOKEN_EXPIRED", "Status 401 received.")
                token = None
                continue
                
            raw_json = response.json()
            # Log Device Fetch Response
            log_event(name, "API_DEVICE_FETCH_RESPONSE", f"Status: {response.status_code}", raw_json)
                
            if response.status_code == 200:
                devices_list = raw_json.get('data', [])

                if not devices_list:
                    print(f"[{name}] No devices found.")

                # 3. Process Mappings
                for device in devices_list:
                    dev_name = device.get('devicename', '')
                    dev_sn = device.get('serialno', '')
                    
                    # Check every mapping rule
                    for map_rule in mappings:
                        keyword = map_rule.get('device_keyword', '*') # Default to * if missing
                        mqtt_conf = map_rule.get('mqtt', {})
                        
                        # Match Logic
                        is_match = (keyword == "*") or (keyword in dev_name) or (keyword in dev_sn)
                        
                        if is_match:
                            payload = format_mqtt_string(device)
                            if payload:
                                success = publish_mqtt(payload, mqtt_conf, name)
                                if success:
                                    details = f"Device: {dev_name} ({dev_sn}) -> Topic: {mqtt_conf.get('topic')}"
                                    log_event(name, "MQTT_PUBLISH_SUCCESS", details, payload)
                                else:
                                    details = f"Device: {dev_name} ({dev_sn}) -> FAILED to publish to {mqtt_conf.get('broker')}"
                                    log_event(name, "MQTT_PUBLISH_FAILURE", details, payload)
                            else:
                                print(f"[{name}] Data format failed for {dev_name}")
                                log_event(name, "DATA_FORMAT_ERROR", f"Failed to format data for device: {dev_name} ({dev_sn})", device)
            else:
                print(f"[{name}] API Error: {response.status_code}")
                
        except json.JSONDecodeError:
            print(f"[{name}] API Error: Received non-JSON response.")
            log_event(name, "API_RESPONSE_ERROR", f"Received non-JSON response or timeout for URL: {DEVICE_URL}")
        except requests.exceptions.Timeout:
            print(f"[{name}] API Error: Request timed out.")
            log_event(name, "API_TIMEOUT_ERROR", f"Request timed out for URL: {DEVICE_URL}")
        except Exception as e:
            print(f"[{name}] Loop Exception: {e}")
            log_event(name, "LOOP_EXCEPTION", f"Unhandled exception in run_job loop: {str(e)}")
            token = None

        elapsed = time.time() - loop_start
        sleep_time = max(0, interval - elapsed)
        time.sleep(sleep_time)

def main():
    print("--- Multi-Broker / Multi-Account Gateway ---")
    print(f"--- Logging enabled: Keeping last {MAX_LOG_LINES} lines ---")
    config = load_config()
    if not config: return

    threads = []
    for job in config:
        if job.get('enabled', True):
            t = threading.Thread(target=run_job, args=(job,))
            t.daemon = True
            t.start()
            threads.append(t)

    print(f"--- Running {len(threads)} jobs simultaneously ---")
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")

if __name__ == "__main__":
    main()