import requests
import json
import os

# --- SETTINGS ---
CONFIG_FILENAME = "config.json"
LOGIN_URL = "https://airquality.aqi.in/api/v1/login"
DEVICE_URL = "https://airquality.aqi.in/api/v1/GetAllUserDevices"

def load_config():
    """Loads the list of jobs from config.json."""
    if not os.path.exists(CONFIG_FILENAME):
        print(f"ERROR: {CONFIG_FILENAME} not found.")
        return []
    try:
        with open(CONFIG_FILENAME, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"ERROR: Could not parse {CONFIG_FILENAME}. {e}")
        return []

def get_devices_for_account(email, password, job_name):
    """Logs into an account and fetches its device list."""
    print(f"\n>>> Checking Job: {job_name} ({email})")
    
    # 1. Login to get token
    try:
        # Note: API expects form-data (application/x-www-form-urlencoded)
        payload = {'email': email, 'password': password}
        response = requests.post(LOGIN_URL, data=payload, timeout=15)
        
        if response.status_code != 200:
            print(f"    [!] Login Failed. Status: {response.status_code}")
            return

        data = response.json()
        token = data.get('token')
        
        if not token:
            print("    [!] Auth Failed: No token returned.")
            return

        # 2. Fetch all devices
        headers = {'Authorization': f'bearer {token}'}
        dev_res = requests.get(DEVICE_URL, headers=headers, timeout=15)
        
        if dev_res.status_code == 200:
            devices = dev_res.json().get('data', [])
            if not devices:
                print("    (No devices found in this account)")
            else:
                print(f"    Found {len(devices)} device(s):")
                print("    " + "-" * 40)
                for d in devices:
                    name = d.get('devicename', 'N/A')
                    sn = d.get('serialNo', 'N/A')
                    print(f"    - Device Name : {name}")
                    print(f"      Serial No   : {sn}")
                    print("    " + "-" * 40)
        else:
            print(f"    [!] Device Fetch Failed. Status: {dev_res.status_code}")

    except Exception as e:
        print(f"    [!] Error: {e}")

def main():
    print("=== AQI.IN DEVICE DISCOVERY ===")
    config = load_config()
    
    if not config:
        print("No jobs found in config.")
        return

    for job in config:
        # Extract credentials from the 'api' block of each job
        api_info = job.get('api', {})
        email = api_info.get('email')
        password = api_info.get('password')
        job_name = job.get('job_name', 'Unnamed Job')
        
        if email and password:
            get_devices_for_account(email, password, job_name)
        else:
            print(f"\n>>> Skipping Job: {job_name} (Missing credentials)")

if __name__ == "__main__":
    main()