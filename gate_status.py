import cv2
import time
from datetime import datetime, timezone, timedelta
from collections import deque, Counter
import requests
import urllib.parse
import os
import mysql.connector
import re
import json
from ultralytics import YOLO
import torch
import ast
import socket
import requests.packages.urllib3.util.connection as urllib3_cn
import paho.mqtt.client as mqtt

# ===================== CONFIG =====================
VIDEO_PATH = "tapo_gate2.mp4"
BEST_WEIGHTS = "runs/classify/train1/weights/best.pt"
WRITE_VIDEO = True
OUTPUT_VIDEO_PATH = "output/sms_testing.mp4"
STATE_HOLD_FRAMES = 12
OPEN_DURATION_THRESHOLD = 60   # send SMS only if open >= 1min
ACTIVE_START_HOUR = 0
ACTIVE_END_HOUR = 24

# SMS CONFIG
SMS_USERNAME = "vbengg"
SMS_APIKEY   = "81f85419a3873fe69a7c"
SMS_SENDERID = "WISTWN"
SMS_MOBILE   = "919390777484"
SMS_TEMPLATE_OPEN  = "1707175569545170117"
SMS_TEMPLATE_CLOSE = "1707175575199632016"

# MySQL config
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASS = "Vidya@123"
MYSQL_DB   = "gate_status"

# MQTT Config
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "gate/status"

# ================= Helper: Force IPv4 =================
def _allowed_gai_family():
    return socket.AF_INET
urllib3_cn.allowed_gai_family = _allowed_gai_family

# ================= Helper: GET with retry/backoff =================
def safe_get(url, params=None, timeout=8, retries=3, backoff=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout) if params else requests.get(url, timeout=timeout)
            return r
        except Exception as e:
            print(f"[HTTP Retry {attempt}/{retries}] {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
    return None

# ================== MySQL Setup ==================
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASS,
    database=MYSQL_DB
)
cursor = conn.cursor()

mobiles = [m.strip() for m in SMS_MOBILE.split(",") if m.strip()]
num_mobiles = len(mobiles)

cursor.execute("""
CREATE TABLE IF NOT EXISTS gate_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    gate_status VARCHAR(20) NOT NULL
)
""")
conn.commit()

cursor.execute("SHOW COLUMNS FROM gate_status")
existing_cols = [r[0].lower() for r in cursor.fetchall()]
for i in range(1, num_mobiles + 1):
    for col, typ in [(f"mobile{i}", "VARCHAR(15)"), (f"campid{i}", "VARCHAR(50)"), (f"delivery_status{i}", "VARCHAR(20)")] :
        if col not in existing_cols:
            cursor.execute(f"ALTER TABLE gate_status ADD COLUMN {col} {typ}")
            print(f"[DB] Added column {col}")
conn.commit()
print(f"[DB] Schema verified. Table supports {num_mobiles} mobile(s).")

# ================= Timezone =================
IST = timezone(timedelta(hours=5, minutes=30))
def get_timestamp():
    return datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')

# ================= MQTT Setup =================
mqtt_client = mqtt.Client()
try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()
    print(f"[MQTT] Connected to broker {MQTT_BROKER}:{MQTT_PORT}")
except Exception as e:
    print(f"[MQTT ERROR] Could not connect: {e}")

# ---------------- Improved DB write logic (prevents duplicate same-timestamp rows) ----------------

def _get_latest_row():
    """Return (row_dict, raw_row) of latest row or (None, None) if empty."""
    try:
        cursor.execute("SELECT * FROM gate_status ORDER BY id DESC LIMIT 1")
        r = cursor.fetchone()
        if not r:
            return None, None
        cols = [c[0] for c in cursor.description]
        return dict(zip(cols, r)), r
    except Exception as e:
        print(f"[DB ERROR] _get_latest_row: {e}")
        return None, None

def log_gate_state(state, sms_results=None, campid=None):
    """
    Insert a new gate_status row unless the latest row already has the same
    timestamp and state — in that case update missing/empty columns instead.
    This prevents duplicate rows for the same event.
    """
    ts = get_timestamp()  # e.g. '2025-09-30 17:28:04'
    new_row = {"timestamp": ts, "gate_status": state}

    # fill columns for mobiles/campid/status
    for i in range(1, num_mobiles + 1):
        if sms_results and (i-1) < len(sms_results):
            entry = sms_results[i-1] or {}
            new_row[f"mobile{i}"] = entry.get("mobile") or None
            new_row[f"campid{i}"] = entry.get("campid") or (campid if campid else None)
            new_row[f"delivery_status{i}"] = entry.get("status") or "PENDING"
        else:
            # if no sms_results provided, prefer to keep known mobile numbers
            new_row[f"mobile{i}"] = mobiles[i-1] if i-1 < len(mobiles) else None
            new_row[f"campid{i}"] = campid if campid else None
            new_row[f"delivery_status{i}"] = "PENDING"

    try:
        # check latest row to avoid duplicate insert
        latest_dict, latest_raw = _get_latest_row()
        if latest_dict and str(latest_dict.get("timestamp")) == ts and str(latest_dict.get("gate_status")).lower() == str(state).lower():
            # same timestamp & state: update any columns that are empty/NULL in DB with values we have
            updates = []
            values = []
            for k, v in new_row.items():
                # skip timestamp and gate_status when building updates
                if k in ("timestamp", "gate_status"):
                    continue
                # if DB row has empty/NULL or blank string and our new_row has a value, set it
                db_val = latest_dict.get(k)
                if (db_val is None or (isinstance(db_val, str) and db_val.strip() == "")) and (v is not None and (not (isinstance(v, str) and v.strip() == ""))):
                    updates.append(f"{k}=%s")
                    values.append(v)
            if updates:
                values.append(latest_dict["id"])
                sql = f"UPDATE gate_status SET {', '.join(updates)} WHERE id=%s"
                cursor.execute(sql, values)
                conn.commit()
                print(f"[DB] Updated existing row id={latest_dict['id']} for {ts} / {state}")
            else:
                print(f"[DB] Duplicate event for {ts} / {state} — nothing to update.")
        else:
            # Insert new row (no duplicate)
            columns = ", ".join(new_row.keys())
            placeholders = ", ".join(["%s"] * len(new_row))
            sql = f"INSERT INTO gate_status ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(new_row.values()))
            conn.commit()
            inserted_id = cursor.lastrowid
            print(f"[DB] Inserted gate state id={inserted_id} at {ts}: {state}")

    except Exception as e:
        print(f"[DB ERROR] log_gate_state: {e}")

    # ----- MQTT Publish (always publish the event) -----
    try:
        mqtt_payload = json.dumps({"timestamp": ts, "gate_status": state})
        mqtt_client.publish(MQTT_TOPIC, mqtt_payload)
        print(f"[MQTT] Published: {mqtt_payload}")
    except Exception as e:
        print(f"[MQTT ERROR] {e}")

def parse_send_response_text(text):
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data.get("campid")
    except Exception:
        pass
    try:
        data = ast.literal_eval(text)
        if isinstance(data, dict):
            return data.get("campid")
    except Exception:
        pass
    m = re.search(r"campid[=:]'?\"?([0-9a-zA-Z]+)'?\"?", text)
    if m:
        return m.group(1)
    return None


def update_sms_delivery(campid, sms_results, state):
    """
    Update SMS delivery columns in the latest row of the given state.
    (Kept mostly same, but with defensive checks and log messages.)
    """
    try:
        cursor.execute("SELECT id FROM gate_status WHERE gate_status=%s ORDER BY id DESC LIMIT 1", (state,))
        row = cursor.fetchone()
        if not row:
            print(f"[DB] update_sms_delivery: no existing row found for state={state}")
            return
        row_id = row[0]

        updates = []
        values = []
        for i in range(1, num_mobiles + 1):
            if (i-1) < len(sms_results):
                entry = sms_results[i-1] or {}
                updates.append(f"campid{i}=%s")
                updates.append(f"delivery_status{i}=%s")
                values.append(entry.get("campid"))
                values.append(entry.get("status"))
        if updates:
            values.append(row_id)
            sql = f"UPDATE gate_status SET {', '.join(updates)} WHERE id=%s"
            cursor.execute(sql, values)
            conn.commit()
            print(f"[DB] Updated SMS delivery for row {row_id}")
    except Exception as e:
        print(f"[DB ERROR] update_sms_delivery: {e}")


def log_sms_delivery(campid, state, max_retries=12, delay=10):
    """
    Manage SMS delivery reporting. We only call log_gate_state() here when:
      - campid is present and we need to create the initial row (log_gate_state
        will itself be duplicate-safe), OR
      - when the SMS API fails completely we call log_gate_state(state) once.
    Subsequent delivery updates will call update_sms_delivery() to patch the same row.
    """
    final_results = [{"mobile": m, "campid": campid, "status": "PENDING"} for m in mobiles]
    retries = 0

    # If we already have a campid then insert (or update) the base row now.
    # log_gate_state is safe to call multiple times because it checks duplicates.
    if campid:
        log_gate_state(state, final_results, campid)

    while retries < max_retries:
        url = f"https://smslogin.co/v3/api.php?username={SMS_USERNAME}&apikey={SMS_APIKEY}&campid={campid}"
        r = safe_get(url, timeout=10, retries=3, backoff=2)
        if not r:
            retries += 1
            time.sleep(delay)
            continue

        text = r.text.strip()
        reports_str = ""
        try:
            data = json.loads(text.replace("'", '"'))
            reports_str = data.get("Reports", "")
        except Exception:
            m = re.search(r"'?Reports'? *: *'([^']+)'", text)
            if m:
                reports_str = m.group(1)

        if reports_str:
            all_done = True
            entries = [e.strip() for e in reports_str.split(";") if e.strip()]
            for entry in entries:
                parts = entry.split("-", 1)
                if len(parts) == 2:
                    mobile, status = parts
                    status = status.upper()
                    if status not in ["DELIVERED", "FAILED"]:
                        all_done = False
                    for fr in final_results:
                        if fr["mobile"] == mobile:
                            fr["status"] = status
                            break

            # Update the same row instead of inserting again
            update_sms_delivery(campid, final_results, state)

            if all_done:
                break

        retries += 1
        time.sleep(delay)

def send_sms_alert(state):
    ts = get_timestamp()
    if state.lower() == "open":
        message = f"Dear Team, Alert: Main Gate is open on {ts}. - Team Vijaya Bhanu"
        template_id = SMS_TEMPLATE_OPEN
    else:
        message = f"Dear Team, Alert: Main Gate is closed on {ts}. - Team Vijaya Bhanu"
        template_id = SMS_TEMPLATE_CLOSE
    url = "http://smslogin.co/v3/api.php"
    mobile_param = ",".join(mobiles)
    encoded_url = f"{url}?username={SMS_USERNAME}&apikey={SMS_APIKEY}&senderid={SMS_SENDERID}&mobile={urllib.parse.quote(mobile_param)}&message={urllib.parse.quote(message)}&templateid={template_id}"
    r = safe_get(encoded_url, timeout=10, retries=3, backoff=2)
    if not r:
        log_gate_state(state)
        return
    campid = parse_send_response_text(r.text)
    if campid:
        log_sms_delivery(campid, state)
    else:
        log_gate_state(state)

# ================= YOLO =================
device = 0 if torch.cuda.is_available() else 'cpu'
model = YOLO(BEST_WEIGHTS)
try:
    _ = model.predict(source=None, imgsz=224, device=device, verbose=False, stream=False)
except Exception as e:
    print(f"[WARN] YOLO load warmup failed: {e}")

# ================= Video =================
cap = cv2.VideoCapture(VIDEO_PATH)
src_fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
ret, first_frame = cap.read()
if not ret:
    raise RuntimeError("Cannot read from video source.")
h, w = first_frame.shape[:2]
writer = None
if WRITE_VIDEO:
    os.makedirs(os.path.dirname(OUTPUT_VIDEO_PATH), exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    writer = cv2.VideoWriter(OUTPUT_VIDEO_PATH, fourcc, src_fps, (w, h))

# ================= State Tracking =================
open_scores = deque(maxlen=STATE_HOLD_FRAMES)
closed_scores = deque(maxlen=STATE_HOLD_FRAMES)
frame_labels = deque(maxlen=STATE_HOLD_FRAMES)

def classify_frame(img_bgr):
    res = model(img_bgr, device=device, verbose=False)[0]
    probs = res.probs.data.cpu().numpy()
    names = res.names
    inv_names = {v.lower(): k for k, v in names.items()}
    idx_open   = inv_names.get('open', 0)
    idx_closed = inv_names.get('closed', 1 if len(probs) > 1 else 0)
    p_open, p_closed = float(probs[idx_open]), float(probs[idx_closed])
    return ("Open" if p_open >= p_closed else "Closed"), p_open, p_closed

# Initial state
pred0, _, _ = classify_frame(first_frame)
last_state = pred0
state_change_candidate = last_state
state_change_counter = STATE_HOLD_FRAMES
open_start_time = datetime.now(IST) if last_state == "Open" else None
open_sms_sent = False
frame_idx = 0

try:
    while True:
        frame = first_frame if frame_idx == 0 else cap.read()[1]
        if frame is None:
            break

        pred, p_open, p_closed = classify_frame(frame)
        open_scores.append(p_open)
        closed_scores.append(p_closed)
        frame_labels.append(pred)

        label_counts = Counter(frame_labels)
        majority_label = label_counts.most_common(1)[0][0]
        avg_open = sum(open_scores)/len(open_scores)
        avg_closed = sum(closed_scores)/len(closed_scores)
        gate_state = "Open" if (majority_label=="Open" and avg_open>=avg_closed) else "Closed"

        if gate_state != state_change_candidate:
            state_change_candidate = gate_state
            state_change_counter = 1
        else:
            state_change_counter += 1

        if state_change_counter >= STATE_HOLD_FRAMES and state_change_candidate != last_state:
            if state_change_candidate == "Open":
                open_start_time = datetime.now(IST)
                open_sms_sent = False
            elif state_change_candidate == "Closed":
                if open_start_time:
                    elapsed = (datetime.now(IST) - open_start_time).total_seconds()
                    if elapsed >= OPEN_DURATION_THRESHOLD and open_sms_sent:
                        send_sms_alert("Closed")
                open_start_time = None
                open_sms_sent = False
            last_state = state_change_candidate

        if last_state == "Open" and open_start_time and not open_sms_sent:
            elapsed = (datetime.now(IST) - open_start_time).total_seconds()
            if elapsed >= OPEN_DURATION_THRESHOLD:
                send_sms_alert("Open")
                open_sms_sent = True

        # Overlay
        cv2.putText(frame, f"Gate: {last_state}", (120, 390), cv2.FONT_HERSHEY_SIMPLEX, 1.2,
                    (0,255,0) if last_state=="Open" else (0,0,255), 3)

        # Show frame live
        cv2.imshow("Gate Monitoring", frame)
        # Delay based on source fps (in ms)
        # delay = int(1000 / src_fps)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

        if writer:
            writer.write(frame)

        # time.sleep(1.0/src_fps)
        frame_idx += 1


finally:
    cap.release()
    if writer:
        writer.release()
    cursor.close()
    conn.close()
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    cv2.destroyAllWindows()
    print(f"[INFO] ✅ Gate monitoring completed. Logs stored in MySQL and MQTT")
