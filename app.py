from flask import Flask, render_template, jsonify, request, Response
import mysql.connector
from datetime import datetime, date, time as dtime, timedelta
import os
import threading
import json
import time
import paho.mqtt.client as mqtt

# ----------------- Config -----------------
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASS = "Vidya@123"
MYSQL_DB   = "gate_status"

MQTT_ENABLED = True
MQTT_BROKER = "localhost"
MQTT_PORT   = 1883
MQTT_TOPIC  = "gate/status"

app = Flask(__name__, template_folder="templates", static_folder="static")

# ----------------- Database (READ-ONLY) -----------------
def get_db_conn():
    """Read-only DB connection for fetching historical data"""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        autocommit=True,
    )

def rows_to_dicts(cursor, rows):
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, r)) for r in rows]

# ----------------- MQTT (Live Gate Status) -----------------
latest_gate = {"gate_status": "N/A", "timestamp": datetime.now().isoformat(), "duration": 0}
lock = threading.Lock()

# Prevent duplicate in-memory updates
last_logged_status = None

def mqtt_on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Connected successfully")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"[MQTT] Connection failed with code {rc}")

def mqtt_on_message(client, userdata, msg):
    """Update in-memory gate status from MQTT messages (no DB writes!)"""
    global latest_gate, last_logged_status
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        gate_status = data.get("gate_status")
        ts = data.get("timestamp", datetime.now().isoformat())

        if gate_status and gate_status != last_logged_status:
            with lock:
                # preserve previous last_open_ts if present
                if gate_status == "Closed":
                    # compute duration if last_open_ts exists
                    last_open_iso = latest_gate.get("last_open_ts", ts)
                    try:
                        last_open_ts = datetime.fromisoformat(last_open_iso)
                        closed_ts = datetime.fromisoformat(ts)
                        duration_sec = (closed_ts - last_open_ts).total_seconds()
                        latest_gate["duration"] = duration_sec
                    except Exception:
                        latest_gate["duration"] = 0
                else:
                    latest_gate["last_open_ts"] = ts
                    latest_gate["duration"] = 0

                latest_gate["gate_status"] = gate_status
                latest_gate["timestamp"] = ts

            last_logged_status = gate_status
            print(f"[MQTT] Updated in-memory gate_status={gate_status} at {ts}")
    except Exception as e:
        print(f"[MQTT ERROR] {e}")

def start_mqtt():
    client = mqtt.Client()
    client.on_connect = mqtt_on_connect
    client.on_message = mqtt_on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"[MQTT] Connection error: {e}")
        return
    client.loop_start()
    print("[MQTT] Loop started")

if MQTT_ENABLED:
    threading.Thread(target=start_mqtt, daemon=True).start()

# ----------------- Shift Helpers -----------------
# Shift definitions: use 24-hour times
SHIFTS = {
    "1": {"label": "Shift 1 (10:00-18:00)", "start": dtime(hour=10, minute=0), "end": dtime(hour=18, minute=0)},
    "2": {"label": "Shift 2 (18:00-02:00)", "start": dtime(hour=18, minute=0), "end": dtime(hour=2, minute=0)},
    "3": {"label": "Shift 3 (02:00-10:00)", "start": dtime(hour=2, minute=0),  "end": dtime(hour=10, minute=0)},
}

def compute_shift_window(shift_id, ref_dt=None):
    """
    Given shift_id ("1","2","3") and an optional reference datetime (defaults to now),
    return start_datetime and end_datetime for the current shift period.
    Handles shifts that cross midnight.
    """
    if ref_dt is None:
        ref_dt = datetime.now()
    s = SHIFTS.get(str(shift_id), SHIFTS["1"])
    start_t = s["start"]
    end_t = s["end"]

    # base date is today
    today = ref_dt.date()
    start_dt = datetime.combine(today, start_t)

    # if shift crosses midnight (start > end), end is next day
    if start_t > end_t:
        # end is on next day
        end_dt = datetime.combine(today + timedelta(days=1), end_t)
        # but if current time is before end_t (i.e., we're after midnight within the shift),
        # then the shift started yesterday
        if ref_dt.time() < end_t:
            start_dt = datetime.combine(today - timedelta(days=1), start_t)
            end_dt = datetime.combine(today, end_t)
    else:
        end_dt = datetime.combine(today, end_t)
        # if current time is after end (meaning we have moved to next shift), keep the same day's window
        # (we treat shift windows based on 'today' and shift pick)
    return start_dt, end_dt

def shift_hours_list(start_dt, end_dt):
    """
    Return a list of integer hours (0-23) that fall inside the shift window in chronological order.
    Example: 18:00-02:00 -> [18,19,20,21,22,23,0,1]
    """
    hours = []
    cur = start_dt
    # iterate by hours until cur >= end_dt
    while cur < end_dt:
        hours.append(cur.hour)
        cur += timedelta(hours=1)
    return hours

# ----------------- SSE (Live Updates) -----------------
def sse_stream():
    """Send live updates to front-end via SSE"""
    prev_status = None
    while True:
        with lock:
            gate = latest_gate.copy()
        if gate != prev_status:
            yield f"data: {json.dumps(gate)}\n\n"
            prev_status = gate
        time.sleep(1)

@app.route("/api/sse")
def api_sse():
    return Response(sse_stream(), mimetype="text/event-stream")

@app.route("/api/live")
def api_live():
    """
    Return live gate status + aggregates limited to a shift window.
    Query param: ?shift=1|2|3  (defaults to 1)
    """
    shift = request.args.get("shift", "1")
    try:
        start_dt, end_dt = compute_shift_window(shift)
    except Exception:
        # fallback to default shift 1
        start_dt, end_dt = compute_shift_window("1")

    with lock:
        latest = latest_gate.copy()

    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    # --- Fetch rows in shift window ---
    cur.execute("""
        SELECT id, gate_status, timestamp
        FROM gate_status
        WHERE timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
    """, (start_dt, end_dt))
    rows = cur.fetchall()

    # --- Compute open-close cycles (>1 min) within shift ---
    total_opens = total_closes = 0
    avg_duration = 0
    recent_events = []

    last_open_ts = None
    durations = []

    for r in rows:
        ts = r['timestamp']
        status = r['gate_status']
        # ensure we return ISO strings
        recent_events.append({'timestamp': ts.isoformat(), 'gate_status': status})

        if status == 'Open':
            last_open_ts = ts
        elif status == 'Closed' and last_open_ts:
            duration_sec = (ts - last_open_ts).total_seconds()
            if duration_sec >= 60:
                total_opens += 1
                total_closes += 1
                durations.append(duration_sec)
            last_open_ts = None  # reset

    if durations:
        avg_duration = sum(durations)/len(durations)
    else:
        avg_duration = 0

    # --- Hourly opens/closes and max open duration per hour (within shift) ---
    # Build hour buckets for the shift in order
    hours = shift_hours_list(start_dt, end_dt)
    hourly_dict = {h: {'opens':0, 'closes':0, 'max_open_duration':0} for h in hours}

    last_open_ts = None
    for r in rows:
        ts = r['timestamp']
        status = r['gate_status']
        hr = ts.hour
        # Only consider hours that belong to the shift (in case)
        if hr not in hourly_dict:
            # some events might be exactly at end boundary - map them if they are < end_dt
            # allow them if within the shift window
            if not (start_dt <= ts < end_dt):
                continue
            else:
                hourly_dict.setdefault(hr, {'opens':0,'closes':0,'max_open_duration':0})

        if status == 'Open':
            last_open_ts = ts
        elif status == 'Closed' and last_open_ts:
            duration_sec = (ts - last_open_ts).total_seconds()
            if duration_sec >= 60:
                # attribute this cycle to the hour where the CLOSED event occurred (as earlier code)
                hourly_dict[hr]['opens'] += 1
                hourly_dict[hr]['closes'] += 1
                if duration_sec > hourly_dict[hr]['max_open_duration']:
                    hourly_dict[hr]['max_open_duration'] = duration_sec
            last_open_ts = None

    hourly = []
    for h in hours:
        v = hourly_dict.get(h, {'opens':0,'closes':0,'max_open_duration':0})
        hourly.append({'hr': h, 'opens': v['opens'], 'closes': v['closes'], 'max_open_duration': v['max_open_duration']})

    # --- SMS counts (counts of messages of each status within the shift window) ---
    sms_open = sum(1 for r in rows if r['gate_status'] == 'Open')
    sms_close = sum(1 for r in rows if r['gate_status'] == 'Closed')

    cur.close()
    conn.close()

    return jsonify({
        "shift": shift,
        "shift_label": SHIFTS.get(str(shift), SHIFTS["1"])['label'],
        "shift_start": start_dt.isoformat(),
        "shift_end": end_dt.isoformat(),
        "latest": latest,
        "sms_open": sms_open,
        "sms_close": sms_close,
        "opens_shift": total_opens,
        "closes_shift": total_closes,
        "avg_open_duration": round(avg_duration, 2),
        "recent_events": recent_events[-10:],  # last 10 events in the shift window
        "hourly": hourly
    })

# ----------------- Routes -----------------
@app.route("/")
def home_page():
    return render_template("index.html", page="home")

@app.route("/live")
def live_page():
    return render_template("index.html", page="live")

@app.route("/history")
def history_page():
    return render_template("index.html", page="history")

# ----------------- Historical Data (Read-Only) -----------------
@app.route("/api/history")
def api_history():
    """Fetch historical gate status from MySQL (no writes)"""
    from_date = request.args.get("from")
    to_date = request.args.get("to")
    if not from_date or not to_date:
        return jsonify({"error": "Missing from/to date"}), 400

    conn = get_db_conn()
    cur = conn.cursor()
    # Raw rows
    cur.execute("""
        SELECT timestamp, gate_status
        FROM gate_status
        WHERE DATE(timestamp) BETWEEN %s AND %s
        ORDER BY timestamp ASC
    """, (from_date, to_date))
    rows = rows_to_dicts(cur, cur.fetchall())

    # Aggregates
    cur.execute("""
        SELECT DATE(timestamp) AS dt,
               SUM(gate_status='Open') AS opens,
               SUM(gate_status='Closed') AS closes
        FROM gate_status
        WHERE DATE(timestamp) BETWEEN %s AND %s
        GROUP BY DATE(timestamp)
        ORDER BY DATE(timestamp)
    """, (from_date, to_date))
    aggregates = rows_to_dicts(cur, cur.fetchall())

    cur.close()
    conn.close()
    return jsonify({"rows": rows, "aggregates": aggregates})

# ----------------- Run App -----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=True, threaded=True)
