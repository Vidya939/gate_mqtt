from flask import Flask, render_template, jsonify, request, Response
import mysql.connector
from datetime import datetime
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
sms_counts = {"Open": 0, "Closed": 0}

def mqtt_on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Connected successfully")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"[MQTT] Connection failed with code {rc}")

def mqtt_on_message(client, userdata, msg):
    """Update in-memory gate status from MQTT messages (no DB writes!)"""
    global latest_gate, last_logged_status, sms_counts
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        gate_status = data.get("gate_status")
        ts = data.get("timestamp", datetime.now().isoformat())

        if gate_status and gate_status != last_logged_status:
            with lock:
                latest_gate["gate_status"] = gate_status
                latest_gate["timestamp"] = ts

                if gate_status == "Closed":
                    # duration since last Open
                    last_open_ts = datetime.fromisoformat(latest_gate.get("last_open_ts", ts))
                    duration_sec = (datetime.fromisoformat(ts) - last_open_ts).total_seconds()
                    latest_gate["duration"] = duration_sec
                else:
                    latest_gate["last_open_ts"] = ts
                    latest_gate["duration"] = 0

                # Update in-memory SMS counts
                sms_counts[gate_status] += 1

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
    """Return live gate status + today aggregates with proper cycle counting"""
    with lock:
        latest = latest_gate.copy()
        sms_open = sms_counts.get("Open", 0)
        sms_close = sms_counts.get("Closed", 0)

    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    # --- Fetch today's rows ---
    cur.execute("""
        SELECT id, gate_status, timestamp
        FROM gate_status
        WHERE DATE(timestamp) = CURDATE()
        ORDER BY id ASC
    """)
    rows = cur.fetchall()

    # --- Compute open-close cycles (>1 min) ---
    total_opens = total_closes = 0
    avg_duration = 0
    recent_events = []

    last_open_ts = None
    durations = []

    for r in rows:
        ts = r['timestamp']
        status = r['gate_status']
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

    # --- Hourly opens/closes (per cycle) ---
    hourly_dict = {}
    for r in rows:
        hour = r['timestamp'].hour
        if hour not in hourly_dict:
            hourly_dict[hour] = {'opens':0, 'closes':0, 'max_open_duration':0}
    last_open_ts = None
    for r in rows:
        ts = r['timestamp']
        status = r['gate_status']
        hour = ts.hour
        if status == 'Open':
            last_open_ts = ts
        elif status == 'Closed' and last_open_ts:
            duration_sec = (ts - last_open_ts).total_seconds()
            if duration_sec >= 60:
                hourly_dict[hour]['opens'] += 1
                hourly_dict[hour]['closes'] += 1
                if duration_sec > hourly_dict[hour]['max_open_duration']:
                    hourly_dict[hour]['max_open_duration'] = duration_sec
            last_open_ts = None

    hourly = []
    for h in sorted(hourly_dict.keys()):
        v = hourly_dict[h]
        hourly.append({'hr': h, 'opens': v['opens'], 'closes': v['closes'], 'max_open_duration': v['max_open_duration']})

    cur.close()
    conn.close()

    return jsonify({
        "latest": latest,
        "sms_open": sms_open,
        "sms_close": sms_close,
        "opens_today": total_opens,
        "closes_today": total_closes,
        "avg_open_duration": round(avg_duration, 2),
        "recent_events": recent_events[-10:],  # last 10 events
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
