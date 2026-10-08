import json
import os
from datetime import datetime
import psycopg2

SEATS_PER_OCCURRENCE = 100
YEAR = 2026
MONTH_NUMBERS = {"octubre": 10, "noviembre": 11, "diciembre": 12}

with open("load_data.json") as f:
    data = json.load(f)

conn = psycopg2.connect(
    host="localhost",           
    port=5432,
    dbname="event-reservation",
    user="root",
    password=os.environ.get("DB_PASSWORD", "root"),
)
cur = conn.cursor()

cur.execute("TRUNCATE reservations, event_occurrences, events")

for event in data["events"]:
    cur.execute("INSERT INTO events (event_id, name) VALUES (%s, %s)", (event["eventID"], f"Evento {event['eventID']}"),)
    month = MONTH_NUMBERS[event["month"]]

    for i, occurrenceID in enumerate(event["occurrences"]):
        day = 5 + 10 * i
        cur.execute(
            "INSERT INTO event_occurrences (occurrence_id, event_id, date_time, capacity) "
            "VALUES (%s, %s, %s, %s)",
            (occurrenceID, event["eventID"], datetime(YEAR, month, day, 18, 0),
             SEATS_PER_OCCURRENCE),
        )

conn.commit()
cur.close()
conn.close()
