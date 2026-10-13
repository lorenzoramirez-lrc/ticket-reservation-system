"""
Archivo: fill_database.py
Objetivo: Llenar la base de datos PostgreSQL con los eventos y las ocurrencias definidos en load_data.json
"""

import json
import os
from datetime import datetime
import psycopg2

# Asientos de cada ocurrencia, y año de las fechas
SEATS_PER_OCCURRENCE = 100
YEAR = 2026

# Número de cada mes, para armar la fecha de cada ocurrencia
MONTH_NUMBERS = {"octubre": 10, "noviembre": 11, "diciembre": 12}

# Archivo de datos de carga. La ruta se calcula a partir de la ubicación de este script,
# así que no depende de la carpeta desde donde se ejecute
DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "clients", "load_data.json")

with open(DATA_FILE) as f:
    data = json.load(f)

# Conexión con PostgreSQL (contenedor de Docker). La contraseña se lee de la variable de entorno DB_PASSWORD
# y, si no está definida, usa la del docker-compose (root)
conn = psycopg2.connect(
    host="localhost",           
    port=5432,
    dbname="event-reservation",
    user="root",
    password=os.environ.get("DB_PASSWORD", "root"),
)

# Cursor: objeto con el que se ejecutan las instrucciones SQL 
cur = conn.cursor()

# Deja la BD en estado inicial: vacía las tres tablas a la vez
cur.execute("TRUNCATE reservations, event_occurrences, events")

# Inserta cada evento y sus ocurrencias. El evento va primero porque
# la tabla event_occurrences tiene una llave foránea hacia events
for event in data["events"]:
    cur.execute("INSERT INTO events (event_id, name) VALUES (%s, %s)", (event["eventID"], f"Evento {event['eventID']}"),)
    month = MONTH_NUMBERS[event["month"]]

    for i, occurrenceID in enumerate(event["occurrences"]):
        # Primera ocurrencia el día 5 y segunda el día 15, siempre dentro del mismo mes
        day = 5 + 10 * i
        # occupied_seats no se indica: por defecto vale 0. Hora fija: 18:00
        cur.execute(
            "INSERT INTO event_occurrences (occurrence_id, event_id, date_time, capacity) "
            "VALUES (%s, %s, %s, %s)",
            (occurrenceID, event["eventID"], datetime(YEAR, month, day, 18, 0),
             SEATS_PER_OCCURRENCE),
        )

# Confirma los cambios y cierra el cursor y la conexión
conn.commit()
cur.close()
conn.close()
