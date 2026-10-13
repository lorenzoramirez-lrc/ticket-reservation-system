"""
Archivo: generate_data.py
Objetivo: Generar el archivo load_data.json con los clientes, eventos, ocurrencias y meses que usan
          tanto los clientes de carga (client.py) como el llenado de la base de datos (fill_database.py)
"""

import json

# Parámetros de los datos a generar
NUM_CLIENTS = 5                    # Clientes disponibles (C001, C002, ...)
NUM_EVENTS = 2                     # Eventos por cada mes
EVENTS_WITH_TWO_OCCURRENCES = 0    # Cantidad de eventos (por mes) con dos ocurrencias; el resto tiene una sola
MONTHS = ["octubre"]               # Meses en los que se programan los eventos

# Construye la lista de eventos. El contador eventNumber no se reinicia entre meses,
# así que cada ID es único en todo el archivo (E1, E2, ...) y cada ocurrencia se nombra con el ID de su evento (E1-1, E1-2, ...)
events = []
eventNumber = 1
for month in MONTHS:
    for e in range(1, NUM_EVENTS + 1):
        # Los primeros EVENTS_WITH_TWO_OCCURRENCES eventos de cada mes tienen dos ocurrencias.
        n_occ = 2 if e <= EVENTS_WITH_TWO_OCCURRENCES else 1
        events.append({
            "eventID": f"E{eventNumber}",
            "month":month,
            "occurrences": [f"E{eventNumber}-{o}" for o in range(1, n_occ + 1)],
        })
        eventNumber+=1

# Estructura final del archivo: clientes (C001, C002, ...), eventos con sus ocurrencias y lista de meses
data = {
    "clients": [f"C{i:03d}" for i in range(1, NUM_CLIENTS + 1)],
    "events": events,
    "months": MONTHS,
}

# Guarda los datos en load_data.json con sangría de 2 espacios para que sea legible
with open("load_data.json", "w") as f:
    json.dump(data, f, indent=2)
