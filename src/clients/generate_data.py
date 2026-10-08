import json

NUM_CLIENTS = 8
NUM_EVENTS = 50
EVENTS_WITH_TWO_OCCURRENCES = 20   
MONTHS = ["octubre", "noviembre", "diciembre"]

events = []
eventNumber = 1
for month in MONTHS:
    for e in range(1, NUM_EVENTS + 1):
        n_occ = 2 if e <= EVENTS_WITH_TWO_OCCURRENCES else 1
        events.append({
            "eventID": f"E{eventNumber}",
            "month":month,
            "occurrences": [f"E{eventNumber}-{o}" for o in range(1, n_occ + 1)],
        })
        eventNumber+=1

data = {
    "clients": [f"C{i:03d}" for i in range(1, NUM_CLIENTS + 1)],
    "events": events,
    "months": MONTHS,
}

with open("load_data.json", "w") as f:
    json.dump(data, f, indent=2)
