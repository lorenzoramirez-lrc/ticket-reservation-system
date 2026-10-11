import json

NUM_CLIENTS = 5
NUM_EVENTS = 2
EVENTS_WITH_TWO_OCCURRENCES = 0
MONTHS = ["octubre"]

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
