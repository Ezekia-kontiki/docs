import os
import json

data_h = [
    {"id": 1, "nom": "nom1"},
    {"id": 2, "nom": "nom2"},
    {"id": 3, "nom": "nom3"}
]

json_file = 'data.json'

if not os.path.exists(json_file) or os.path.getsize(json_file) == 0:
    data = []
    last_isEvent = 0
else:
    with open(json_file, "r") as f:
        data = json.load(f)
        last_isEvent = data[-1].get("isEvent", 0) if data else 0


events = ["clicks", "opens","bounce"]
new_history = []

for event in events:
    print("Ajout event :", event)


    current_entry = []

    for item in data_h:
        entry = item.copy()
        entry["event"] = event
        entry["isEvent"] = 1
        current_entry.append(entry)

    new_history.append(current_entry)


data.extend(new_history)

with open(json_file, "w") as f:
    json.dump(data, f, indent=4)

print("OK")
