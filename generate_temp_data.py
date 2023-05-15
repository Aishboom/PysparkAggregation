import random
import datetime

metrics = ["temperature", "precipitation", "wind_speed"]

records = []
for i in range(200):
    metric = random.choice(metrics)
    value = random.uniform(0, 100)
    timestamp = datetime.datetime.now() - datetime.timedelta(minutes=random.randint(0, 1440))
    records.append((metric, value, timestamp.strftime("%Y-%m-%dT%H:%M:%S.000Z")))

with open("temp_data.csv", "w") as f:
    f.write("metric,value,timestamp\n")
    for record in records:
        f.write("{},{},{}\n".format(*record))
