import json
from fetch import ConsumerAPI


api = ConsumerAPI(
    'BANK OF AMERICA, NATIONAL ASSOCIATION',
    date_received_max='2022-01-01',
    date_received_min='2019-01-01'
)
res = []
for data in api.fetch_data():
    if data:
        res.extend(data['hits']['hits'])

print(f"Downloaded {len(res)} items")
with open('export.json', 'w') as f:
    json.dump(res, f)
