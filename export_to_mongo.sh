#!/usr/bin/env bash

for i in $(seq 12); do
  wget -c "http://nyctaxitrips.blob.core.windows.net/data/trip_data_${i}.csv.zip"
  wget -c "http://nyctaxitrips.blob.core.windows.net/data/trip_fare_${i}.csv.zip"
done

for i in *.zip; do
  unzip -o "${i}"
done

for i in trip_data_*.csv; do
  mongoimport -d nyctaxi -c data --file "${i}" --headerline --type=csv --drop
done

for i in trip_fare_*.csv; do
  mongoimport -d nyctaxi -c fare --file "${i}" --headerline --type=csv --drop
done

mongo nyctaxi --eval '
db.data.find({pickup_datetime: {$type: 2}}).forEach(function(doc) {
  doc.pickup_datetime = new ISODate(doc.pickup_datetime);
  doc.dropoff_datetime = new ISODate(doc.dropoff_datetime);
  doc.pickup = { "type": "Point", "coordinates": [doc.pickup_longitude, doc.pickup_latitude] };
  delete doc.pickup_longitude;
  delete doc.pickup_latitude;
  doc.dropoff = { "type": "Point", "coordinates": [doc.dropoff_longitude, doc.dropoff_latitude] };
  delete doc.dropoff_longitude;
  delete doc.dropoff_latitude;
  db.data.update({_id: doc._id}, doc);
});
'

mongo nyctaxi --eval '
db.fare.find({pickup_datetime: {$type: 2}}).forEach(function(doc) {
  doc.pickup_datetime = new ISODate(doc.pickup_datetime);
  db.fare.update({_id: doc._id}, doc);
});
'
