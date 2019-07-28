# Fix errors in the collected data

Ideally, we would convert this to scripts that would run against the REST API.
But the timeseries doesn't support deleting entries right now
(https://github.com/e-mission/e-mission-docs/issues/366, and we will not want
to support editing or deleting entries using API calls until we have a more
robust authentication mechanism.

So for now, I'm documenting the commands that need to be run directly on the
server to edit/delete data. You will need to talk to the datastore maintainer
to use these - they can copy and paste them in an ipython console.

```
e-mission-server $ ./e-mission-ipy.bash
```

```
import emission.core.get_database as edb
from uuid import UUID
import bson.objectid as boi
```

### Resetting the start/end times for the experiment

```
curr_test_uuid = UUID(...)
list(edb.get_usercache_db().find({"user_id": curr_test_uuid, "metadata.key": "manual/evaluation_transition"}))

oid_to_change = boi.ObjectId(...)
edb.get_usercache_db().update_one({"_id": oid_to_change}, {"$set": {"data.label.start_fmt_date": "2019-06-13"}}, upsert=False)

edb.get_usercache_db().update_one({"_id": oid_to_change}, {"$set": {"data.start_ts": 1560384000}}, upsert=False)
```

### Delete an invalid/extraneous transition

```
curr_test_uuid = UUID(...)
list(edb.get_usercache_db().find({"user_id": curr_test_uuid, "metadata.key": "manual/evaluation_transition", "data.transition": 'STOP_CALIBRATION_PERIOD'}))

oid_to_change = boi.ObjectId(...)
edb.get_usercache_db().delete_one({"_id": oid_to_change})
```

OR

Find the timestamp of the entry to delete from the notebook (e.g. `('STOP_CALIBRATION_PERIOD', 'low_accuracy_stationary_run_4', 1560966154, <Arrow [2019-06-19T10:42:34-07:00]>)`)

```
In [26]: edb.get_usercache_db().find({"metadata.key": "manual/evaluation_transition", "d
    ...: ata.ts": <ts_from_notebook>}).count()
Out[26]: 1

In [31]: edb.get_usercache_db().delete_one({"metadata.key": "manual/evaluation_transitio
    ...: n", "data.ts": <ts_from_notebook>})
Out[31]: <pymongo.results.DeleteResult at 0x7fc75ed7da48>
```

### Delete an entire calibration or evaluation range

e.g. the one where I forgot to unplug the phone

```
tz = "America/Los_Angeles"

def delete_trip_data(user_id, spec_id, trip_id, is_dry_run=True):
    transitions = list(edb.get_usercache_db().find({"user_id": user_id, "metadata.key": "manual/evaluation_transition", "data.trip_id": trip_id, "data.spec_id": spec_id}))
    print("\n".join([str(t["data"].values()) for t in transitions]))
    ts_range_start = transitions[0]["metadata"]["write_ts"]
    if len(transitions) == 2:
        ts_range_end = transitions[1]["metadata"]["write_ts"]
    else:
        ts_range_end = arrow.now().timestamp
    print("%s -> %s" % (arrow.get(ts_range_start).to(tz), arrow.get(ts_range_end).to(tz)))
    print(edb.get_usercache_db().find({"user_id": user_id, 'metadata.write_ts': {'$lte': ts_range_end, '$gte': ts_range_start}}).count())
    print(edb.get_usercache_db().find({"user_id": user_id, 'metadata.write_ts': {'$lte': ts_range_end, '$gte': ts_range_start}}).distinct("metadata.key"))
    battery_list = list(edb.get_usercache_db()
            .find({"user_id": user_id, "metadata.key": "background/battery",
                'metadata.write_ts': {'$lte': ts_range_end, '$gte': ts_range_start}}))
    if "battery_level_ratio" in battery_list[0]["data"]:
        print([v["data"]["battery_level_ratio"] for v in battery_list])
    else:
        print([v["data"]["battery_level_pct"] for v in battery_list])
    if not is_dry_run:
        print(edb.get_usercache_db().delete_many({"user_id": user_id, 'metadata.write_ts': {'$lte': ts_range_end, '$gte': ts_range_start}}).raw_result)

delete_trip_data(UUID("..."), "high_accuracy_stationary_3")

phone_list = ["ucb-sdb-android-%d" % i for i in range(1,5)] + ["ucb-sdb-ios-%d
     ...: " % i for i in range(1,5)]

uuid_list = [ue["uuid"] for ue in list(edb.get_uuid_db().find({"user_email": {"$in": phone_list}}))]

for u in uuid_list:
    delete_trip_data(u["uuid"], "sfba_med_freq_calibration_only", "high_accuracy_stationary_2")
```

### Adjusting transition timestamp slightly

```
def adjust_transition_timestamp(user_id, transition, trip_id, delta):
    match_query = {"user_id": user_id, "data.transition": transition, "data.trip_id": trip_id}
    assert edb.get_usercache_db().find(match_query).count() == 1
    curr_entry = edb.get_usercache_db().find_one(match_query)
    print("Setting %s -> %s" % (match_query, curr_entry["data"]["ts"] + delta))

    edb.get_usercache_db().update_one(match_query,
        {"$set": {"data.ts": curr_entry["data"]["ts"] + delta}})

```

### Replace existing spec

If the existing spec has a lot of changes, it might be easiest to delete and
re-create it

```
del_spec_id = "many_unimodal_trips"
edb.get_usercache_db().find({"metadata.key": "config/evaluation_spec",
    "data.label.id": del_spec_id}).count()

edb.get_usercache_db().delete_one({"metadata.key": "config/evaluation_spec",
    "data.label.id": del_spec_id}).raw_result
```
