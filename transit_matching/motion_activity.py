import json
from pathlib import Path

def parse_android_motion_activity(data):
    output = {}
    if 'agb' in data:
        output['mode'] = data['agb']
    elif 'zzaEg' in data:
        output['mode'] = data['zzaEg']
    elif 'zzbjA' in data:
        output['mode'] = data['zzbjA']
    elif 'ajO' in data:
        output['mode'] = data['ajO']
    elif 'zzaKM' in data:
        output['mode'] = data['zzaKM']
    elif 'zzbhB' in data:
        output['mode'] = data['zzbhB']
    
    if 'agc' in data:
        output['confidence'] = data['agc']
    elif 'zzaEh' in data:
        output['confidence'] = data['zzaEh']
    elif 'zzbjB' in data:
        output['confidence'] = data['zzbjB']
    elif 'ajP' in data:
        output['confidence'] = data['ajP']
    elif 'zzaKN' in data:
        output['confidence'] = data['zzaKN']
    elif 'zzbhC' in data:
        output['confidence'] = data['zzbhC']
    
    return output

def parse_ios_motion_activity(data):
    output = {'mode': 4}
    if data['confidence'] == 'high':
        output['confidence'] = 100
    elif data['confidence'] == 'medium':
        output['confidence'] = 75
    elif data['confidence'] == 'low':
        output['confidence'] = 50
    
    if data['automotive']:
        output['mode'] = 0 
    elif data['cycling']:
        output['mode'] = 1
    elif data['walking'] or data['running']:
        output['mode'] = 2
    
    return output

def get_sensed_mode(datastore_loc, view, os, phone_str, start_ts, end_ts):
    motion_activity_dir = datastore_loc + phone_str + "/" + view + "/background~motion_activity"
    dir_path = Path(motion_activity_dir)

    modes = []

    if dir_path.is_dir():
        for entry in dir_path.iterdir():
            if entry.is_file():
                file = open(entry, 'r')
                content = json.loads(file.read())
                for point in content:
                    data = point['data']

                    if 'write_ts' not in data:
                        continue

                    
                    if start_ts < data['write_ts'] and data['write_ts'] < end_ts:
                        if os == 'android':
                            modes.append(parse_android_motion_activity(data))
                        if os == 'ios':
                            modes.append(parse_ios_motion_activity(data))


    weights = {}

    for point in modes:
        # 0: vehicle
        # 1: bicycle
        # 2: on_foot
        
        if point['mode'] > 2:
            continue

        if point['mode'] not in weights:
            weights[point['mode']] = 0
        
        weights[point['mode']] += round((point['confidence'] / 100) ** 2 * 100)

    if len(weights) == 0:
        return 2
    
    best_mode = max(weights, key=weights.get)
    return best_mode