import json
from transit_matching.motion_activity import get_sensed_mode
from transit_matching.overpass_parser import build_maps

class AnalysisTaks:
    def __init__(self, datastore_loc, locations, view, os, phone_label, trip_name, section_name, evaluation_range):
        self.locations = locations
        self.view = view
        self.trip_name = trip_name
        self.section_name = section_name
        self.os = os
        self.phone_label = phone_label
        self.evaluation_range = evaluation_range

        self.stripped_trip_name = "_".join(self.trip_name.split("_")[:-1])
        self.stripped_section_name = "_".join(self.section_name.split("_")[:-1])

        self.initial_time = locations.iloc[0]['ts']
        self.final_time = locations.iloc[-1]['ts']

        self.phone_number = int(phone_label.split("-")[-1])

        self.sensed_mode = get_sensed_mode(datastore_loc, view, os, phone_label, self.initial_time, self.final_time)
        self.actual_mode = self.get_actual_mode()

        self.error = None
        self.predicted_mode = None
        self.path = None
        self.modern = None
    
    def __str__(self):
        return f'VIEW: {self.view.ljust(30)}PHONE: {(self.os + "-" + str(self.phone_number)).ljust(12)}TRIP: {self.stripped_trip_name.ljust(40)}SECTION: {self.stripped_section_name.ljust(40)}EVAL: {str(self.evaluation_range).ljust(4)}MODE: {self.predicted_mode.ljust(11)}| {self.actual_mode.ljust(11)}'

    def get_actual_mode(self):
        if 'walk' in self.section_name:
            return 'WALKING'

        f = open(f"spec_creation/final_sfbayarea/{self.view}.json", 'r')
        trips = json.loads(f.read())['evaluation_trips']

        trips_with_id = [n for n in trips if n['id'] == self.stripped_trip_name]
        if len(trips_with_id) == 0:
            return None
        trip = trips_with_id[0]
        
        if 'legs' in trip:
            sections_with_id = [n for n in trip['legs'] if n['id'] == self.stripped_section_name]
            if len(sections_with_id) == 0:
                return None
            section = sections_with_id[0]
        else:
            section = trip
        
        f.close()
        
        return section['mode']
        

    def map_match(self, map_data, error_function):
        if self.stripped_trip_name not in map_data:
            map_data[self.stripped_trip_name] = {}
        if self.stripped_section_name not in map_data[self.stripped_trip_name]:
            map_data[self.stripped_trip_name][self.stripped_section_name] = build_maps(self.locations)
        
        error, path, mode, modern, distances = error_function(self.locations, map_data[self.stripped_trip_name][self.stripped_section_name], self.sensed_mode)

        self.error = error
        self.path = path
        self.predicted_mode = mode
        self.modern = modern
        self.distances = distances
        
        
def create_tasks(pvs, datastore_loc):

    tasks = []

    for view, pv in pvs.items():
        for phone_os, phone_map in pv.map().items():
            for phone_label, phone_detail_map in phone_map.items():
                for evaluation_num in range(len(phone_detail_map["evaluation_ranges"])):
                    evaluation = phone_detail_map["evaluation_ranges"][evaluation_num]
                    for trip in evaluation["evaluation_trip_ranges"]:
                        for section in trip["evaluation_section_ranges"]:
                            locations = section['location_df']

                            if len(locations) <= 1:
                                continue
                                
                            task = AnalysisTaks(datastore_loc, locations, view, phone_os, phone_label, trip['trip_id'], section['trip_id'], evaluation_num)

                            # If this trip is not in the timeline, skip this task
                            if task.actual_mode is None:
                                continue

                            tasks.append(task)
                print(f'created all tasks for {phone_label} on {view}')
    
    return tasks