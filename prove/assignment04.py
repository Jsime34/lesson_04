"""
Course    : CSE 351
Assignment: 04
Student   : Jaime Gutierrez

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0

"""

import time
from common import *
from threading import Thread, Lock
from queue import Queue
from cse351 import *
from collections import defaultdict

WORKERS = 110
THREADS = 250             # TODO - set for your program
RECORDS_TO_RETRIEVE = 5000  # Don't change


# ---------------------------------------------------------------------------
def retrieve_weather_data(command_queue, data_queue):
    # TODO - fill out this thread function (and arguments)
    while True:
        try:
            city, recno = command_queue.get(timeout=1)
        except:
            break

        url = f'{TOP_API_URL}/record/{city}/{recno}'
        record = get_data_from_server(url)

        if not record:
            print(f"[WARN] Empty record for {city}-{recno}")
            command_queue.task_done()
            continue

        if isinstance(record, dict) and 'date' in record and 'temp' in record:
            date = record['date']
            temp_r = record['temp']
        else:
            print(f"[WARN] Unexpected record format: {record}")
            command_queue.task_done()
            continue

        try:
            temp = float(temp_r)
            print(f"[Producer] Got: {city} {date} {temp}")
            data_queue.put((city, date, temp))
        except ValueError:
            print(f"[ERROR] Bad temperature: {temp_r}")

        command_queue.task_done()
# ---------------------------------------------------------------------------
# TODO - Create Worker threaded class
class WeatherWorker(Thread):
    def __init__(self, queue, noaa):
        super().__init__()
        self.queue = queue
        self.noaa = noaa

    def run(self):
        while True:
            item = self.queue.get()
            if item is None:
                break
            city, date, temp = item
            print(f"Worker processing: {city}, {date}, {temp}")
            self.noaa.add_record(city, date, temp)
            self.queue.task_done()


# ---------------------------------------------------------------------------
# TODO - Complete this class
class NOAA:

    def __init__(self):
        self.city_data = defaultdict(list)
        self.lock = Lock()

    def add_record(self, city, date, temp):
        with self.lock:
            print(f"[NOAA] Adding record for {city}: {date} -> {temp}")
            self.city_data[city].append((date, temp))

    def get_temp_details(self, city):
        with self.lock:
            records = self.city_data.get(city, [])
            if not records:
                return 0.0
            total_temp = sum(temp for _, temp in records if temp is not None)
            return total_temp / len(records)


# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):

    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():

    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    command_queue = Queue()
    data_queue = Queue(maxsize=10)
    noaa = NOAA()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]["records"]:,}')
    print('===================================')

    # TODO - Create any queues, pipes, locks, barriers you need

    for city in CITIES:
        max_recs = city_details[city]["records"]
        for recno in range(min(RECORDS_TO_RETRIEVE, max_recs)):
            command_queue.put((city, recno))

    workers = [WeatherWorker(data_queue, noaa) for _ in range(WORKERS)]
    for w in workers:
        w.start()
    
    producer_thread = [Thread(target=retrieve_weather_data, args=(command_queue, data_queue)) for _ in range(THREADS)]
    for p in producer_thread:
        p.start()
    
    command_queue.join()
    for p in producer_thread:
        p.join()

    for _ in workers:
        data_queue.put(None)

    for w in workers:
        w.join()
    

    # End server - don't change below
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()

