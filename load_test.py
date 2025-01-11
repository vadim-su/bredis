import requests
import threading
import time

BASE_URL = "http://localhost:4123/keys"
NUM_THREADS = 50
NUM_REQUESTS = 100

request_times = []

def set_key(key, value):
    url = BASE_URL
    data = {"key": key, "value": value}
    start_time = time.time()
    response = requests.post(url, json=data)
    end_time = time.time()
    request_times.append(end_time - start_time)
    print(f"SET {key}: {response.status_code}")

def get_key(key):
    url = f"{BASE_URL}/{key}"
    start_time = time.time()
    response = requests.get(url)
    end_time = time.time()
    request_times.append(end_time - start_time)
    print(f"GET {key}: {response.status_code}")

def delete_key(key):
    url = f"{BASE_URL}/{key}"
    start_time = time.time()
    response = requests.delete(url)
    end_time = time.time()
    request_times.append(end_time - start_time)
    print(f"DELETE {key}: {response.status_code}")

def worker(thread_id):
    for i in range(NUM_REQUESTS):
        key = f"key-{thread_id}-{i}"
        value = f"value-{thread_id}-{i}"
        set_key(key, value)
        get_key(key)
        delete_key(key)

if __name__ == "__main__":
    threads = []
    start_time = time.time()

    for i in range(NUM_THREADS):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()
    total_requests = NUM_THREADS * NUM_REQUESTS * 3  # 3 requests per iteration (set, get, delete)
    average_request_time = sum(request_times) / total_requests

    print(f"Completed {total_requests} requests in {end_time - start_time} seconds")
    print(f"Average request time: {average_request_time} seconds")
