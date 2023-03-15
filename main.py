import firebase_admin
from firebase_admin import credentials, firestore, db, storage
import threading
import google.auth.exceptions
import time

damage_1 = 0.0
damage_2 = 5.0
damage_3 = 10.0
damage_4 = 0.0
damage_5 = 0.0
damage_6 = 0.0
damage_7 = 0.0
damage_8 = 0.0
get_log = 0
user_id = ""
device_name = ""

class MyThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self._stop_event = threading.Event()
        self.name = name

    def run(self):
        try:
            while not self._stop_event.is_set():
                print("Thread is running...")
                bucket = storage.bucket()
                directory = f"user{user_id}/device{device_name}/log/"

                # loop over the files in the directory
                for blob in bucket.list_blobs(prefix=directory):
                    if blob.name.endswith('.csv'):
                        # download the CSV file as a string
                        content = blob.download_as_string()
                        # read the CSV string and parse it
                        print(content)
                time.sleep(1)
        except google.auth.exceptions.TransportError as ex:
            print(f"A TransportError occurred: {ex}")
        except Exception as ex:
            print(f"An unknown error occurred: {ex}")

    def stop(self):
        self._stop_event.set()

def start_thread():
    my_thread = MyThread("My Thread")
    my_thread.start()
    return my_thread

def stop_thread(thread):
    thread.stop()
    thread.join()


# Initialize Firebase Admin SDK with credentials
cred = credentials.Certificate("mollebalestra-4ef0c-b57a5061ae27.json")
firebase_admin.initialize_app(cred, {
    'projectId': 'mollebalestra-4ef0c',
    'databaseURL': 'https://mollebalestra-4ef0c-default-rtdb.europe-west1.firebasedatabase.app',
    'storageBucket': 'mollebalestra-4ef0c.appspot.com'
})

# Reference to the devices collection in Firestore
devices_ref = firestore.client().collection('devices')
#my_thread = threading.Thread(target=my_function)
# Loop through each device document in the collection
for device in devices_ref.stream():
    # Get the userId and device fields from the device document
    device_data = device.to_dict()
    user_id = device_data['user']
    device_name = device_data['device']

    # Construct the topic to subscribe to based on the device's userId and device name
    topic = f"user{user_id}/device{device_name}"
    topic2 = f"user{user_id}/device{device_name}/get_log"
    # Reference to the topic in Realtime Database
    topic_ref = db.reference(topic)
    topic_ref2 = db.reference(topic2)

    # Subscribe to the topic and start listening for changes
    def handle_change(event):
        # Get the value of the get_log variable
        try:
            print(event.data)
            get_log = event.data
            # if get_log equals 1, write some variables and change it back to 0
            if get_log == 1:
                print(f"Received request for logs for device {device_name}")
                # start the thread
                thread = start_thread()
                time.sleep(1)

                #calcola danneggiamento qui....


                stop_thread(thread)
                topic_ref.update({
                    'last_damage_1': str(damage_1),
                    'last_damage_2': str(damage_2),
                    'last_damage_3': str(damage_3),
                    'last_damage_4': str(damage_4),
                    'last_damage_5': str(damage_5),
                    'last_damage_6': str(damage_6),
                    'last_damage_7': str(damage_7),
                    'last_damage_8': str(damage_8),
                    'get_log': 0,
                })
                print(f"Logs for device {device_name} written and get_log reset to 0")
            else:
                print(f"Do nothing. get log set to 0 for device {device_name} ")

                '''if get_log == 1:
                    topic_ref.update({'get_log': 0})
                else:
                    pass'''
        except google.auth.exceptions.TransportError as ex:
            print(f"A TransportError occurred: {ex}")
        except Exception as ex:
            print(f"An unknown error occurred: {ex}")

    try:
        topic_ref2.listen(handle_change)
        #topic_ref.order_by_child('get_log').stream(handle_change)
    except Exception as e:
        print(e)

