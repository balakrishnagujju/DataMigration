import threading
import time

# Define a function that takes another function as a callback
def async_callback(task, callback, x):
    def operation():
        time.sleep(3)  # Simulate a long-running operation
        if(task()){
            callback()
        }else{
            async_callback(task,callback,x)
        }
    
    # Start the operation in a separate thread
    thread = threading.Thread(target=operation)
    thread.start()


