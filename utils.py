import time
import logging
import functools
import json 


def timer(start_time, end_time):
    run_time = end_time - start_time    # 3
    run_time_in_minutes = round(run_time / 60, 3)

    return run_time, run_time_in_minutes
    

def timer_decorator(func):
    """
    Print the runtime of the decorated function
    """
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        # Start time - 1
        start_time = time.time()
        # Perform function
        value = func(*args, **kwargs)
        # Capture end time and subtract for run_time
        end_time = time.time()      # 2
        run_time, run_time_in_minutes = timer(start_time, end_time)

        logging.info(f'  --- Finished {func.__name__!r} in {run_time} seconds. [{run_time_in_minutes} minutes]')
        return value

    return wrapper_timer 

def logger(func):
    """
    Simple logging wrapper/decorator to use with functions.
    """
    @functools.wraps(func)
    def wrapper_logger(*args, **kwargs):
        value = func(*args, **kwargs)
        print('---------------------------------------')
        args=[*args]
        kwargs={**kwargs}
        logging.info(f'Logging info for {func.__name__!r}: kwargs: ' + json.dumps(kwargs, indent=4, sort_keys=True, default=str)
        + '\n Non-keyword arguments: ' + args)
        print('---------------------------------------')
        return value
        
    return wrapper_logger 
