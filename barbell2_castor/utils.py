import math
import time


def current_time_millis():
    return int(round(time.time() * 1000))


def current_time_secs():
    return int(round(current_time_millis() / 1000.0))


def elapsed_millis(start_time_millis):
    return current_time_millis() - start_time_millis


def elapsed_secs(start_time_secs):
    return current_time_secs() - start_time_secs


def duration(seconds):
    h = int(math.floor(seconds/3600.0))
    remainder = seconds - h * 3600
    m = int(math.floor(remainder/60.0))
    remainder = remainder - m * 60
    s = int(math.floor(remainder))
    return '{} hours, {} minutes, {} seconds'.format(h, m, s)
