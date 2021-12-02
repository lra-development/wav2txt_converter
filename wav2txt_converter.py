#!/usr/bin/env python3.8

from scipy.io import wavfile
import numpy as np
import sys
import pathlib
import os
import multiprocessing as mp
import traceback
from tendo import singleton

def writeDataToFile(sample_rate, channel_data, base_file_name):
    try:
        with open(base_file_name, "w") as text_file:
            text_file.write(str(sample_rate) + "\n")
            np.savetxt(text_file, channel_data)
            text_file.close()
    except:
        print(f"exception was caught for file '{base_file_name}'")
        traceback.print_exc()

def writingFinishedCallback(some_value):
    assert (cond is not None)
    with cond:
        all_cores_used = (nAvailableCores == nProcesses.value)
        nProcesses.value -= 1
        if all_cores_used is True:
            cond.notify()

if __name__ == '__main__':
    try:
        instance = singleton.SingleInstance()
    except:
        sys.exit()
    process_id = os.getpid()
    nAvailableCores = len(os.sched_getaffinity(process_id))
    if nAvailableCores == 0:
        sys.exit()
    print("start of processing wav-files")
    file_path = pathlib.Path(__file__).parent.absolute()
    nProcesses = mp.Value('i', 1)
    cond = None
    pool = None
    results = []

    for subdir, dirs, files in os.walk(file_path):
        for file in files:
            if not file.endswith(".wav"):
                continue
            sample_rate = None
            file_data = None
            try:
                sample_rate, file_data = wavfile.read(file)
            except:
                print(f"exception was caught for file '{file}'")
                traceback.print_exc()
                continue
            file_name = file[:-4]
            if not file_name.endswith('_'):
                file_name += '_'
            nChannels = file_data.shape[1]
            if nChannels <= 0:
                continue
            nChannelsLen = len(str(nChannels))
            if (pool is None) and (nAvailableCores > 1):
                cond = mp.Condition()
                pool = mp.Pool(nAvailableCores - 1)
            for channel in range(nChannels):
                base_file_name = file_name
                base_file_name += "channel_"
                base_file_name += str(channel).zfill(nChannelsLen)
                base_file_name += ".txt"
                channel_data = file_data[:, channel]
                if nAvailableCores == 1:
                    writeDataToFile(sample_rate, channel_data, base_file_name)
                else:
                    assert (cond is not None)
                    with cond:
                        if nAvailableCores <= nProcesses.value:
                            cond.wait_for(lambda : nAvailableCores > nProcesses.value)
                        nProcesses.value += 1
                    assert (pool is not None)
                    result = pool.apply_async(
                        writeDataToFile,
                        (sample_rate, channel_data, base_file_name),
                        callback = writingFinishedCallback
                    )
                    results.append(result)

    if (pool is not None):
        for result in results:
            result.get()
        pool.close()
        pool.join()
    print("end of processing wav-files")
