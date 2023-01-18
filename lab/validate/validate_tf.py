import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 
from tensorflow.python.client import device_lib

def is_tensorflow_gpu_installed():
    return len([device.name for device in device_lib.list_local_devices() if device.device_type == 'GPU']) > 0

if __name__ == '__main__':
    if is_tensorflow_gpu_installed():
        print('Tensorflow: OK')
    else:  
        print('GPU unavailable or runtime misconfigured.')
        exit(1)
