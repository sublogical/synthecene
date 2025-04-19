import torch

def is_torch_gpu_installed():
    try:
        return torch.cuda.is_available()
    except:
        return False

if __name__ == '__main__':

    if is_torch_gpu_installed():
        print('PyTorch: OK')
        exit(0)
    else:  
        print('PyTorch: NOT OK')
        exit(1)
