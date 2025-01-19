



# HOWTO

## Python Environment

### TF/Jax Environment
```
conda activate tf-gpu
```

Verify TF GPU is working
```
python -c "import tensorflow as tf; print(tf.config.list_physical_devices('GPU'))"
```
Should see something like
```
[PhysicalDevice(name='/physical_device:GPU:0', device_type='GPU')]
```

Verify JAX GPU is working
```
python -c "import jax; print(jax.devices())"
```
Should see something like
```
[cuda(id=0)]
```

Verify PyTorch GPU is working
```
python -c "import torch; print(torch.cuda.is_available())"
```
Should see something like
```
True
```




### CONDA RECIPE
conda install jaxlib=0.4.35=cuda124py312* jax cuda-nvcc cudnn tensorflow[and-cuda]=2.17.0=cuda124py312* orbax-checkpoint flax optax -c conda-forge -c nvidia
conda install tensorflow-datasets  datasets
// conda install pytorch torchvision torchaudio pytorch-cuda=12.1 -c pytorch -c nvidia
conda install tokenizers




