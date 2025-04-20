# TODO List


* AutoLabeler - label with LLMs
* AutoWorker - thinking model to construct workflows
* AutoRater - generalized FT / minified student models
* Adverse - adversarial play-based classification
* Lossifier - diagnose loss patterns from eval results
* SynthGen - synthetically generate datasets
  * DP
  * i18n
  * Tables




## LOW PRIORITY
* transformer
  * refactor model configuration out of learn.py
  * refactor dataloader out of learn.py
  * add support for mixtures in dataloader
  * implement inference
* proximal policy optimization
* models
  * calculate embeddings with pretrained LLM
  * reward model
  * fine-tune LLM on corpus
  * PPO optimization on pos/neg examples
  * prompt tuning
* inference serving
  * local container
  * inferentia
* training
  * train on trainium / sagemaker



* Build same model with torch, tf, jax
* Models to build
  * Jax + Flax CiFAR10 CNN: https://www.kaggle.com/code/aakashnain/building-models-in-jax-part2-flax
  * causal decoder only language model
  * extractive summarization model
* Experimental PoCs
  * Diffusion Models
    * Text to Image
    * Image to Image
    * Image to Image with masking
    * Image to Image with text instructions
    * Upscale output images
    * Variable aspect ratio images
    * Tiling images
    * Composition of multiple images
    * Fine-tune Diffusion Model on specific corpus


# HOWTO

## Machine Setup: Ubuntu

### Nvidia/Cuda
Install Nvidia Drivers
```
sudo apt install nvidia-driver-525 nvidia-dkms-525 nvidia-utils-525
sudo apt install nvidia-cuda-toolkit

```
Reboot.

Make sure the driver is running & card is found
```
nvidia-smi
```
There should be a GPU. See it?


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

***NOTE: THIS DOESN't WORK NOW FOR tf-gpu-3.12***



### CONDA RECIPE
conda install jaxlib=0.4.35=cuda124py312* jax cuda-nvcc cudnn tensorflow[and-cuda]=2.17.0=cuda124py312* orbax-checkpoint flax optax -c conda-forge -c nvidia
conda install tensorflow-datasets  datasets
// conda install pytorch torchvision torchaudio pytorch-cuda=12.1 -c pytorch -c nvidia
conda install tokenizers




