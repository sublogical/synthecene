## Notes

### Check CUDA Version Available

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


### Mamba

Install Mambaforge
```
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-$(uname)-$(uname -m).sh"
bash Mambaforge-$(uname)-$(uname -m).sh
```

Install Mamba
```
conda install mamba -n base -c conda-forge
```

Bootstrap lab environment
```
mamba create --name calico_lab python=3.9
mamba install -n calico_lab -c nvidia -c anaconda keras-gpu tensorflow-gpu
mamba install -n calico_lab pytorch torchvision torchaudio pytorch-cuda=11.6 -c pytorch -c nvidia
mamba install -n calico_lab -c nvidia -c anaconda keras-gpu tensorflow-gpu jax[cuda]=0.3.25 jaxlib
mamba install -n calico_lab tensorflow-datasets
mamba install -n calico_lab flax jraph dm-haiku
mamba install -n calico_lab scikit-learn scikit-image scikit-video

cd lab
conda env create
conda activate calico
```

Verify Torch can see it
```
python
>>> import torch
>>> torch.cuda.is_available()
True
```
 



## TODO List
* Build same model with torch, tf, jax
* Models to build
  * causal decoder only language model
  * extractive summarization model
* Get conda environment working with torch, cuda
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
