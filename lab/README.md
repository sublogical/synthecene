## Notes

### Check CUDA Version Available

## Machine Setup: Ubuntu

Install Nvidia Drivers
```
sudo apt install nvidia-driver-525 nvidia-dkms-525
```
Reboot.

Make sure the driver is running & card is found
```
nvidia-smi
```
There should be a GPU. See it?


Verify Torch can see it
```
python
>>> import torch
>>> torch.cuda.is_available()
True
```
 


## TODO List

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
