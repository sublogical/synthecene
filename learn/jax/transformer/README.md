# TODO

- checkpointing
  - add tokenizer checkpointing
  - add checkpointing with ranking metrics
  - sharded/distributed checkpointing
  DONE - add checkpointing restore
  DONE - add basic checkpointing
  DONE - add checkpointing with metrics
  DONE - add checkpointing with optimizer state
- dataloader
  - switch dataloader to grain
  - refactor dataloader out of learn.py
  - add support for mixtures in dataloader
  - add support for shuffling
  - add support for sampling
  - add support for distributed training
- multimodal
- tokenizer
  - refactor tokenizer out of learn.py
  - support precomputing tokenizer
- model
  - distributed compute
- training loop
  - add learning rate scheduler
  - add early stopping
  - add support for distributed training
  - add tensorboard support
- inference
  - load checkpoint
  - inference API
    - single
    - batch
- distributed training
  - dataloader
  - model
  - training loop
  - checkpointing
  - model configuration
  DONE - refactor model configuration out of learn.py
  DONE - add cli for model configuration inspection
