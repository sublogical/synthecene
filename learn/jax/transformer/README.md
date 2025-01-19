# TODO

- model configuration
  DONE - refactor model configuration out of learn.py
  - add cli for model configuration inspection
- dataloader
  - refactor dataloader out of learn.py
  - add support for mixtures in dataloader
  - add support for shuffling
  - add support for sampling
  - add support for distributed training
- model
  - distributed compute
- training loop
  - add learning rate scheduler
  - add early stopping
  - add support for distributed training
  - add tensorboard support
- checkpointing
  - add checkpointing with ranking metrics
  - add checkpointing restore
  - sharded/distributed checkpointing
  DONE - add basic checkpointing
  DONE - add checkpointing with metrics
  DONE - add checkpointing with optimizer state
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
  