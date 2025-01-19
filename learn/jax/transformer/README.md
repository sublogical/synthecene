# TODO

- model configuration
  - refactor model configuration out of learn.py
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
- checkpointing
  - add basic checkpointing
  - add checkpointing with ranking metrics
  - add checkpointing restore
  - sharded/distributed checkpointing
  DONE - add checkpointing with metrics
  DONE - add checkpointing with optimizer state
- inference
  - inference service
    - gRPC
    - REST
    - batch inference
    - caching
    - prefix caching
  - inference client
- distributed training
  - dataloader
  - model
  - training loop
  - checkpointing
  