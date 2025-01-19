import os
os.environ['XLA_FLAGS'] = '--xla_gpu_cuda_data_dir="/home/sublogical/.miniconda3/envs/tf-gpu-3.12"'

# Standard library imports
import math
import argparse
from typing import Any, Callable, NamedTuple, Tuple

# JAX and related libraries
import jax
import jax.numpy as jnp
from orbax import checkpoint as ocp
from flax.training import train_state
from orbax.checkpoint import checkpoint_utils

# Data processing and ML libraries
import numpy as np

from torch.utils.data import DataLoader
from datasets import load_dataset

# Local imports
from model import Transformer
from trainer import Trainer
from model_config import ModelConfig, MODEL_CONFIGS

jax.config.update('jax_default_matmul_precision', 'high')

from tokenizers import Tokenizer
from tokenizers.models import BPE
from tokenizers.trainers import BpeTrainer
from tokenizers.pre_tokenizers import Whitespace

def create_data_loader(config: ModelConfig, split: str = "train", max_samples: int = None):
    """Create a dataloader for wikitext-2 dataset"""
    dataset = load_dataset("wikitext", "wikitext-2-raw-v1")[split]
    if max_samples:
        dataset = dataset.select(range(max_samples))

    # Initialize and train tokenizer
    tokenizer = Tokenizer(BPE(unk_token="[UNK]"))
    tokenizer.pre_tokenizer = Whitespace()
    trainer = BpeTrainer(
        vocab_size=config.vocab_size,
        special_tokens=["[UNK]", "[PAD]"],
    )
    
    # Train the tokenizer
    tokenizer.train_from_iterator(
        [text for text in dataset["text"] if text.strip()],
        trainer=trainer
    )

    def tokenize_and_chunk(text):
        if not text.strip():
            return None
        # Encode the text to token IDs
        encoded = tokenizer.encode(text)
        tokens = encoded.ids
        if len(tokens) < 2:  # Skip very short sequences
            return None
        
        # Create input/output pairs for language modeling
        chunks = []
        stride = 32  # Can be adjusted - smaller stride means more overlap
        
        # Generate overlapping chunks using sliding window
        for i in range(0, max(1, len(tokens) - config.max_seq_len + 1), stride):
            chunk = tokens[i:i + config.max_seq_len]
            # Pad if necessary
            if len(chunk) < config.max_seq_len:
                chunk = chunk + [1] * (config.max_seq_len - len(chunk))  # 1 is [PAD] token
            chunks.append(chunk)
        
        return chunks if chunks else None

    # Process dataset
    sequences = []
    for text in dataset["text"]:
        chunks = tokenize_and_chunk(text)
        if chunks:
            sequences.extend(chunks)

    # Convert to numpy array
    sequences = np.array(sequences)

    # Create input/output pairs
    inputs = sequences[:, :-1]
    labels = sequences[:, 1:]

    # Create dataloader
    class TextDataset:
        def __init__(self, inputs, labels):
            self.inputs = inputs
            self.labels = labels
        
        def __len__(self):
            return len(self.inputs)
        
        def __getitem__(self, idx):
            return self.inputs[idx], self.labels[idx]

    dataset = TextDataset(inputs, labels)
    return DataLoader(dataset, batch_size=config.batch_size, shuffle=True)

def main():
    parser = argparse.ArgumentParser(description='Train a transformer model')
    parser.add_argument('--model-size', type=str, default='xxs',
                      choices=MODEL_CONFIGS.keys(),
                      help='Model size configuration to use')
    parser.add_argument('--learning-rate', type=float, default=1e-4,
                      help='Learning rate')
    parser.add_argument('--weight-decay', type=float, default=0.01,
                      help='Weight decay for AdamW optimizer')
    parser.add_argument('--epochs', type=int, default=3,
                      help='Number of epochs to train')
    parser.add_argument('--max-samples', type=int, default=None,
                      help='Maximum number of samples to use (for testing)')
    parser.add_argument('--checkpoint-dir', type=str, default='checkpoints',
                      help='Directory to save checkpoints')
    parser.add_argument('--resume-checkpoint', type=str, default=None,
                      help='Specific checkpoint step to resume from (default: latest)')
    parser.add_argument('--max-checkpoints', type=int, default=3,
                      help='Maximum number of checkpoints to keep')
    parser.add_argument('--checkpoint-interval', type=int, default=2,
                      help='Save checkpoint every N steps')
    args = parser.parse_args()

    # Get model configuration
    config = MODEL_CONFIGS[args.model_size]
    print(f"Using model configuration: {args.model_size}")
    print(f"Config details: {config}")
    print(f"Estimated memory usage: {config.estimate_memory_usage():.2f} GB")

    # Initialize model
    model = Transformer(
        vocab_size=config.vocab_size,
        num_layers=config.num_layers,
        num_heads=config.num_heads,
        head_dim=config.head_dim,
        mlp_dim=config.mlp_dim,
        max_seq_len=config.max_seq_len,
        dropout_rate=config.dropout_rate
    )

    # Initialize trainer
    trainer = Trainer(
        model=model,
        learning_rate=args.learning_rate,
        weight_decay=args.weight_decay,
        rng = jax.random.PRNGKey(0),
        input_shape = (config.batch_size, config.max_seq_len)
    )

    # Initialize checkpoint manager
    options = ocp.CheckpointManagerOptions(
        max_to_keep=args.max_checkpoints, 
        save_interval_steps=args.checkpoint_interval
    )
    mngr = ocp.CheckpointManager(
        args.checkpoint_dir, options=options, item_names=('state', 'epoch_metrics')
    )

    start_epoch = 0
    loaded_metrics = None

    if args.resume_checkpoint == "latest":
        start_epoch = mngr.latest_step()
    elif args.resume_checkpoint is not None:
        start_epoch = int(args.resume_checkpoint)

    if start_epoch > 0:
        restored = mngr.restore(start_epoch, args=ocp.args.Composite(
            state=ocp.args.StandardRestore(trainer.get_abstract_state()),
            epoch_metrics=ocp.args.JsonRestore(trainer.get_abstract_metrics())
        ))
        trainer.restore_state(restored.state)

        print(f"Resumed from epoch {start_epoch}")
        print(f"Loaded metrics: {restored.epoch_metrics}")
    else:
        print("Starting training from scratch")

    print(f"Model initialized with {trainer.count_parameters():,} parameters")

    # Create data loaders
    train_loader = create_data_loader(config, "train", args.max_samples)
    val_loader = create_data_loader(config, "validation", args.max_samples)

    # Training loop
    for epoch in range(start_epoch, args.epochs):
        print(f"\nEpoch {epoch+1}/{args.epochs}")
        
        # Training
        train_metrics = trainer.train_epoch(train_loader, epoch+1)
        train_metrics = {k: float(np.mean([m[k] for m in train_metrics])) for k in train_metrics[0].keys()}
        print(f"Training metrics: {train_metrics}")
        
        # Validation
        val_metrics = trainer.eval_epoch(val_loader, epoch+1)
        val_metrics = {k: float(np.mean([m[k] for m in val_metrics])) for k in val_metrics[0].keys()}
        print(f"Validation metrics: {val_metrics}")

        # Save checkpoint
        metrics = {
            'train': train_metrics,
            'validation': val_metrics
        }
        state_dict = trainer.get_state_dict()

        mngr.save(
            epoch+1,
            args=ocp.args.Composite(
                state=ocp.args.StandardSave(trainer.get_state_dict()),
                epoch_metrics=ocp.args.JsonSave(metrics),
            ),
        )
        
    print(f"Waiting for checkpoint manager to finish")
    mngr.wait_until_finished()

if __name__ == "__main__":
    main() 

