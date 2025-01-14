# Standard library imports
import os
import math
import argparse
from typing import Any, Callable, NamedTuple, Tuple

# JAX and related libraries
import jax
import jax.numpy as jnp
import optax
from flax.training import train_state

# Data processing and ML libraries
import numpy as np
import tensorflow as tf
from torch.utils.data import DataLoader
from datasets import load_dataset

# Local imports
from model import Transformer
from trainer import Trainer


class ModelConfig(NamedTuple):
    vocab_size: int
    num_layers: int 
    num_heads: int
    head_dim: int
    mlp_dim: int
    max_seq_len: int
    dropout_rate: float
    batch_size: int

    @property
    def hidden_dim(self) -> int:
        return self.num_heads * self.head_dim

    def estimate_memory_usage(self) -> float:
        """Estimate memory usage in GB"""
        # Parameter count
        embedding_params = self.vocab_size * self.hidden_dim  # Token embeddings
        embedding_params += self.max_seq_len * self.hidden_dim  # Position embeddings
        
        # Per layer parameters
        attention_params = 4 * (self.hidden_dim * self.hidden_dim)  # Q,K,V, and output projections
        mlp_params = 2 * (self.hidden_dim * self.mlp_dim)  # Two dense layers
        layer_norm_params = 4 * self.hidden_dim  # 2 layer norms per block
        
        total_params = (embedding_params + 
                       self.num_layers * (attention_params + mlp_params + layer_norm_params))
        
        # Estimate activation memory (batch_size * seq_len * hidden_dim * num_layers)
        activations = self.batch_size * self.max_seq_len * self.hidden_dim * self.num_layers * 4
        
        # Convert to GB (assuming float32)
        return (total_params + activations) * 4 / (1024**3)

# Model size configurations
MODEL_CONFIGS = {
    "nano": ModelConfig(
        vocab_size=32000,
        num_layers=3,
        num_heads=4,
        head_dim=32,
        mlp_dim=256,
        max_seq_len=256,
        dropout_rate=0.1,
        batch_size=4
    ),
    "xxs": ModelConfig(
        vocab_size=32000,
        num_layers=4,
        num_heads=4,
        head_dim=32,
        mlp_dim=512,
        max_seq_len=512,
        dropout_rate=0.1,
        batch_size=4
    ),
    "xs": ModelConfig(
        vocab_size=32000,
        num_layers=6,
        num_heads=8,
        head_dim=64,
        mlp_dim=1024,
        max_seq_len=1024,
        dropout_rate=0.1,
        batch_size=2
    ),
    "base": ModelConfig(
        vocab_size=50000,
        num_layers=12,
        num_heads=12,
        head_dim=64,
        mlp_dim=2048,
        max_seq_len=2048,
        dropout_rate=0.1,
        batch_size=1
    )
}

def create_data_loader(config: ModelConfig, split: str = "train", max_samples: int = None):
    """Create a dataloader for wikitext-2 dataset"""
    dataset = load_dataset("wikitext", "wikitext-2-raw-v1")[split]
    if max_samples:
        dataset = dataset.select(range(max_samples))

    # Load tokenizer
    tokenizer = tf.keras.preprocessing.text.Tokenizer(num_words=config.vocab_size)
    tokenizer.fit_on_texts([text for text in dataset["text"] if text.strip()])

    def tokenize_and_chunk(text):
        if not text.strip():
            return None
        tokens = tokenizer.texts_to_sequences([text])[0]
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
                chunk = chunk + [0] * (config.max_seq_len - len(chunk))
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

def train_epoch(state, dataloader, epoch, rng) -> Tuple[train_state.TrainState, jax.random.PRNGKey, dict]:
    """Train for one epoch"""
    metrics = []
    for batch_idx, (inputs, labels) in enumerate(dataloader):
        inputs = jnp.array(inputs)
        labels = jnp.array(labels)
        state, rng, batch_metrics = Trainer.train_step(state, (inputs, labels), rng)
        metrics.append(batch_metrics)
        
        if batch_idx % 100 == 0:
            avg_metrics = {k: float(np.mean([m[k] for m in metrics])) for k in metrics[0].keys()}
            print(f"Epoch {epoch}, Batch {batch_idx}: {avg_metrics}")
    
    return state, rng, metrics

def eval_epoch(state, dataloader, epoch, rng) -> Tuple[jax.random.PRNGKey, dict]:
    """Evaluate for one epoch"""
    metrics = []
    for batch_idx, (inputs, labels) in enumerate(dataloader):
        inputs = jnp.array(inputs)
        labels = jnp.array(labels)
        rng, batch_metrics = Trainer.eval_step(state, (inputs, labels), rng)
        metrics.append(batch_metrics)
        
        if batch_idx % 100 == 0:
            avg_metrics = {k: float(np.mean([m[k] for m in metrics])) for k in metrics[0].keys()}
            print(f"Eval Epoch {epoch}, Batch {batch_idx}: {avg_metrics}")
    
    return rng, metrics

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
        weight_decay=args.weight_decay
    )

    # Initialize training state
    rng = jax.random.PRNGKey(0)
    input_shape = (config.batch_size, config.max_seq_len)
    state, rng = trainer.create_train_state(rng, input_shape)

    print(f"Model initialized with {sum(x.size for x in jax.tree_util.tree_leaves(state.params)):,} parameters")

    # Create data loaders
    train_loader = create_data_loader(config, "train", args.max_samples)
    val_loader = create_data_loader(config, "validation", args.max_samples)

    # Training loop
    for epoch in range(args.epochs):
        print(f"\nEpoch {epoch+1}/{args.epochs}")
        
        # Training
        state, rng, train_metrics = train_epoch(state, train_loader, epoch+1, rng)
        train_metrics = {k: float(np.mean([m[k] for m in train_metrics])) for k in train_metrics[0].keys()}
        print(f"Training metrics: {train_metrics}")
        
        # Validation
        rng, val_metrics = eval_epoch(state, val_loader, epoch+1, rng)
        val_metrics = {k: float(np.mean([m[k] for m in val_metrics])) for k in val_metrics[0].keys()}
        print(f"Validation metrics: {val_metrics}")

if __name__ == "__main__":
    main() 