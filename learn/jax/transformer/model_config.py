from typing import NamedTuple

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

    def count_parameters(self) -> int:
        """Count total number of trainable parameters"""
        # Embedding parameters
        embedding_params = self.vocab_size * self.hidden_dim  # Token embeddings
        embedding_params += self.max_seq_len * self.hidden_dim  # Position embeddings
        
        # Per layer parameters
        attention_params = 4 * (self.hidden_dim * self.hidden_dim)  # Q,K,V, and output projections
        mlp_params = 2 * (self.hidden_dim * self.mlp_dim)  # Two dense layers
        layer_norm_params = 4 * self.hidden_dim  # 2 layer norms per block
        
        # Total parameters
        return (embedding_params + 
                self.num_layers * (attention_params + mlp_params + layer_norm_params))

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
    "s": ModelConfig(
        vocab_size=50000,
        num_layers=12,
        num_heads=12,
        head_dim=64,
        mlp_dim=2048,
        max_seq_len=2048,
        dropout_rate=0.1,
        batch_size=1
    ),
    "m": ModelConfig(
        vocab_size=50000,
        num_layers=24,
        num_heads=16,
        head_dim=64,
        mlp_dim=4096,
        max_seq_len=2048,
        dropout_rate=0.1,
        batch_size=1
    ),
    "l": ModelConfig(
        vocab_size=50000,
        num_layers=32,
        num_heads=24,
        head_dim=96,
        mlp_dim=8192,
        max_seq_len=2048,
        dropout_rate=0.1,
        batch_size=1
    ),
    "xl": ModelConfig(
        vocab_size=50000,
        num_layers=48,
        num_heads=32,
        head_dim=128,
        mlp_dim=16384,
        max_seq_len=2048,
        dropout_rate=0.1,
        batch_size=1
    )
}

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Query transformer model configurations')
    parser.add_argument('model_name', type=str, choices=MODEL_CONFIGS.keys(),
                      help='Name of the model configuration to query')
    
    args = parser.parse_args()
    
    config = MODEL_CONFIGS[args.model_name]
    
    # Print configuration details
    print(f"\nConfiguration for model '{args.model_name}':")
    print("-" * 50)
    for field in config._fields:
        value = getattr(config, field)
        print(f"{field:15} : {value}")
    
    print("-" * 50)
    print(f"hidden_dim       : {config.hidden_dim}")
    params = config.count_parameters()
    print(f"Parameters      : {params:,} ({params/1e6:.1f}M)")
    print(f"Memory Usage    : {config.estimate_memory_usage():.2f} GB")

if __name__ == '__main__':
    main()

