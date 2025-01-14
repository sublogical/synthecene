import jax
import jax.numpy as jnp
from flax import linen as nn
from typing import Tuple, Optional

class MultiHeadAttention(nn.Module):
    num_heads: int
    head_dim: int
    dropout_rate: float = 0.0

    @nn.compact
    def __call__(self, x: jnp.ndarray, mask: Optional[jnp.ndarray] = None, training: bool = True):
        batch_size, seq_len, d_model = x.shape
        
        # Linear projections
        q = nn.Dense(self.num_heads * self.head_dim, name='q')(x)
        k = nn.Dense(self.num_heads * self.head_dim, name='k')(x)
        v = nn.Dense(self.num_heads * self.head_dim, name='v')(x)
        
        # Reshape for multi-head attention
        q = q.reshape(batch_size, seq_len, self.num_heads, self.head_dim)
        k = k.reshape(batch_size, seq_len, self.num_heads, self.head_dim)
        v = v.reshape(batch_size, seq_len, self.num_heads, self.head_dim)
        
        # Transpose to [batch_size, num_heads, seq_len, head_dim]
        q = jnp.transpose(q, (0, 2, 1, 3))
        k = jnp.transpose(k, (0, 2, 1, 3))
        v = jnp.transpose(v, (0, 2, 1, 3))
        
        # Scaled dot-product attention
        scale = jnp.sqrt(self.head_dim)
        attention = (q @ jnp.transpose(k, (0, 1, 3, 2))) / scale
        
        if mask is not None:
            attention = jnp.where(mask, attention, float('-inf'))
        
        attention = jax.nn.softmax(attention, axis=-1)
        attention = nn.Dropout(rate=self.dropout_rate)(attention, deterministic=not training)
        
        # Apply attention to values
        out = attention @ v
        
        # Reshape back
        out = jnp.transpose(out, (0, 2, 1, 3))
        out = out.reshape(batch_size, seq_len, self.num_heads * self.head_dim)
        
        # Final linear projection
        return nn.Dense(d_model)(out)

class TransformerBlock(nn.Module):
    num_heads: int
    head_dim: int
    mlp_dim: int
    dropout_rate: float = 0.1

    @nn.compact
    def __call__(self, x: jnp.ndarray, mask: Optional[jnp.ndarray] = None, training: bool = True, deterministic: bool = False):
        # Attention Block
        attention_output = MultiHeadAttention(
            num_heads=self.num_heads,
            head_dim=self.head_dim,
            dropout_rate=self.dropout_rate
        )(nn.LayerNorm()(x), mask, training)
        x = x + nn.Dropout(rate=self.dropout_rate)(attention_output, deterministic=deterministic)
        
        # MLP Block
        mlp_output = nn.Sequential([
            nn.Dense(self.mlp_dim),
            nn.gelu,
            nn.Dropout(rate=self.dropout_rate, deterministic=deterministic),
            nn.Dense(x.shape[-1])
        ])(nn.LayerNorm()(x))
        return x + nn.Dropout(rate=self.dropout_rate)(mlp_output, deterministic=deterministic)

class Transformer(nn.Module):
    vocab_size: int
    num_layers: int
    num_heads: int
    head_dim: int
    mlp_dim: int
    max_seq_len: int
    dropout_rate: float = 0.1

    @nn.compact
    def __call__(self, x: jnp.ndarray, training: bool = True, deterministic: bool = False):
        batch_size, seq_len = x.shape
        
        # Token and position embeddings
        token_embedding = nn.Embed(self.vocab_size, self.head_dim * self.num_heads)(x)
        position = jnp.arange(seq_len)[None, :]
        position_embedding = nn.Embed(self.max_seq_len, self.head_dim * self.num_heads)(position)
        
        x = token_embedding + position_embedding
        x = nn.Dropout(rate=self.dropout_rate)(x, deterministic=deterministic)
        
        # Create causal mask
        mask = jnp.tril(jnp.ones((seq_len, seq_len)))[None, None, :, :]
        
        # Transformer blocks
        for _ in range(self.num_layers):
            x = TransformerBlock(
                num_heads=self.num_heads,
                head_dim=self.head_dim,
                mlp_dim=self.mlp_dim,
                dropout_rate=self.dropout_rate
            )(x, mask, training)
        
        x = nn.LayerNorm()(x)
        return nn.Dense(self.vocab_size)(x) 