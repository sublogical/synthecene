import jax
import jax.numpy as jnp
from flax import linen as nn
from flax.training import train_state
import optax
from typing import Any, Callable, Tuple

class Trainer:
    def __init__(
        self,
        model: nn.Module,
        learning_rate: float = 1e-4,
        weight_decay: float = 0.01,
    ):
        self.model = model
        self.learning_rate = learning_rate
        self.weight_decay = weight_decay
        
    def create_train_state(self, rng: jax.random.PRNGKey, input_shape: Tuple) -> Tuple[train_state.TrainState, jax.random.PRNGKey]:
        # Split the RNG key for initialization and dropout
        rng, dropout_rng = jax.random.split(rng)
        
        # Create a dictionary with the dropout PRNG
        rngs = {'params': rng, 'dropout': dropout_rng}

        """Initialize the training state"""
        params = self.model.init(rngs, jnp.ones(input_shape, dtype=jnp.int32))['params']
        tx = optax.adamw(
            learning_rate=self.learning_rate,
            weight_decay=self.weight_decay
        )
        return train_state.TrainState.create(
            apply_fn=self.model.apply,
            params=params,
            tx=tx,
        ), rng
    
    @staticmethod
    def compute_loss(logits: jnp.ndarray, labels: jnp.ndarray) -> jnp.ndarray:
        """Compute cross entropy loss"""
        return optax.softmax_cross_entropy_with_integer_labels(logits, labels).mean()
    
    @staticmethod
    def compute_metrics(logits: jnp.ndarray, labels: jnp.ndarray) -> dict:
        """Compute metrics for evaluation"""
        loss = Trainer.compute_loss(logits, labels)
        accuracy = jnp.mean(jnp.argmax(logits, -1) == labels)
        return {'loss': loss, 'accuracy': accuracy}
    
    @staticmethod
    @jax.jit
    def train_step(
        state: train_state.TrainState,
        batch: Tuple[jnp.ndarray, jnp.ndarray],
        rng: jax.random.PRNGKey,
    ) -> Tuple[train_state.TrainState, jax.random.PRNGKey, dict]:
        """Single training step"""
        inputs, labels = batch
        
        rng, dropout_rng = jax.random.split(rng)

        def loss_fn(params):
            logits = state.apply_fn(
                {'params': params},
                inputs,
                rngs={'dropout': dropout_rng},
                deterministic=False,
            )
            loss = Trainer.compute_loss(logits, labels)
            return loss, logits
        
        grad_fn = jax.value_and_grad(loss_fn, has_aux=True)
        (loss, logits), grads = grad_fn(state.params)
        state = state.apply_gradients(grads=grads)
        metrics = Trainer.compute_metrics(logits, labels)
        
        return state, rng, metrics
    
    @staticmethod
    @jax.jit
    def eval_step(
        state: train_state.TrainState,
        batch: Tuple[jnp.ndarray, jnp.ndarray],
        rng: jax.random.PRNGKey,
    ) -> Tuple[jax.random.PRNGKey, dict]:
        rng, dropout_rng = jax.random.split(rng)

        """Single evaluation step"""
        inputs, labels = batch
        logits = state.apply_fn({'params': state.params}, inputs, rngs={'dropout': dropout_rng}, deterministic=True)
        return rng, Trainer.compute_metrics(logits, labels)

