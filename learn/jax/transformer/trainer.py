import jax
import jax.numpy as jnp
from flax import linen as nn
from flax.training import train_state
import optax
from typing import Any, Callable, Tuple

# Data processing and ML libraries
import numpy as np

class Trainer:
    def __init__(
        self,
        model: nn.Module,
        learning_rate: float = 1e-4,
        weight_decay: float = 0.01,
        rng: jax.random.PRNGKey = jax.random.PRNGKey(0),
        input_shape: Tuple[int, int] = (1, 1),
    ):
        self.model = model
        self.learning_rate = learning_rate
        self.weight_decay = weight_decay
        self.rng = rng
        self.input_shape = input_shape
    
        # Split the RNG key for initialization
        self.rng, init_rng = jax.random.split(self.rng)

        # Initialize the training state
        variables = self.model.init(init_rng, jnp.ones(self.input_shape, dtype=jnp.int32))
        params = variables['params']

        tx = optax.adamw(
            learning_rate=self.learning_rate,
            weight_decay=self.weight_decay
        )

        self.state = train_state.TrainState.create(
            apply_fn=self.model.apply,
            params=params,
            tx=tx
        )

    def restore_state(self, checkpoint: dict):
        self.state.replace(
            step=checkpoint['step'],
            params=checkpoint['params'],
            opt_state=checkpoint['opt_state']
        )
    
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

        def loss_fn(params):
            logits = state.apply_fn(
                {'params': params},
                inputs,
                rngs={'dropout': rng},
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

    def train_epoch(self, dataloader, epoch) -> dict:
        """Train for one epoch"""
        metrics = []
        for batch_idx, (inputs, labels) in enumerate(dataloader):
            inputs = jnp.array(inputs)
            labels = jnp.array(labels)
            
            self.state, self.rng, batch_metrics = Trainer.train_step(self.state, (inputs, labels), self.rng)
            metrics.append(batch_metrics)
            
            if batch_idx % 100 == 0:
                avg_metrics = {k: float(np.mean([m[k] for m in metrics])) for k in metrics[0].keys()}
                print(f"Epoch {epoch}, Batch {batch_idx}: {avg_metrics}")
        
        return metrics

    def eval_epoch(self, dataloader, epoch) -> dict:
        """Evaluate for one epoch"""
        metrics = []
        for batch_idx, (inputs, labels) in enumerate(dataloader):
            inputs = jnp.array(inputs)
            labels = jnp.array(labels)
            self.rng, batch_metrics = Trainer.eval_step(self.state, (inputs, labels), self.rng)
            metrics.append(batch_metrics)
            
            if batch_idx % 100 == 0:
                avg_metrics = {k: float(np.mean([m[k] for m in metrics])) for k in metrics[0].keys()}
                print(f"Eval Epoch {epoch}, Batch {batch_idx}: {avg_metrics}")
        
        return metrics

    def get_state_dict(self) -> dict:
        """Create a state dictionary for checkpointing"""
        return {
            'step': self.state.step,
            'params': self.state.params,
            'opt_state': self.state.opt_state,
            'rng': self.rng,
        }
    
    def get_abstract_state(self) -> dict:
        rng = jax.random.PRNGKey(0)

        variables = self.model.init(rng, jnp.ones(self.input_shape, dtype=jnp.int32))
        params = variables['params']

        return {
            'step': 0,
            'params': params,
            'opt_state': self.state.opt_state,
            'rng': rng,
        }
    
    def get_abstract_metrics(self) -> dict:
        return {
            'train': {'loss': 0.0, 'accuracy': 0.0 },
            'validation': {'loss': 0.0, 'accuracy': 0.0 },
        }
    
    def count_parameters(self) -> int:
        """Count the total number of parameters in the model"""
        return sum(x.size for x in jax.tree_util.tree_leaves(self.state.params))
    