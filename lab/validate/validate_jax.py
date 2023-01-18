from jax.lib import xla_bridge

def is_jax_gpu_installed():
    try:
        return xla_bridge.get_backend().platform == 'gpu'
    except:
        print("exception")
        return False

if __name__ == '__main__':
    if is_jax_gpu_installed():
        print('JAX: OK')
    else:  
        print('JAX: NOT OK')
        exit(1)
