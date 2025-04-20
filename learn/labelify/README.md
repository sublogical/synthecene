# TODO List


* providers
  * anthropic
    * add batching support
    * add support for text classification tasks
* pipelining
  * implement beam job runner
  * read from storage (cassandra)
  * write to storage (cassandra)

# HOWTO

## Python Environment

```
conda activate labelify-3.12
```


# What it does

autolabel will:
* read inputs from an example attribute {input}
* it will use a model configuration {model} and prompt configuration {prompt}
* which produce a set of additional attributes



