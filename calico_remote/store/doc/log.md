# Transaction Log Design

The transaction log for 


## Object Model

- Table - represents a schema projection onto a TableStore that can be used for reads & writes
- TableStore - represents a physical store, consisting of metadata, objects and transaction log

## Design Debates
### How can we distribute transactions horizontally?

One of the design goals for Calico is to support massive horizontal scalability in table design, managed transparently to the reader, and with independence between writers. To achieve this we need to design for write-time isolation between column groups. A single transaction log would break this isolation goal. Options for this include:

1. Single transaction log with mapping from a column group to the ref that tracks it. 
    - Implied mapping through ref naming
        - Pro: simple solution
        - Con: no way to manage cross-column-group transactions, no way to easily manage branching
    - Explicit mapping stored in column-group metadata
        - Pro: supports cross-column-group transactions by mapping multiple column groups to a single log
        - Con: adds complexity
    - Explicitly by having branch heads per column-group
        - Con: no way to manage cross-column-group transactions
        - Pro: simply solution, allows column-group brnaching
        - Could explicitly map a column branch heads to be the same thing

2. Separate transaction logs for each
    - Pro: simple solution, allows column-group brnaching
    - Con: no way to manage cross-column-group transactions

Note that we can design the log append & read code to make switching between these solutions easy. For now, we'll use a single global transaction log but design for the assumption that column-groups will be distributed.

