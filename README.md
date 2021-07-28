# 2021-sourmash-greymake2

parallelize containment searches of large sourmash databases using
manifests, picklists, and snakemake.

Briefly, this code -
* spits a database manifest into 25 batches and saves the batches into CSV files
* uses the CSV files as picklists to search the database in parallel with prefetch
* combines the resulting prefetch output into a single picklist and then uses that to search the database again, to generate the final output
