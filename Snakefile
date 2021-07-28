QUERY='63.fa.sig'
DB='gtdb-rs202.genomic-reps.k31.zip'
NUM_BATCHES=25

rule all:
    input:
        "gather.csv"

rule gather_final:
    input:
        all_prefetch = "out/matches.csv",
        db = DB,
        query = QUERY,
    output:
        "gather.csv"
    shell: """
        sourmash gather {input.query} {input.db} -o {output} \
           --picklist {input.all_prefetch}::prefetch
    """

rule concat_csv:
    input:
        expand("out/{d}.{num}.csv", d=DB, num=range(1, NUM_BATCHES + 1))
    output:
        "out/matches.csv"
    shell: """
        csvtk concat {input} -o {output}
    """


rule split_manifest:
    input:
        DB
    output:
        expand("m/{d}.{batch}.csv", d=DB, batch=range(1, NUM_BATCHES + 1))
    params:
        num_batches=NUM_BATCHES,
    shell: """
        ./split-manifest.py {input} -n {params.num_batches} -o m/{input}
    """


rule search_batch:
    input:
        query=QUERY,
        db=DB,
        chunk_manifest="m/{db}.{num,[0-9]+}.csv",
    output:
        "out/{db}.{num}.csv"
    shell: """
        sourmash prefetch {input.query} {input.db} -o {output} \
            --picklist {input.chunk_manifest}::manifest
    """
