# NOTES TO SELF: important to keep threshold at 50kb, or else there is a
# pair of ambiguous matches (Sphingomonas) where one is above 50kb and the
# other is below, and they can occur in either order.

rule all:
    input:
        "srr.fg.csv",
        "srr.fmg.csv",
        "srr.fmg-rocksdb.csv",
        "SRR606249.x.combined-matches.gather.picklist.csv",

rule fastgather:
    input:
        q="SRR606249.trim.k31.sig.zip",
        db="combined-matches-k31.sig.zip",
    output:
        "srr.fg.csv",
    threads: 8
    shell: """
        sourmash scripts fastgather {input.q} {input.db} -o {output} \
           -c {threads} -t 50000
    """

rule fastmultigather:
    input:
        q="SRR606249.trim.k31.sig.zip",
        db="combined-matches-k31.sig.zip",
    output:
        actual="SRR606249.gather.csv",
        rename="srr.fmg.csv",
    threads: 8
    shell: """
        sourmash scripts fastmultigather {input.q} {input.db} \
           -c {threads} -t 50000
        cp {output.actual} {output.rename}
    """

rule rocksdb_index_db:
    input:
        db="combined-matches-k31.sig.zip",
    output:
        rocksdb=directory("combined-matches-k31.sig.rocksdb"),
        rocksdb_current ="combined-matches-k31.sig.rocksdb/CURRENT",
    threads: 1
    shell: """
        sourmash scripts index {input.db} -m DNA -k 31 \
           -c {threads} -o {output.rocksdb}
    """

rule fastmultigather_rocksdb:
    input:
        q="SRR606249.trim.k31.sig.zip",
        db_current="combined-matches-k31.sig.rocksdb/CURRENT",
    output:
        output="srr.fmg-rocksdb.csv",
    threads: 8
    params:
        db="combined-matches-k31.sig.rocksdb",
    shell: """
        sourmash scripts fastmultigather {input.q} {params.db} \
           -c {threads} -t 50000 -o {output}
    """


rule gather_picklist:
    input:
        q="SRR606249.trim.k31.sig.zip",
        db="combined-matches-k31.sig.zip",
        picklist="srr.fg.csv",
    output:
        csv="SRR606249.x.combined-matches.gather.picklist.csv",
    shell: """
        sourmash gather {input.q} {input.db} \
            --picklist {input.picklist}:match_name:ident \
            -o {output} --threshold-bp 50000
    """
