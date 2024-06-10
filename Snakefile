rule all:
    input:
        "srr.fg.csv",
        "srr.fmg.csv",
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
           -t {threads}
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
           -t {threads}
        cp {output.actual} {output.rename}
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
            -o {output}
    """
