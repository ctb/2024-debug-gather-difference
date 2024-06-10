# commands run for each variant:

#sourmash gather SRR606249.trim.k31.sig.zip combined-matches-k31.sig.zip -o SRR606249.x.combined-matches.gather.csv

#sourmash scripts index -k 31 -m DNA -o combined-matches-k31.rocksdb combined-matches-k31.sig.zip

#sourmash scripts fastmultigather SRR606249.trim.k31.sig.zip combined-matches-k31.rocksdb -o srr.fmg-rdb.csv 2> srr.fmg-rdb.out

sourmash scripts fastgather SRR606249.trim.k31.sig.zip combined-matches-k31.sig.zip -o srr.fg.csv

sourmash scripts fastmultigather SRR606249.trim.k31.sig.zip combined-matches-k31.sig.zip 2> srr.fmg.out # -o srr.fmg.csv
mv SRR606249.gather.csv srr.fmg.csv
