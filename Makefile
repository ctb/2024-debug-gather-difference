.PHONY: all clean cleanall

all:
	snakemake -c 8

clean:
	snakemake -c 8 --delete-all-output

cleanall: clean all
