# SuffixPipeline

## Feature
I want to bulid a **self-described** Pipeline system.

Thus, maintaining suffix in filename is the way to show the procedure the sample applied.
e.g. `NA12878.trim.bwa.bam` indicate the NA12878's data is trimmed and then mapped by bwa

In bioinformatic, most of tools share same standard format. e.g. bam, fastq, vcf.
The advantage is that we can run the tools without any modification in file. However,
I often mess up the filename because the name will collided when the same sample applied in different procedure

With Suffix, we can separate the files by filename when 
* Using different tools, `xx.bwa.bam` `xx.bowtie.bam`
* Using different parameters , `xx.bwa.bam` `xx.bwa_L1000.bam` `xx.bwa_hg38.bam`
* Using different procedure, `xx.bwa.bam` `xx.trim.bwa.bam`

In this system, 
I don't need to comment out anything (should not), 
because each module indicate the suffix

``` Python
Suffix(stage="tmp", suffix="").runPipeline([
    SampleCSV(),  # read samples name  => Input data/tmp.samples.csv
    Bwa(),        # run BWA            => Output data/tmp.xx.bwa.bam
    SortBam(),    # sort bam file      => Output data/tmp.xx.bwa.sort.bam
    ExtractChr6(),#                    => Output data/tmp.xx.bwa.sort.chr6.bam
])

Suffix(stage="tmp", suffix="").runPipeline([
    SampleCSV(),  # read samples name  => Input data/tmp.samples.csv
    Bwa(),        # run BWA            => Output data/tmp.xx.bwa.bam
    # SortBam(),  # 
    ExtractChr6(),#                    => Output data/tmp.xx.bwa.chr6.bam
                  # But will fail because the bam not sorted
])
```

After a proper Suffix Pipeline system,
we can more **focus on pipeline building and parameter tunning**.

Each module is easily defined, e.g. BWA
``` python
class Bwa(Suffix):
    suffix_add = ".bwa"  # Add .bwa in the suffix
    parallel = True      # Run all the samples parallelly

    def require(self, name):
        # The list of files the module required
        file_in = self.getFullPathIn(name)
        return [file_in + ".R1.fq.gz", 
                file_in + ".R2.fq.gz"]

    def runSample(self, name, thr):
        # define runSample() when parallel is not None
        file_r1, file_r2 = self.require(name)
        file_out = self.getFullPathOut(name)
        os.system(f"bwa -t {thr} bwa/{stage}.hg38.fa {file_r1} {file_r2} -o {file_out}")
```

## TODO
Connect the suffix system to snakemake or ruffus API

## Example
https://github.com/linnil1/SuffixPipeline/blob/main/example.py

`python3 example.py`
