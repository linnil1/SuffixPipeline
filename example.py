from Suffix import Suffix, Rename, SetSample
import os


class SampleCSV(Suffix):
    suffix_add = ""     # This module has no output
    cannot_skip = True  # Setup samples name (Cannot skip)
    parallel = None     # non-sample-wise

    def require(self, name=""):
        # The files the module required
        file_in = self.getFullPathIn()
        return [file_in + ".samples.csv"]

    def run(self, thr):
        # define run() when parallel = None
        file_csv, = self.require()
        self.samples = list(map(lambda a: a.strip(), open(file_csv)))


class Bwa(Suffix):
    suffix_add = ".bwa"  # add .bwa in the suffix
    parallel = True

    def require(self, name):
        # The list of files the module required
        file_in = self.getFullPathIn(name)
        return [file_in + ".R1.fq.gz", 
                file_in + ".R2.fq.gz"]

    def runSample(self, name, thr):
        # define runSample() when parallel is not None
        file_r1, file_r2 = self.require(name)
        file_out = self.getFullPathOut(name)
        os.system(f"echo bwa > {file_out}.bam")
        os.system(f"cat {file_r1} >> {file_out}.bam")
        os.system(f"cat {file_r2} >> {file_out}.bam")


class SortBam(Suffix):
    suffix_add = ".sort"
    parallel = True

    def require(self, name):
        file_in = self.getFullPathIn(name)
        return [file_in + ".bam"]

    def runSample(self, name, thr):
        file_unsort_bam, = self.require(name)
        file_out = self.getFullPathOut(name)
        os.system(f"echo sort > {file_out}.bam")
        os.system(f"cat {file_unsort_bam} >> {file_out}.bam")


class ExtractChr6(Suffix):
    suffix_add = ".chr6"

    def require(self, name):
        file_in = self.getFullPathIn(name)
        return [file_in + ".bam"]

    def runSample(self, name, thr):
        file_bam, = self.require(name)
        file_out = self.getFullPathOut(name)
        os.system(f"echo extract6 > {file_out}.bam")
        os.system(f"cat {file_bam} >> {file_out}.bam")


class StatBam(Suffix):
    suffix_add = ".stat"

    def require(self, name):
        file_in = self.getFullPathIn(name)
        return [file_in + ".bam"]

    def runSample(self, name, thr):
        file_bam, = self.require(name)
        file_out = self.getFullPathOut(name)
        os.system(f"echo stat > {file_out}.txt")
        os.system(f"cat {file_bam} >> {file_out}.txt")


class MergeStat(Suffix):
    suffix_add = ""  # data/stage.name.suffix -> data/stage.suffix
    parallel = None  # non-sample-wise

    def require(self, name=""):
        return [self.getFullPathIn(name) + ".txt" for name in self.getSample()]

    def run(self, thr):
        files_txt = self.require()
        fout = open(self.getFullPathOut() + ".txt", "w")
        for f in files_txt:
            fout.write(f + "\n")
            fout.writelines(open(f))
        fout.close()


class Bam2Fastq(Suffix):
    suffix_add = ".bam2fq"

    def require(self, name):
        file_in = self.getFullPathIn(name)
        return [file_in + ".bam"]

    def runSample(self, name, thr):
        file_bam, = self.require(name)
        file_out = self.getFullPathOut(name)
        os.system(f"echo bam2fq     > {file_out}.R1.fq.gz")
        os.system(f"cat {file_bam} >> {file_out}.R1.fq.gz")
        os.system(f"echo bam2fq     > {file_out}.R2.fq.gz")
        os.system(f"cat {file_bam} >> {file_out}.R2.fq.gz")


def setupTest():
    os.system("mkdir -p data/")
    os.system("echo s1 >> data/tmp.samples.csv")
    os.system("echo s2 >> data/tmp.samples.csv")
    os.system("echo s3 >> data/tmp.samples.csv")
    os.system("echo 11 > data/tmp.s1.R1.fq.gz")
    os.system("echo 12 > data/tmp.s1.R2.fq.gz")
    os.system("echo 21 > data/tmp.s2.R1.fq.gz")
    os.system("echo 22 > data/tmp.s2.R2.fq.gz")
    os.system("echo 31 > data/tmp.s3.R1.fq.gz")
    os.system("echo 32 > data/tmp.s3.R2.fq.gz")


setupTest()

Suffix(stage="tmp", suffix="").runPipeline([
    SampleCSV(),  # get samples' name
    Bwa(),        # run bwa for each sample => data/tmp.xx.R1.fq.gz -> data/tmp.xx.bwa.bam
    SortBam(),    # sort bam file           => data/tmp.xx.bwa.bam  -> data/tmp.xx.bwa.sort.bam
    ExtractChr6(),# Extract chr6 region     => data/tmp.xx.bwa.sort.bam -> data/tmp.xx.bwa.sort.chr6.bam
    StatBam(),    # View stat of bam        => data/tmp.xx.bwa.sort.chr6.bam -> data/tmp.xx.bwa.sort.chr6.stat.txt
    MergeStat(),  # Merge the stat of bam   => data/tmp.xx*.bwa.sort.chr6.stat.txt -> data/tmp.bwa.sort.chr6.stat.txt
    Rename(parallel=None, new_suffix=".stat"),  # Want shorter name => data/tmp.bwa.sort.chr6.stat.xx -> data/tmp.stat.xx
])

Suffix(stage="tmp", suffix="").runPipeline([
    SampleCSV(),
    Bwa(),         # The advantage is to reuse the module without any chaning
                   # You can use stage name as index folder -> e.g. bwa.tmp/hg38.fa
    SortBam(),
    ExtractChr6(), # Same as above, it'll skip it, don't need to comment out
    Bam2Fastq(),
    Rename(new_suffix="", new_stage="tmp2", parallel=False),  # Want shorter name => data/tmp.bwa.sort.chr6.bam -> data/tmp.stat.xx
])

# When changing to different stage, it indicates different samples may used
# In this case I just use same dataset
os.system("ln -s ../data/tmp.samples.csv data/tmp2.samples.csv")

Suffix(stage="tmp2", suffix="").runPipeline([
    SampleCSV(),
    Bwa(),        # BWA for new dataset     => data/tmp2.xx.fq.gz -> data/tmp2.xx.bwa.bam
    StatBam(),    # View stat of bam        => data/tmp2.xx.bwa.bam -> data/tmp2.xx.bwa.stat.txt
    MergeStat(),  # Merge the stat of bam   => data/tmp2.xx*.bwa.stat.txt -> data/tmp2.bwa.stat.txt
    Rename(new_suffix=".stat", parallel=None),  # Want shorter name => data/tmp2.bwa.stat.txt -> data/tmp2.stat.xx
])

# output
# tmp.stat.txt
# tmp2.stat.txt
