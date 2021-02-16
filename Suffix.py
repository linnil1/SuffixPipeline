import re
import os
import glob
from concurrent.futures import ThreadPoolExecutor


class Suffix:
    """
    Every module should inherited Suffix

    Attrs:
      suffix_add(str): The suffix the module will add
      cannot_skip(bool): Cannot skip the module when True
      force(bool): Force to run the module and the following
      parallel(bool): Run module in parallel
          * True  for calling runSample() for each sample in parallel
          * False for calling runSample() for each sample
          * None will call run() once

      thread(int): The available threads of the PC
      samples(list of str): The samples for iterations
      base_folder(str): The path to folder
      stage(str): The stage of overall module
      suffix(str): The suffix string when the module running
            The above four elements will be propogated when running module.

      stop(bool): Stop the modules after it
    """
    suffix_add = ""
    cannot_skip = False
    parallel = False

    def __init__(self, base="data", stage="tmp", suffix="", 
                 force=False, stop=False, thread=30):
        self.base_folder = base
        self.stage       = stage
        self.suffix      = suffix
        self.samples     = []
        self.force       = force
        self.thread      = thread
        self.stop        = stop

    def log(self, s):
        print(f"[{type(self).__name__} {self.getFullPathOut()}] {s}")

    def copyFrom(self, a):
        """ Copy from another Suffix object """
        self.base_folder = a.base_folder
        self.stage       = a.stage
        self.suffix      = a.suffix
        self.samples     = a.samples
        self.force       = self.force or a.force
        self.thread      = a.thread

    def getSample(self):
        return self.samples

    def getFullPathIn(self, name="") -> str:
        """ Get input file path by Suffix rule """
        dot = "." if name and self.stage else ""
        return f"{self.base_folder}/{self.stage}{dot}{name}{self.suffix}"
    
    def getFullPathOut(self, name="") -> str:
        """ Get output file path by Suffix rule """
        return self.getFullPathIn(name) + self.suffix_add

    def require(self, name=""):
        """ Custom it. Return the files needed before module running """
        return []

    def checkRequire(self, name="") -> bool:
        """ Check if files in require() is exist """
        for path in self.require(name):
            if not os.path.exists(path):
                self.log(f"Miss {path}")
                return False
        return True

    def checkSkip(self, name=""):
        """ Skip Rule. Using output_suffix to check if the files are existed """
        if self.cannot_skip:
            return False
        if self.force:
            return False
        if glob.glob(self.getFullPathOut(name) + ".*"):
            return True
        return False

    def checkRun(self):
        """ A wrapper the check the require() before run """
        # run non-sample-wise
        if self.parallel is None:
            if self.checkRequire():
                if self.checkSkip():
                    self.log(f"SKIP")
                else:
                    self.log(f"RUN")
                    self.run(self.thread)
            else:
                self.log(f"Miss some files")
                self.stop = True
            return

        # run sample-wise
        # check requirments
        names = []
        for name in self.getSample():
            if self.checkRequire(name):
                if self.checkSkip(name):
                    self.log(f"SKIP {name}")
                else:
                    names.append(name)
            else:
                self.log(f"Miss some files")
                self.stop = True

        # parallel mode
        if self.parallel:
            thr = self.thread // max(len(names), 1) + 1
            with ThreadPoolExecutor(max_workers=self.thread) as executor:
                for name in names:
                    executor.submit(self.runSample, name, thr=thr)
        else:
            for name in names:
                self.log(f"RUN {name}")
                self.runSample(name, thr=self.thread)

    def run(self, thr=0):
        """ Custom this function when parallel is None """
        return

    def runSample(self, name="", thr=0):
        """ Custom this function when parallel is not None """
        return

    def runAfter(self):
        """ (Optional) Run this before  """
        pass

    def runBefore(self):
        """ (Optional) Run this after """
        pass

    def runPipeline(self, modules=[]):
        """ Run module in modules in order """
        for m in modules:
            m.copyFrom(self)  # copy suffix into module
            self.runBefore()
            m.checkRun()      # MAIN
            self.runAfter()
            if m.stop:        # stop
                self.log("STOP")
                break
            self.copyFrom(m)  # copy syffix out from module
            self.suffix += m.suffix_add  # add the suffix
