import os, subprocess

def test_Placeholder():
    cmd = "python3 phylogeny_tools/time_to_coalescence.py --help"
    subprocess.run(cmd, shell=True)