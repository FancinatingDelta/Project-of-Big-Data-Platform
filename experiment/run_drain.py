import sys
import os
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

CUSTOM_INPUT_FILE = "./experiment/data/test.csv"
CUSTOM_OUTPUT_FILE = "./experiment/result/custom/result.txt"

LOGPARSER_INPUT_DIR = "./experiment/data/"
LOGPARSER_OUTPUT_DIR = "./experiment/result/logparser/"
LOGPARSER_LOG_FILE = "test.csv"

os.makedirs(os.path.dirname(CUSTOM_OUTPUT_FILE), exist_ok=True)
os.makedirs(LOGPARSER_OUTPUT_DIR, exist_ok=True)

IP_REX = r"\b\d{1,3}(\.\d{1,3}){3}\b"
NUM_REX = r"\d+"

MAX_DEPTH = 5
MAX_CHILD = 5
ST = 0.4


def run_custom_drain():
    from drain import Drain

    drain = Drain(
        max_depth=MAX_DEPTH,
        max_children=MAX_CHILD,
        st=ST,
        rules=[
            {"pattern": IP_REX, "replacement": "<IP>"},
            {"pattern": NUM_REX, "replacement": "<NUM>"},
        ]
    )

    with open(CUSTOM_INPUT_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                drain.parse(line)
    
    with open(CUSTOM_OUTPUT_FILE, "w") as f:
        for tpl in drain.export_templates():
            f.write(str(tpl) + "\n")


def run_logparser_drain():
    from logparser import Drain

    drain = Drain.LogParser(
        log_format="<Content>",
        depth=MAX_DEPTH,
        maxChild=MAX_CHILD,
        indir=LOGPARSER_INPUT_DIR,
        outdir=LOGPARSER_OUTPUT_DIR,
        st=ST,
        rex=[IP_REX, NUM_REX],
    )

    drain.parse(LOGPARSER_LOG_FILE)

if __name__ == "__main__":
    run_custom_drain()
    run_logparser_drain()