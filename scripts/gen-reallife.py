import math
from collections import defaultdict
import os

# List files in the directory
fastqc_dir = "/Users/vlad/Seqera/multiqc_heavy_examples/Petropoulus_2016/fastqc"
report_files = os.listdir(fastqc_dir)

# Extract sample names (e.g., ERX1120885)
sample_pattern = r"(ERX\d+)\_fastqc\.zip"
paths = []
for file in report_files:
    paths.append(file)

# Sort samples to ensure consistent grouping
paths.sort()

# Calculate roughly how many samples per group (aiming for 8 groups)
num_groups = 8
samples_per_group = math.ceil(len(paths) / num_groups)

# Create the groups
sample_groups = defaultdict(list)
for i, path in enumerate(paths):
    group_idx = i // samples_per_group
    sample_groups[f"group_{group_idx + 1}"].append(fastqc_dir + "/" + path)

# Generate glob patterns for each group
group_patterns = {}
for i, (group_name, group_samples) in enumerate(sample_groups.items()):
    print(f"multiqc -o tmp/runs/run{i + 1} -f --strict {' '.join(group_samples)}")
