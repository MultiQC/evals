import math
from collections import defaultdict
import os
from pathlib import Path
import re

# List files in the directory
base_dir = Path("/Users/vlad/Seqera/multiqc_heavy_examples/Petropoulus_2016")
report_files = os.listdir(base_dir / "fastqc")
# Extract sample names (e.g., ERX1120885)
sample_pattern = r"(ERX\d+)\_fastqc\.zip"
paths = []
samples = []
for path in report_files:
    match = re.match(sample_pattern, path)
    if match:
        samples.append(match.group(1))
        paths.append(path)

# Sort samples to ensure consistent grouping
samples.sort()

# Calculate roughly how many samples per group (aiming for 8 groups)
num_groups = 8
samples_per_group = math.ceil(len(samples) / num_groups)

# Create the groups
sample_groups = defaultdict(list)
for i, sample in enumerate(samples):
    group_idx = i // samples_per_group
    sample_groups[f"group_{group_idx + 1}"].append(sample)

# Generate glob patterns for each group
group_patterns = {}
output_dir = "data/reallife"
for i, (group_name, group_samples) in enumerate(sample_groups.items()):
    print(
        f"multiqc {base_dir} -o {output_dir}/run{i + 1} -f --strict {' '.join(f'--only-samples {sample}' for sample in group_samples)}"
    )
