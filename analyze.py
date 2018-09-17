import argparse
from collections import Counter, defaultdict
import glob
import json
import os


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', help='Directory containing subdirectories for each patient')
    parser.add_argument('-b', '--bin-size', help='Histogram bin size', default=5, type=int)

    return parser.parse_args()


def process_directory(directory):
    """Given a `SyncForScience` directory within a patient directory, collects
    total number of resources returned in each searchset by counting unique
    resource IDs for each JSON file present in the directory tree. Returns a
    mapping of resource type to number of returned results.
    """
    uniques = defaultdict(set)
    for subdirectory in os.listdir(directory):
        for data_file in os.listdir(f'{directory}/{subdirectory}'):
            if data_file in ('log.json', 'PATIENT_DEMOGRAPHICS.json'):
                continue  # do nothing with patient demographics for now
            try:
                with open(f'{directory}/{subdirectory}/{data_file}') as f:
                    data = json.load(f)
            except ValueError:
                continue  # any non-JSON files will be ignored

            # update set of unique resource IDs
            # trim `.json` from the filename for the key
            uniques[data_file[:-5]].update(
                entry['resource']['id'] for entry in data['entry']
            )

    return {k: len(v) for k, v in uniques.items()}


def main():
    """Find patient S4S directories and compute statistics on the number of
    resources found for each resource type present. Statistics include mean,
    median, max, min, and histogram data.
    """
    args = parse_arguments()

    # mapping of resource type to list of resource counts for each patient
    total_counts = defaultdict(list)

    for directory in glob.glob(f'{args.path}/*/SyncForScience/'):
        dir_counts = process_directory(directory)
        for type_, count in dir_counts.items():
            total_counts[type_].append(count)

    # TODO: pad each count list with 0s if necessary

    # output data structure
    results = dict()
    for type_, counts in total_counts.items():
        counts.sort()  # for median
        n = len(counts)
        summary = {
            'mean': sum(counts) / n,
            'median': counts[n // 2],  # not a true median, oops
            'min': counts[0],
            'max': counts[-1],
            'histogram': list()
        }

        # generate histogram data
        b = 0
        while True:
            # number of counts in the bin
            in_bin = sum(
                b * args.bin_size <= x < (b + 1) * args.bin_size
                for x in counts
            )
            summary['histogram'].append({
                'bin_start': b * args.bin_size,
                'bin_end': (b + 1) * args.bin_size - 1,  # inclusive
                'count': in_bin
            })
            if (b + 1) * args.bin_size > counts[-1]:
                break
            b += 1
        results[type_] = summary
    return results


if __name__ == '__main__':
    print(json.dumps(main(), indent=2))
