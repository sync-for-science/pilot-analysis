from collections import Counter, defaultdict
import json
import os
import time
from uuid import uuid4

import requests


"""Retrieves some sample data from the public HAPI FHIR server at
http://hapi.fhir.org. Finds 100 patients with linked resources of specified
types. (Quick and dirty.)
"""


FHIR_BASE = 'http://hapi.fhir.org/baseDstu2/'
DATA_DIR = 'data'
N = 100


def get_entries(path):
    uri = f'{FHIR_BASE}/{path}'
    while uri:
        data = requests.get(uri).json()
        uri = next((l['url'] for l in data['link'] if l['relation'] == 'next'), None)
        yield from (e['resource'] for e in data['entry'])


def find_reference_properties(obj, resource_type):
    if not isinstance(obj, dict):
        return
    if obj.get('reference', '').startswith(f'{resource_type}/'):
        yield 'reference'
    for k, v in obj.items():
        if not (isinstance(v, dict) or isinstance(v, list)):
            continue
        if isinstance(v, dict):
            v = [v]
        for subentry in v:
            yield from (f'{k}.{path}' for path in find_reference_properties(subentry, resource_type))


class FHIRFetcher:
    def __init__(self):
        self.manifest = {'timestamp': int(time.time()), 'fetches': list()}

    def fetch(self, path, filename):
        data = requests.get(f'{FHIR_BASE}/{path}').json()
        self.manifest['fetches'].append({
            'api_call': path,
            'filename': filename
        })
        return data


if __name__ == '__main__':
    # results = defaultdict(Counter)
    # for entry in get_entries():
    #     resource_type = entry['resourceType']
    #     if resource_type == 'Patient':
    #         continue
    #     results[resource_type].update(find_reference_properties(entry, 'Patient'))

    # import pprint
    # pprint.pprint(results)

    links = {
        'Condition': 'patient',
        'DiagnosticReport': 'subject',
        'Encounter': 'patient',
        'Immunization': 'patient',
        'MedicationStatement': 'patient',
        'Observation': 'subject',
        'Procedure': 'subject'
    }

    observation_cats = ['social-history', 'vital-signs', 'imaging', 'laboratory', 'procedure', 'survey', 'exam', 'therapy']

    patients = set()

    for entry in get_entries('Patient?_revinclude=*'):
        resource_type = entry['resourceType']
        if resource_type not in links:
            continue

        link_name = links[resource_type]
        ref_value = entry[link_name]['reference']
        if not ref_value.startswith('Patient/'):
            continue
        patients.add(ref_value[8:])

        if len(patients) >= N:
            break

    for patient in patients:
        directory = os.path.join(DATA_DIR, patient, 'S4S', str(uuid4()))
        os.makedirs(directory, exist_ok=True)

        fetcher = FHIRFetcher()
        paths = [('Patient.json', f'Patient/{patient}')]
        paths.extend((f'{k}.json', f'{k}?{v}=Patient/{patient}') for k, v in links.items())
        paths.extend((f'Observation-{cat}.json', f'Observation?category={cat}&subject=Patient/{patient}') for cat in observation_cats)
        for filename, path in paths:
            data = fetcher.fetch(path, filename)
            with open(os.path.join(directory, filename), 'w') as f:
                json.dump(data, f)

        with open(os.path.join(directory, 'manifest.json'), 'w') as f:
            json.dump(fetcher.manifest, f)
