from collections import Counter, defaultdict
import json
import os
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
        self.manifest = {'source': 'HAPI FHIR', 'query': list()}

    def fetch(self, path, filename):
        response = requests.get(f'{FHIR_BASE}/{path}')
        self.manifest['query'].append({
            'request': path,
            'response': filename,
            'status': response.status_code
        })
        try:
            return response.json()
        except:
            return None


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
        'AllergyIntolerance': ('ALLERGY_INTOLERANCE.json', 'patient'),
        'Procedure': ('PROCEDURE.json', 'subject'),
        'Immunization': ('IMMUNIZATION.json', 'patient')
    }

    observation_cats = [
        ('SMOKING_STATUS.json', 'social-history'),
        ('LAB.json', 'laboratory'),
        ('VITAL.json', 'vital-signs')
    ]

    patients = set()

    for entry in get_entries('Patient?_revinclude=*'):
        resource_type = entry['resourceType']
        if resource_type not in links:
            continue

        filename, link_name = links[resource_type]
        ref_value = entry[link_name]['reference']
        if not ref_value.startswith('Patient/'):
            continue
        patients.add(ref_value[8:])

        if len(patients) >= N:
            break

    for patient in patients:
        directory = os.path.join(DATA_DIR, patient, 'SyncForScience', str(uuid4()))
        os.makedirs(directory, exist_ok=True)

        fetcher = FHIRFetcher()
        paths = [('PATIENT_DEMOGRAPHICS.json', f'Patient/{patient}')]
        paths.extend((filename, f'{k}?{link_name}=Patient/{patient}') for k, (filename, link_name) in links.items())
        paths.extend((filename, f'Observation?category={cat_name}&subject=Patient/{patient}') for filename, cat_name in observation_cats)
        for filename, path in paths:
            data = fetcher.fetch(path, filename)
            with open(os.path.join(directory, filename), 'w') as f:
                json.dump(data, f)

        with open(os.path.join(directory, 'log.json'), 'w') as f:
            json.dump(fetcher.manifest, f)
