import csv
import hashlib
import json
import sys
from collections import defaultdict
from typing import Generator


class HTANSchema:
    """ A class to represent the HTAN schema """
    def __init__(self, schema_file):
        # Load JSON-LD data from a file
        with open(schema_file, "r") as f:
            self.model = json.load(f)
        # add missing nodes
        # thing was missing
        self.model['@graph'].append({
            "@id": "bts:Thing",
            "@type": "rdfs:Class",
            "rdfs:comment": "The most generic type of item.",
            "rdfs:label": "Thing",
            "rdfs:subClassOf": [],
            "schema:isPartOf": {
                "@id": "http://schema.biothings.io"
            },
            "sms:displayName": "Thing",
            "sms:required": "sms:false",
            "sms:validationRules": []
        })
        # make data access a descendant of file
        self.model['@graph'].append({
            "@type": "rdfs:Class",
            "@id": "bts:DataAccess",
            "sms:displayName": "Data Access",
            "rdfs:subClassOf": [{"@id": "bts:File"}],
            "description": "A type of permission which can be granted for accessing a digital document.",
            "sms:required": "sms:false",
            "sms:validationRules": []
        })
        # make synapse id a descendant of file
        self.model['@graph'].append({
            "@type": "rdfs:Class",
            "@id": "bts:SynapseID",
            "sms:displayName": "Synapse Id",
            "rdfs:subClassOf": [{"@id": "bts:File"}],
            "description": "A type of permission which can be granted for accessing a digital document.",
            "sms:required": "sms:false",
            "sms:validationRules": []
        })

        # fix nodes
        for _ in self.model['@graph']:
            if _.get('sms:displayName') == "Imaging Level 3 Image":
                _['sms:displayName'] = "Imaging Level 3"
                _['@id'] = 'bts:ImagingLevel3'
                _['rdfs:label'] = 'ImagingLevel3'

        # Create a dictionary of the model by @id
        self.model_by_id = {_['@id']: _ for _ in self.model['@graph']}
        # Create a dictionary of the model by sms:displayName
        self.model_by_display_name = {_['sms:displayName']: _ for _ in self.model['@graph']}
        # Create a dictionary of the model by rdfs:subClassOf
        self.model_by_sub_class = defaultdict(dict)
        # Create a dictionary of the model by sms:requiresDependency
        self.model_by_dependency_of = defaultdict(dict)
        for _ in self.model['@graph']:
            for sc in _.get('rdfs:subClassOf', []):
                self.model_by_sub_class[sc['@id']][_['@id']] = _
            for rd in _.get('sms:requiresDependency', []):
                self.model_by_dependency_of[rd['@id']][_['@id']] = _

    def get_by_id(self, _id):
        return self.model_by_id.get(_id, None)

    def get_by_display_name(self, display_name):
        if not display_name:
            return None
        node = None
        if not node:
            node = self.model_by_display_name.get(display_name, None)
        if not node:
            node = self.model_by_display_name.get('HTAN ' + display_name, None)
        if not node:
            node = self.model_by_id.get('bts:' + display_name, None)
        if not node:
            node = self.model_by_id.get('bts:' + display_name.replace(' ', ''), None)

        return node

    def get_by_sub_class(self, sub_class) -> dict:
        return self.model_by_sub_class[sub_class]

    def get_by_dependency_of(self, _id) -> dict:
        return self.model_by_dependency_of[_id]

    def get_by_content(self, content) -> dict:
        if not content:
            return None
        node = self.model_by_id.get('bts:' + content.replace(' ', ''), None)
        return node

    def get_column(self, display_name, content) -> dict:
        node = self.get_by_display_name(display_name)
        if not node:
            node = self.get_by_content(content)
        return node


def tree():
    """A recursive defaultdict"""
    return defaultdict(tree)


def normalize(data_path="table_data.tsv", skip_empty=True, sample_assays=False) -> Generator[dict, None, None]:
    """Normalize the data in the table into the BTS schema.
    Returns a generator of the normalized data.  The dict's key is bts_Thing and the value is the normalized data.
    bts_Thing's children are the various bts: classes in the schema.
    """
    # TODO - nothing elegant about this code but it gets the job done
    hs = HTANSchema("HTAN.model.jsonld")
    assay_types_seen_already = set()
    logged_already = set()
    with open(data_path, mode='r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            if sample_assays and row['Assay'] in assay_types_seen_already:
                continue
            if row['HTAN Participant ID'] == '':
                # some rows have two biospecimens
                _biospecimen = row['Biospecimen'].split(',')[0]
                _biospecimen = _biospecimen.replace(" ", "")
                row['HTAN Participant ID'] = '_'.join(_biospecimen.split('_')[:-1])
            # render Assay
            # navigate to from assay type to assay
            assay_types_seen_already.add(row['Assay'])
            assay_dependencies = []
            assay_type = assay_type_parent = assay_type_klass = assay_id = assay_parent_klass = assay_grandparent_klass = None
            assay_type = hs.get_by_content(content=row['Assay'])
            if not assay_type:
                assay_type = hs.get_by_display_name(display_name=row['Assay'])
            if not assay_type:
                msg = f"Assay type {row['Assay']} not found in the schema"
                if msg not in logged_already:
                    print(msg, file=sys.stderr)
                    logged_already.add(msg)
            if assay_type:
                assay_type_parent = assay_type['rdfs:subClassOf'][0]['@id']
                if assay_type_parent == 'bts:DataType':
                    msg = f"Assay type {row['Assay']} parent not an bts:Assay is {assay_type_parent}"
                    if msg not in logged_already:
                        print(msg, file=sys.stderr)
                        logged_already.add(msg)
                    assay_grandparent_klass = "bts:Thing"
                    assay_parent_klass = "bts:Assay"
                    # create an assay type
                    assay_klass = 'ohsu:' + row['Assay'].replace(" ", "")
                    assay_dependencies = ["bts:HTANParticipantID", "bts:HTANBiospecimenID", "bts:HTANParentBiospecimenID", "bts:HTANDataFileID"]
                else:
                    assay_type_klass = assay_type['@id']
                    assay_id = hs.get_by_id(assay_type['rdfs:subClassOf'][0]['@id'])['rdfs:subClassOf'][0]['@id']
                    # assays have levels
                    level = row['Level'].replace(" ", "")
                    assay_id = assay_id + level
                    assay = hs.get_by_id(assay_id)
                    assert assay, f"Assay {assay_type_klass} {assay_type['rdfs:subClassOf'][0]['@id']} {assay_id} not found in the schema"
                    # TODO - get the dependencies of 'sms:requiresComponent'
                    assay_dependencies = [_['@id'] for _ in assay['sms:requiresDependency']]
                    assay_klass = assay['@id']
                    assay_parent_klass = assay['rdfs:subClassOf'][0]['@id']
                    parent = hs.get_by_id(assay_parent_klass)
                    if parent.get('rdfs:subClassOf', []):
                        assay_grandparent_klass = parent['rdfs:subClassOf'][0]['@id']
            else:
                assay_grandparent_klass = "bts:Thing"
                assay_parent_klass = "bts:Assay"
                # create an assay type
                assay_klass = ('ohsu:' + row['Assay'] + row['Level']).replace(" ", "")
                assay_dependencies = ["bts:HTANParticipantID", "bts:HTANBiospecimenID", "bts:HTANParentBiospecimenID", "bts:HTANDataFileID"]

            # render other datatypes
            model = defaultdict(tree)
            for column in row.keys():
                # skip if empty
                if skip_empty and not row[column]:
                    continue
                # get the node of the schema
                if column == 'Biospecimen':
                    n = hs.get_by_id("bts:HTANBiospecimenID")
                elif column == 'Assay':
                    n = hs.get_by_id("bts:AssayType")
                else:
                    n = hs.get_column(display_name=column, content=row[column])
                if not n:
                    model['MISSING_MAPPING'][column] = row[column] if row[column] else None
                    continue
                # fill the assay dependencies
                if n['@id'] in assay_dependencies:
                    # print(assay_grandparent_klass, assay_parent_klass, assay_klass, n['@id'], file=sys.stderr)
                    # print('\t', model[assay_grandparent_klass][assay_parent_klass], file=sys.stderr)
                    model[assay_grandparent_klass][assay_parent_klass][assay_klass][n['@id']] = row[column] if row[column] else None
                    # continue
                # and any other class's dependencies
                path = []
                while n:
                    path.append(n['@id'])
                    if n['rdfs:subClassOf']:
                        n = hs.get_by_id(n['rdfs:subClassOf'][0]['@id'])
                    else:
                        n = None
                path.reverse()

                # print(path, file=sys.stderr)
                if 'bts:Assay' not in path:
                    model_content = model
                    for p in path:
                        if p == path[-1]:
                            model_content[p] = row[column] if row[column] else None
                            break
                        else:
                            if isinstance(model_content[p], str):
                                _ = model_content[p]
                                model_content[p] = {'_': _}
                        model_content = model_content[p]

            # clean up model: move nodes to correct child
            for k in ["bts:Filename", "bts:FileFormat"]:
                thing = model["bts:Thing"]
                if k in thing:
                    thing["bts:InformationContentEntity"]["bts:File"][k] = thing.pop(k)

            if 'bts:HTANParticipantID' not in model['bts:Thing']["bts:IndividualOrganism"]['bts:Patient']:
                model['bts:Thing']["bts:IndividualOrganism"]['bts:Patient']['bts:HTANParticipantID'] = row['HTAN Participant ID']

            if 'bts:HTANParticipantID' not in model['bts:Thing']['bts:Biosample']['bts:Biospecimen']:
                model['bts:Thing']['bts:Biosample']['bts:Biospecimen']['bts:HTANParticipantID'] = row['HTAN Participant ID']

            # move DataType to our constructed assay
            if "bts:DataType" in model['bts:Thing']["bts:Publication"]:
                dt = model['bts:Thing']["bts:Publication"].pop("bts:DataType")
                dt_k = list(dt.keys())[0]
                as_k = list(model['bts:Thing']["bts:Assay"].keys())[0]
                for _k, v in dt[dt_k].items():
                    model['bts:Thing']["bts:Assay"][as_k][_k] = v

            # move Participant, Specimens to our constructed assay
            as_k = list(model['bts:Thing']["bts:Assay"].keys())[0]
            model['bts:Thing']["bts:Assay"][as_k]['bts:HTANParticipantID'] = model['bts:Thing']['bts:Biosample']['bts:Biospecimen']['bts:HTANParticipantID']
            model['bts:Thing']["bts:Assay"][as_k]['bts:HTANBiospecimenID'] = model['bts:Thing']['bts:Biosample']['bts:Biospecimen']['bts:HTANBiospecimenID']
            model['bts:Thing']["bts:Assay"][as_k]['bts:HTANParentBiospecimenID'] = model['bts:Thing']['bts:Biosample']['bts:Biospecimen']['bts:HTANParentBiospecimenID']
            # and file
            model['bts:Thing']["bts:InformationContentEntity"]["bts:File"]['bts:HTANParticipantID'] = model['bts:Thing']['bts:Biosample']['bts:Biospecimen']['bts:HTANParticipantID']

            yield model


def dict_md5(d):
    """Return the MD5 hash of a dictionary."""
    # Convert the dictionary to a JSON string
    dict_str = json.dumps(d, sort_keys=True)
    # Create an MD5 hash object
    hash_obj = hashlib.md5(dict_str.encode())
    # Return the hexadecimal digest of the hash
    return hash_obj.hexdigest()


def _to_id(_id: str):
    """Convert a string to a valid FHIR id."""
    return _id.replace(":", "_").replace(" ", "").replace("_", "-").replace(",", "-")


def fhirized(thing, htan_type) -> list[dict]:
    """Convert a BTS thing to FHIR resources."""
    if htan_type == 'bts_Assay':
        assay_type = next(iter(thing.keys()))
        thing = next(iter(thing.values()))
        specimen_id = thing.get('bts:HTANBiospecimenID', thing.get('bts:HTANParentBiospecimenID'))
        assert specimen_id, (assay_type, thing)
        specimen_ids = specimen_id.split(',')
        inputs = [
                {
                    'type': {
                        'coding': [
                            {
                                'system': "http://hl7.org/fhir/fhir-types",
                                'code': 'Patient',
                                'display': 'Patient'
                            }
                        ]
                    },
                    'valueReference': {
                        'reference': f"Patient/{_to_id(thing['bts:HTANParticipantID'])}"
                    }
                }
            ]
        inputs.extend([
            {
                'type': {
                    'coding': [
                        {
                            'system': "http://schema.biothings.io/",
                            'code': k.replace("bts:", ""),
                            'display': k.replace("bts:", ""),
                        }
                    ],
                },
                "valueString": v
            }
            for k, v in thing.items() if k not in ['bts:HTANParentBiospecimenID', 'bts:HTANParticipantID', 'bts:Filename', 'bts:HTANDataFileID', 'bts:HTANBiospecimenID', '_id', '_type']
        ])
        inputs.extend([
            {
                'type': {
                    'coding': [
                        {
                            'system': "http://hl7.org/fhir/fhir-types",
                            'code': 'Specimen',
                            'display': 'Specimen'
                        }
                    ]
                },
                'valueReference': {
                    'reference': f"Specimen/{_to_id(specimen_id)}"
                }
            }
            for specimen_id in specimen_ids
        ])
        assay = {
            'resourceType': "Task",
             'id': _to_id(thing['bts:HTANDataFileID'] + '-' + specimen_id),
             'identifier': [
                 {
                     "system": "https://htan.org/assay_type",
                     "value": assay_type.replace("bts:", "").replace("ohsu:", "")
                 },
                 {
                     "system": "https://htan.org/HTANDataFileID",
                     "value": thing['bts:HTANDataFileID']
                 }
             ],
            'status': "requested",
            'intent': "order",
            'focus': {
                'reference': f"Specimen/{_to_id(specimen_ids[0])}"
            },
            'for': {
                'reference': f"Patient/{_to_id(thing["bts:HTANParticipantID"])}"
            },
            'code': {
                'coding': [
                    {
                        'system': "https://htan.org",
                        'code': htan_type,
                        'display': htan_type
                    }
                ]
            },
            'description': f"Assay that created {thing['bts:Filename']} file for {thing['bts:HTANParentBiospecimenID']}",
            'input': inputs,
            'output': [
                {
                    'type': {
                        'coding': [
                            {
                                'system': "http://hl7.org/fhir/fhir-types",
                                'code': 'DocumentReference',
                                'display': 'DocumentReference'
                            }
                        ]
                    },
                    'valueReference': {
                        'reference': f"DocumentReference/{_to_id(thing['bts:HTANDataFileID'])}"
                    }
                }
            ]
         }

        return [assay]

    if htan_type == 'bts_IndividualOrganism':
        thing = thing['bts:Patient']
        assert 'bts:HTANParticipantID' in thing, thing
        patient = {'resourceType': "Patient",
                   'id': _to_id(thing['bts:HTANParticipantID']),
                   'identifier': [
                       {
                           "system": "https://htan.org",
                           "value": thing['bts:HTANParticipantID']
                       }
                   ]}
        research_subject = {
            'resourceType': "ResearchSubject",
            'id': _to_id(thing['bts:HTANParticipantID'] + '-HTA9'),
            'subject': {
                'reference': f"Patient/{_to_id(thing['bts:HTANParticipantID'])}"
            },
            'status': "candidate",
            'study': {
                'reference': f"ResearchStudy/HTA9" #  TODO {_to_id(thing['bts:HTANCenterID'])}
            }
        }
        return [patient, research_subject]

    if htan_type == 'bts_Biosample':
        biospecimen_type = next(iter(thing.keys()))
        thing = next(iter(thing.values()))
        specimen_ids = thing['bts:HTANBiospecimenID'].split(',')
        specimens = []
        for specimen_id in specimen_ids:
            specimen = {'resourceType': "Specimen",
                         'id': _to_id(specimen_id),
                         'identifier': [
                             {
                                 "system": "https://htan.org",
                                 "value": specimen_id,
                             }
                         ],
                         'subject': {
                                'reference': f"Patient/{_to_id(thing['bts:HTANParticipantID'])}"
                         }
                         }
            specimens.append(specimen)
        return specimens

    if htan_type == 'bts_InformationContentEntity':
        thing = thing['bts:File']
        document_reference = {'resourceType': "DocumentReference",
            'id': _to_id(thing['bts:HTANDataFileID']),
            'status': "current",
            'identifier': [
                {
                  "system": "https://htan.org",
                  "value": thing['bts:HTANDataFileID'],
                },
                {
                  "system": "https://synapse.org",
                  "value": thing['bts:SynapseID'],
                },
                {
                    "system": "https://biothings.io/DataAccess",
                    "value": thing['bts:DataAccess'],
                },
            ],
            'subject': {
                'reference': f"Patient/{_to_id(thing['bts:HTANParticipantID'])}"
            },
            'content': [
                {
                    'attachment': {
                        'url': thing['bts:Filename'],
                        'contentType': thing['bts:FileFormat']
                    }
                }
            ]
        }
        return [document_reference]

    if htan_type == 'bts_Publication':
        center_id = next(iter(thing['bts:HTANCenterID'].values()))
        return [{
            'resourceType': "ResearchStudy",
            'id': _to_id(center_id),
            'status': "completed",
        }]

    assert False, f"Unknown HTAN type {htan_type}"
    return {'resourceType': "TODO"}


def main():
    """Main function, reads HTAN schema, table_data and outputs FHIR."""
    emitters = {}
    emitted_already = set()

    for normalized in normalize():
        #
        for k, thing in normalized['bts:Thing'].items():
            k = k.replace(":", "_")
            for resource in fhirized(thing, k):
                if not resource:
                    continue

                if resource['id'] in emitted_already:
                    continue
                emitted_already.add(resource['id'])

                k = resource['resourceType']
                if k not in emitters:
                    f = open(f"META/{k}.ndjson", "w")
                    emitters[k] = f
                f = emitters.get(k)
                f.write(json.dumps(resource))
                f.write('\n')

    for _ in emitters.values():
        _.close()


main()
