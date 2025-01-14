# HTAN-FHIR
python scripts to transform HTAN metadata to FHIR

## Fetching Schema

wget https://raw.githubusercontent.com/ncihtan/schematic/main/data/schema_org_schemas/HTAN.jsonld

## Fetching Data
data https://data.humantumoratlas.org/explore?selectedFilters=%5B%7B%22group%22%3A%22AtlasName%22%2C%22value%22%3A%22HTAN+OHSU%22%7D%5D and click Download Metadata

## Transforming Data

```bash
$ python model.py
Assay type Bulk RNA-seq parent not an bts:Assay is bts:DataType
Assay type Bulk DNA not found in the schema
Assay type scATAC-seq parent not an bts:Assay is bts:DataType
Assay type Electron Microscopy parent not an bts:Assay is bts:DataType
Assay type RPPA parent not an bts:Assay is bts:DataType
$ g3t meta validate
{'summary': {'DocumentReference': 94880, 'Specimen': 300, 'ResearchStudy': 1, 'Task': 94880, 'ResearchSubject': 21, 'Patient': 21}}
$ g3t meta graph
```

![image](https://github.com/user-attachments/assets/4caa466b-89d0-47d3-a85c-13fc3be8e3b8)
