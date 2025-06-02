import csv
import os
import re
from elasticsearch import AsyncElasticsearch, AIOHttpConnection
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
import io
import json
from pydantic import BaseModel
from fastapi.responses import StreamingResponse, JSONResponse
from elasticsearch.exceptions import ConnectionTimeout
from .constants import DATA_PORTAL_AGGREGATIONS, ARTICLES_AGGREGATIONS, PHYLOGENETIC_RANKS


app = FastAPI()

# Create API router to handle /api prefix
api_router = APIRouter(prefix="/api")

origins = [
    "http://localhost:4200",
    "http://127.0.0.1:4200",
    "*"
]

ES_HOST = os.getenv('ES_HOST')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

es = AsyncElasticsearch(
    [ES_HOST],
    timeout=120,
    connection_class=AIOHttpConnection,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    use_ssl=True, verify_certs=True)


@api_router.get("/downloader_utility_data/")
async def downloader_utility_data(taxonomy_filter: str, data_status: str, experiment_type: str, project_name: str):
    body = dict()
    if taxonomy_filter != '':

        if taxonomy_filter:
            body["query"] = {
                "bool": {
                    "filter": list()
                }
            }

            nested_queries = []
            for rank in PHYLOGENETIC_RANKS:
                nested_query = {
                    "nested": {
                        "path": f"taxonomies.{rank}",
                        "query": {
                            "bool": {
                                "filter": [
                                    {
                                        "term": {
                                            f"taxonomies.{rank}.scientificName": taxonomy_filter
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
                nested_queries.append(nested_query)
            final_query = {
                "bool": {
                    "should": nested_queries,
                    "minimum_should_match": 1  # Adjust depending on your logic
                }
            }


            body["query"]["bool"]["filter"].append(final_query)

    if data_status is not None and data_status != '':
        split_array = data_status.split("-")
        if split_array and split_array[0].strip() == 'Biosamples':
            body["query"]["bool"]["filter"].append(
                {"term": {'biosamples': split_array[1].strip()}}
            )
        elif split_array and split_array[0].strip() == 'Raw Data':
            body["query"]["bool"]["filter"].append(
                {"term": {'raw_data': split_array[1].strip()}}
            )
        elif split_array and split_array[0].strip() == 'Mapped Reads':
            body["query"]["bool"]["filter"].append(
                {"term": {'mapped_reads': split_array[1].strip()}})

        elif split_array and split_array[0].strip() == 'Assemblies':
            body["query"]["bool"]["filter"].append(
                {"term": {'assemblies_status': split_array[1].strip()}})
        elif split_array and split_array[0].strip() == 'Annotation Complete':
            body["query"]["bool"]["filter"].append(
                {"term": {'annotation_complete': split_array[1].strip()}})
        elif split_array and split_array[0].strip() == 'Annotation':
            body["query"]["bool"]["filter"].append(
                {"term": {'annotation_status': split_array[1].strip()}})
        elif split_array and split_array[0].strip() == 'Genome Notes':
            nested_dict = {
                "nested": {
                    "path": "genome_notes",
                    "query": {
                        "bool": {
                            "must": [{
                                "exists": {
                                    "field": "genome_notes.url"
                                }
                            }]
                        }
                    }
                }
            }
            body["query"]["bool"]["filter"].append(nested_dict)
    if experiment_type != '' and experiment_type is not None:
        nested_dict = {
            "nested": {
                "path": "experiment",
                "query": {
                    "bool": {
                        "must": [{
                            "term": {
                                "experiment.library_construction_protocol": experiment_type
                            }
                        }]
                    }
                }
            }
        }
        body["query"]["bool"]["filter"].append(nested_dict)
    if project_name is not None and project_name != '':
        body["query"]["bool"]["filter"].append(
            {"term": {'project_name': project_name}})

    response = await es.search(index="data_portal", from_=0, size=10000, body=body)
    total_count = response['hits']['total']['value']
    result = response['hits']['hits']
    results_count = len(response['hits']['hits'])
    while total_count > results_count:
        response1 = await es.search(index="data_portal", from_=results_count, size=10000, body=body)
        result.extend(response1['hits']['hits'])
        results_count += len(response1['hits']['hits'])

    return result


@api_router.get("/downloader_utility_data_with_species/")
async def downloader_utility_data_with_species(species_list: str, project_name: str):
    body = dict()
    result = []
    if species_list != '' and species_list is not None:
        species_list_array = species_list.split(",")
        for organism in species_list_array:
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"project_name": project_name}}
                        ],
                        "should": [
                            {"term": {"_id": organism}},
                            {"term": {"organism": organism}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            }
            response = await es.search(index='data_portal',
                                       body=body)
            result.extend(response['hits']['hits'])

    return result


@api_router.get("/summary")
async def summary():
    response = await es.search(index="summary")
    data = dict()
    data['results'] = response['hits']['hits']
    return data


def convert_to_title_case(input_string):
    # Add a space before each capital letter
    spaced_string = re.sub(r'([A-Z])', r' \1', input_string)

    # Split the string into words, capitalize each word, and join them with a space
    title_cased_string = ' '.join(
        word.capitalize() for word in spaced_string.split())

    return title_cased_string


class QueryParam(BaseModel):
    pageIndex: int
    pageSize: int
    searchValue: str = ''
    sortValue: str
    filterValue: str = ''
    currentClass: str
    phylogeny_filters: str
    index_name: str
    downloadOption: str


@api_router.post("/data-download")
async def get_data_files(item: QueryParam):

    data = await fetch_data_in_batches(item)

    if len(data) > 0 :
        csv_data = create_data_files_csv(data, item.downloadOption,
                                         item.index_name)

        return StreamingResponse(
            csv_data,
            media_type='text/csv',
            headers={"Content-Disposition": "attachment; filename=download.csv"}
        )
    else:
        return JSONResponse(
            status_code=500,
            content={"error": "There was an issue downloading the file"}
        )


def create_data_files_csv(results, download_option, index_name):
    header = []
    if download_option.lower() == "metadata" and index_name in ['data_portal', 'data_portal_test']:
        header = ['Organism', 'Common Name', 'Common Name Source',
                  'Current Status']
    elif download_option.lower() == "metadata" and index_name in ['tracking_status', 'tracking_status_index_test']:
        header = ['Organism', 'Common Name', 'Metadata submitted to BioSamples',
                  'Raw data submitted to ENA',
                  'Mapped reads submitted to ENA',
                  'Assemblies submitted to ENA',
                  'Annotation complete', 'Annotation submitted to ENA']

    output = io.StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(header)
    for entry in results:
        record = entry["_source"]
        if download_option.lower() == "metadata" and index_name in ['data_portal', 'data_portal_test']:
            organism = record.get('organism', '')
            common_name = record.get('commonName', '')
            common_name_source = record.get('commonNameSource', '')
            current_status = record.get('currentStatus', '')
            entry = [organism, common_name, common_name_source, current_status]
            csv_writer.writerow(entry)

        elif download_option.lower() == "metadata" and index_name in ['tracking_status', 'tracking_status_index_test']:
            organism = record.get('organism', '')
            common_name = record.get('commonName', '')
            metadata_biosamples = record.get('biosamples', '')
            raw_data_ena = record.get('raw_data', '')
            mapped_reads_ena = record.get('mapped_reads', '')
            assemblies_ena = record.get('assemblies_status', '')
            annotation_complete = record.get('annotation_complete', '')
            annotation_submitted_ena = record.get('annotation_status', '')
            entry = [organism, common_name, metadata_biosamples, raw_data_ena,
                     mapped_reads_ena, assemblies_ena,
                     annotation_complete, annotation_submitted_ena]
            csv_writer.writerow(entry)

    output.seek(0)
    return io.BytesIO(output.getvalue().encode('utf-8'))


@api_router.get("/{index}")
async def root(index: str, offset: int = 0, limit: int = 15,
               sort: str | None = None, filter: str | None = None,
               search: str | None = None, current_class: str = 'kingdom',
               phylogeny_filters: str | None = None, action: str = None):
    if index == 'favicon.ico':
        return None

    # data structure for ES query
    body = dict()
    # building aggregations for every request
    body["aggs"] = dict()
    if 'articles' in index:
        aggregations_list = ARTICLES_AGGREGATIONS
    else:
        aggregations_list = DATA_PORTAL_AGGREGATIONS

    for aggregation_field in aggregations_list:
        body["aggs"][aggregation_field] = {
            "terms": {"field": aggregation_field, "size": 20}
        }
        if 'data_portal' in index:
            body["aggs"]["experiment"] = {
                "nested": {"path": "experiment"},
                "aggs": {
                    "library_construction_protocol": {
                        "terms": {
                            "field": "experiment.library_construction_protocol",
                            "size": 20
                        },
                        "aggs": {
                            "distinct_docs": {
                                "reverse_nested": {},
                                # get to the parent document level to count number of docs instead of
                                # number of terms
                                "aggs": {
                                    "parent_doc_count": {
                                        "cardinality": {
                                            "field": "tax_id"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

    if 'data_portal' in index or 'tracking_status' in index:
        body["aggs"]["genome_notes"] = {
            "nested": {"path": "genome_notes"},
            "aggs": {
                "genome_count": {
                    "reverse_nested": {},  # get to the parent document level
                    "aggs": {
                        "distinct_docs": {
                            "cardinality": {
                                "field": "id"
                            }
                        }
                    }
                }
            }
        }

        body["aggs"]["taxonomies"] = {
            "nested": {"path": f"taxonomies.{current_class}"},
            "aggs": {current_class: {
                "terms": {
                    "field": f"taxonomies.{current_class}.scientificName"
                }
            }
            }
        }

        if phylogeny_filters:
            body["query"] = {
                "bool": {
                    "filter": list()
                }
            }
            phylogeny_filters = phylogeny_filters.split("-")
            for phylogeny_filter in phylogeny_filters:
                name, value = phylogeny_filter.split(":")
                nested_dict = {
                    "nested": {
                        "path": f"taxonomies.{name}",
                        "query": {
                            "bool": {
                                "filter": list()
                            }
                        }
                    }
                }
                nested_dict["nested"]["query"]["bool"]["filter"].append(
                    {
                        "term": {
                            f"taxonomies.{name}.scientificName": value
                        }
                    }
                )
                body["query"]["bool"]["filter"].append(nested_dict)

    # adding filters, format: filter_name1:filter_value1, etc...
    if filter:
        filters = filter.split(",")
        if 'query' not in body:
            body["query"] = {
                "bool": {
                    "filter": list()
                }
            }
        for filter_item in filters:
            if current_class in filter_item:
                _, value = filter_item.split(":")
                nested_dict = {
                    "nested": {
                        "path": f"taxonomies.{current_class}",
                        "query": {
                            "bool": {
                                "filter": list()
                            }
                        }
                    }
                }
                nested_dict["nested"]["query"]["bool"]["filter"].append(
                    {
                        "term": {
                            f"taxonomies.{current_class}.scientificName": value
                        }
                    }
                )
                body["query"]["bool"]["filter"].append(nested_dict)

            else:
                filter_name, filter_value = filter_item.split(":")

                # Handle both experimentType and experiment.library_construction_protocol formats
                if filter_name == 'experimentType' or filter_name == 'experiment.library_construction_protocol':
                    # If the filter name is experiment.library_construction_protocol, use it directly
                    # Otherwise, construct the nested path for experimentType
                    field_path = "experiment.library_construction_protocol"
                    
                    nested_dict = {
                        "nested": {
                            "path": "experiment",
                            "query": {
                                "bool": {
                                    "filter": {
                                        "term": {
                                            field_path: filter_value
                                        }
                                    }
                                }
                            }
                        }
                    }
                    body["query"]["bool"]["filter"].append(nested_dict)
                elif filter_name == 'genome_notes':
                    nested_dict = {
                        'nested': {'path': 'genome_notes', 'query': {
                            'bool': {
                                'must': [
                                    {'exists': {
                                        'field': 'genome_notes.url'}}]}}}}
                    body["query"]["bool"]["filter"].append(nested_dict)
                # Handle taxonomy filters (kingdom, phylum, class, etc.)
                elif filter_name in ["kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "cohort",
        "forma",
        "infraclass",
        "infraorder",
        "parvorder",
        "section",
        "series",
        "species_group",
        "species_subgroup",
        "subclass",
        "subcohort",
        "subfamily",
        "subgenus",
        "subkingdom",
        "suborder",
        "subphylum",
        "subsection",
        "subspecies",
        "subtribe",
        "superclass",
        "superfamily",
        "superkingdom",
        "superorder",
        "superphylum",
        "tribe",
        "varietas"]:
                    nested_dict = {
                        'nested': {'path': f'taxonomies.{filter_name}', 'query': {
                            'bool': {
                                'must': [
                                    {'term': {
                                        f'taxonomies.{filter_name}.scientificName': filter_value
                                    }}
                                ]
                            }
                        }}
                    }
                    body["query"]["bool"]["filter"].append(nested_dict)
                else:
                    body["query"]["bool"]["filter"].append(
                        {"term": {filter_name: filter_value}})

    # Adding search string
    if search:
        if "query" not in body:
            body["query"] = {"bool": {"must": {"bool": {"should": []}}}}
        else:
            body["query"]["bool"].setdefault("must", {"bool": {"should": []}})

        search_fields = (
            ["title", "journal_name", "study_id", "organism_name"]
            if 'articles' in index
            else ["organism", "commonName", "symbionts_records.organism.text"]
        )

        for field in search_fields:
            body["query"]["bool"]["must"]["bool"]["should"].append({
                "wildcard": {
                    field: {
                        "value": f"*{search}*",
                        "case_insensitive": True
                    }
                }
            })
    
    if action == 'download':
        try:
            response = await es.search(index=index, sort=sort, from_=offset,
                                       body=body, size=limit)
        except ConnectionTimeout:
            return {"error": "Request to Elasticsearch timed out."}
    else:
        response = await es.search(index=index, sort=sort, from_=offset,
                                   size=limit, body=body)

    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    data['aggregations'] = response['aggregations']
    return data


@api_router.get("/{index}/{record_id}")
async def details(index: str, record_id: str):
    body = dict()
    response = await es.search(index=index, q=f"_id:{record_id}")
    if len(response['hits']['hits']) == 0:
        body["query"] = {
            "bool": {"filter": [{'term': {'organism': record_id}}]}}
        response = await es.search(index=index, body=body)
    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    return data



async def fetch_data_in_batches(item: QueryParam):
    offset = 0
    batch_size = 1000
    all_data = []

    while True:

        data = await root(
            item.index_name, offset, batch_size,
            item.sortValue, item.filterValue,
            item.searchValue, item.currentClass,
            item.phylogeny_filters, 'download'
        )


        results = data.get('results', [])
        if not results:
            break

        all_data.extend(results)
        offset += batch_size
        print(f"Fetched {len(results)} results, total: {len(all_data)}")

    return all_data


# Include the API router in the main app
app.include_router(api_router)