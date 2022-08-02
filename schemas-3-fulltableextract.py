import csv
import fhirpathpy
import json
import os
import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import sys  # DEMO
import random
import yaml

from functools import cache
from pathlib import Path
from phdi.azure import AzureFhirServerCredentialManager
from phdi.fhir import fhir_server_get
from typing import Literal, List, Union, Callable

from memory_profiler import profile


def load_schema(path: str) -> dict:
    """
    Given the path to local YAML files containing a user-defined schema read the file
    and return the schema as a dictionary.

    :param path: Path specifying the location of a YAML file containing a schema.
    :return schema: A user-defined schema
    """
    try:
        with open(path, "r") as file:
            schema = yaml.safe_load(file)
        return schema
    except FileNotFoundError:
        return {}


def apply_selection_criteria(
    value: list,
    selection_criteria: Literal["first", "last", "random", "all"],
) -> Union[str, List[str]]:
    """
    Given a list of values parsed from a FHIR resource, return value(s) according to the
    selection criteria. In general a single value is returned, but when
    selection_criteria is set to "all" a list containing all of the parsed values is
    returned.

    :param value: A list containing the values parsed from a FHIR resource.
    :param selection_criteria: A string indicating which element(s) of a list to select.
    """

    if selection_criteria == "first":
        value = value[0]
    elif selection_criteria == "last":
        value = value[-1]
    elif selection_criteria == "random":
        value = random.choice(value)

    # Temporary hack to ensure no structured data is written using pyarrow.
    # Currently Pyarrow does not support mixing non-structured and structured data.
    # https://github.com/awslabs/aws-data-wrangler/issues/463
    # Will need to consider other methods of writing to parquet if this is an essential
    # feature.
    if type(value) == dict:
        value = json.dumps(value)
    elif type(value) == list:
        value = ",".join(value)
    return value


@cache
def __get_fhirpathpy_parser(fhirpath_expression: str) -> Callable:
    """
    Return a fhirpathpy parser for a specific FHIRPath.  This cached function minimizes
    calls to the relatively expensive :func:`fhirpathpy.compile` function for any given
    `fhirpath_expression`

    :param fhirpath_expression: The FHIRPath expression to evaluate
    """
    return fhirpathpy.compile(fhirpath_expression)


def apply_schema_to_resource(resource: dict, schema: dict) -> dict:
    """
    Given a resource and a schema, return a dictionary with values of the data
    specified by the schema and associated keys defined by the variable name provided
    by the schema.

    :param resource: A FHIR resource on which to apply a schema.
    :param schema: A schema specifying the desired values by FHIR resource type.
    """

    data = {}
    resource_schema = schema.get(resource.get("resourceType", ""))
    if resource_schema is None:
        return data
    for field in resource_schema.keys():
        path = resource_schema[field]["fhir_path"]

        parse_function = __get_fhirpathpy_parser(path)
        value = parse_function(resource)

        if len(value) == 0:
            data[resource_schema[field]["new_name"]] = ""
        else:
            selection_criteria = resource_schema[field]["selection_criteria"]
            value = apply_selection_criteria(value, selection_criteria)
            data[resource_schema[field]["new_name"]] = str(value)

    return data


def extract_data_from_fhir_server(fhir_url, table_schema, cred_manager):
    all_data = {}
    for resource_type in table_schema:
        url = f"{fhir_url}/{resource_type}?_count=1000"
        data = []

        demo_counter = 0  # DEMO
        while url is not None and demo_counter < 5:  # DEMO
            demo_counter += 1  # DEMO
            response = fhir_server_get(url, cred_manager)
            if response.status_code != 200:
                break

            query_result = response.json()
            resources = [elem.get("resource") for elem in query_result["entry"]]
            resources = list(filter(None, resources))
            data.extend(resources)

            for link in query_result.get("link"):
                if link.get("relation") == "next":
                    url = link.get("url", None)
                    break
                else:
                    url = None

        all_data[resource_type] = data

    return all_data


def make_table(data, table_schema):
    output = []
    for resource_type in data.keys():
        for resource in data[resource_type]:
            values = extract_and_filter_resource_values(
                resource, table_schema[resource_type].values()
            )
            if values:
                output.append(values)

    return pa.Table.from_pylist(output)


def extract_and_filter_resource_values(resource, field_parameters):
    values = {}
    for parameters in field_parameters:
        parser = __get_fhirpathpy_parser(parameters["fhir_path"])
        value = parser(resource)

        if len(value) == 0:
            values[parameters["new_name"]] = None
        elif parameters["selection_criteria"] == "first":
            values[parameters["new_name"]] = value[0]
        elif parameters["selection_criteria"] == "last":
            values[parameters["new_name"]] = value[-1]

        if isinstance(value, dict):
            values[parameters["new_name"]] = json.dumps(value)
        elif isinstance(value, list):
            values[parameters["new_name"]] = ",".join(map(str, value))
        else:
            values[parameters["new_name"]] = value

    return values


def write_table(data, output_file_name, file_format):
    writer = pq.ParquetWriter(output_file_name, data.schema)
    writer.write_table(table=data)
    writer.close()


def print_schema_summary(
    schema_directory: pathlib.Path,
    display_head: bool = False,
):
    """
    Given a directory containing tables of the specified file format, print a summary of
    each table.

    :param schema_directory: Path specifying location of schema tables.
    :param display_head: Print the head of each table when true. Note depending on the
    file format this may require reading large amounts of data into memory.
    """
    for (directory_path, _, file_names) in os.walk(schema_directory):
        for file_name in file_names:
            if file_name.endswith("parquet"):
                # Read metadata from parquet file without loading the actual data.
                parquet_file = pq.ParquetFile(Path(directory_path) / file_name)
                print(parquet_file.metadata)

                # Read data from parquet and convert to pandas data frame.
                if display_head is True:
                    parquet_table = pq.read_table(Path(directory_path) / file_name)
                    df = parquet_table.to_pandas()
                    print(df.head())
                    print(df.info())
            if file_name.endswith("csv"):
                with open(file_name, "r") as csv_file:
                    reader = csv.reader(csv_file, dialect="excel")
                    print(next(reader))
                    return "hi"


@profile
def demo_run(
    schema_path: str, base_output_path: Path, output_format: str, fhir_url: str
):
    cred_manager = AzureFhirServerCredentialManager(fhir_url)

    schema = load_schema(schema_path)
    for table_name in schema.keys():
        output_path = base_output_path / table_name
        output_path.mkdir(parents=True, exist_ok=True)
        output_file_name = output_path / f"{table_name}.{output_format}"

        data = extract_data_from_fhir_server(fhir_url, schema[table_name], cred_manager)

        table = make_table(data, schema[table_name])

        write_table(table, output_file_name, output_format)


# demo
if __name__ == "__main__":
    schema_path = sys.argv[1]
    output_path = Path(sys.argv[2])
    output_format = sys.argv[3]
    fhir_url = sys.argv[4]

    demo_run(schema_path, output_path, output_format, fhir_url)
