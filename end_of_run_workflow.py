from prefect import task, flow, get_run_logger
from data_validation import data_validation, get_run, get_run_processed

# QAS Application Specific
from xas.process import process_interpolate_bin_with_tiled

# PizzaBox Adapter-specific
from tiled.mimetypes import DEFAULT_ADAPTERS_BY_MIMETYPE
from tiled.structures.array import ArrayStructure, StructDtype
from tiled.structures.data_source import Asset, DataSource
from tiled.catalog.orm import Node
from tiled.adapters.array import ArrayAdapter
from tiled.utils import path_from_uri, ensure_uri
import os
from pathlib import Path
import dask.dataframe
import numpy as np
import dask.array as da


tiled_inst = "https://tiled.nsls2.bnl.gov"

# Define custom adapters for Pizzabox data
class PizzaBoxAdapter(ArrayAdapter):
    """Adapter for electrometer and trigger binary files from the PizzaBox system."""

    @staticmethod
    def locate_files(*uris) -> tuple[str, str]:
        if len(uris) > 2:
            raise ValueError("Expected 1 or 2 URIs: one for the .bin file (required) and one for the .txt file (optional)")

        elif len(uris) == 2:
            uri_bin, uri_txt = uris
            ext_bin, ext_txt = os.path.splitext(uri_bin)[1], os.path.splitext(uri_txt)[1]
            if {ext_bin, ext_txt} != {".txt", ".bin"}:
                raise ValueError(f"Expected one .txt and one .bin file, got {ext_txt} and {ext_bin}")
            # Ensure that the first uri contains the .bin file and the second uri contains the .txt file
            if (ext_bin != ".bin") and (ext_txt != ".txt"):
                uri_bin, uri_txt = uri_txt, uri_bin

        elif len(uris) == 1:
            uri_bin = uris[0]
            ext = os.path.splitext(uri_bin)[1]

            if ext == ".bin":
                fpath_bin = path_from_uri(uri_bin)
                fpath_txt = Path(fpath_bin).with_suffix(".txt")
                uri_txt = ensure_uri(fpath_txt) if os.path.exists(fpath_txt) else None
            else:
                raise ValueError(f"Expected a .bin file, got {ext}")

        return uri_bin, uri_txt

    @staticmethod
    def read_txt_file(uri: str) -> dict:

        def str_to_num(s: str) -> Union[int, float, str]:
            s = s.strip()
            if s.isdigit() or (s.startswith('-') and s[1:].isdigit()):
                return int(s)
            else:
                try:
                    return float(s)
                except ValueError:
                    return s

        with open(path_from_uri(uri), "r") as fp:
            content = fp.readlines()

        result = {}
        for line in content:
            key, value = line.strip().split(":", 1)
            key, value = key.strip(), value.strip()

            # Decide whether it's a list or a single number
            if "," in value:
                result[key] = [str_to_num(p) for p in value.split(",")]
            else:
                result[key] = str_to_num(value)

        return result

    @staticmethod
    def read_bin_file(uri, columns=None):
        """Read binary file into a structured numpy array with given columns.

        The binary file is assumed to contain int32 numbers in little-endian format.
        The last two columns are special: they represent the timestamp as seconds and nanoseconds;
        they are combined into a single float column "timestamp" and placed in the first column.
        The resulting (default) columns are: timestamp, i0, it, ir, iff, aux1, aux2, aux3, aux4.

        Parameters
        ----------
            uri : str
                URI of the binary file.
            columns : list of str
                List of column names after converting the timestamp.

        Returns
        -------
            dask.array.core.Array
                Structured array with the specified columns. the number of rows may differ from nrows
                once dask computes the array.
        """

        memmap = np.memmap(path_from_uri(uri), dtype=np.int32, mode="r")

        # If columns are not provided, determine the type and schema of the file:
        # APB trigger files have three columns, and the first column is always 0 or 1.
        if columns is None:
            if set(memmap[:60].reshape(-1, 3)[:, 0]) == {0, 1}:  # APB trigger file
                columns = ["timestamp", "transition"]
            else:  # APB electrometer file
                columns = ["timestamp", "i0", "it", "ir", "iff", "aux1", "aux2", "aux3", "aux4"]

        data = da.from_array(memmap).reshape(-1, len(columns)+1)
        ddf_ts = dask.dataframe.from_array(data[:, -2] + data[:, -1].astype('float') * 8.0051232 * 1e-9, columns=columns[:1])
        ddf = dask.dataframe.from_array(data[:, :-2], columns=columns[1:])
        ddf = dask.dataframe.concat([ddf_ts, ddf], axis=1)

        nrows = memmap.size // (len(columns) + 1)
        array = ddf.set_index(ddf.columns[0]).to_records(lengths=(nrows,)).reshape(-1, 1)

        return array

    @classmethod
    def from_catalog(
        cls,
        data_source: DataSource[ArrayStructure],
        node: Node,
    ) -> "PizzaBoxAdapter":

        # Load the array from binary file lazily with Dask
        bin_uris = [ast.data_uri for ast in data_source.assets if ast.parameter == "data_uris"]
        txt_uris = [ast.data_uri for ast in data_source.assets if ast.parameter == "metadata"]
        if len(bin_uris) != 1 or len(txt_uris) > 1:
            raise ValueError("Expected exactly one data_uris asset and at most one metadata asset")

        structure = data_source.structure
        assert isinstance(structure.data_type, StructDtype), "Array structure must be of StructDtype"
        columns = [f.name for f in structure.data_type.fields]
        array = cls.read_bin_file(bin_uris[0], columns=columns)

        # Read the metadata from the .txt file
        file_metadata = cls.read_txt_file(txt_uris[0]) if txt_uris else {}

        return cls(
            array,
            structure,
            metadata={**node.metadata_, **file_metadata},
            specs=node.specs,
        )

    @classmethod
    def from_uris(cls, *data_uris: str) -> "PizzaBoxAdapter":
        uri_bin, uri_txt = cls.locate_files(*data_uris)
        array = cls.read_bin_file(uri_bin)
        structure = ArrayStructure.from_array(array)

        metadata = cls.read_txt_file(uri_txt) if uri_txt else {}

        return cls(array, structure, metadata=metadata)

DEFAULT_ADAPTERS_BY_MIMETYPE.set("application/x-pizzabox-binary", lambda: PizzaBoxAdapter)

@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! dry_run: {dry_run}")


@flow
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    uid = stop_doc["run_start"]
    data_validation(uid, api_key=api_key, dry_run=dry_run)
    # Processing goes here
    run = get_run(uid, api_key=api_key)
    run_processed = get_run_processed(uid, api_key=api_key)
    process_interpolate_bin_with_tiled(run, run_processed)
    log_completion(dry_run=dry_run)
    return True
