import argparse

from parser import parse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Parse Walmart discounts and save them in desired format"
    )
    help_pattern = "Name {format} of file"
    formats = ("csv", "xlsx", "json")
    for format_ in formats:
        parser.add_argument(
            f"--{format_}",
            type=str,
            default=None,
            help=f"Name of {format_} file",
        )
    return vars(parser.parse_args())


METHODS = {
    "csv": {
        "method": "to_csv",
        "kwargs": {"index": False}
    },
    "xlsx": {
        "method": "to_excel",
        "kwargs": {"index": False}
    },
    "json": {
        "method": "to_json",
        "kwargs": {"orient": "records"}
    },
}


def save():
    output_files = parse_args()
    df = parse()
    for format_, file_name in output_files.items():
        if file_name is not None:
            method_dct = METHODS[format_]
            getattr(df, method_dct["method"])(file_name, **method_dct["kwargs"])

