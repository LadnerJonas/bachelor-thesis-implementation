import re
import argparse
import pandas as pd

# Regex pattern to extract relevant fields
PATTERN = re.compile(
    r"Benchmarking \((?P<synchronised>not-synchronised|synchronised)\) using "
    r"(?P<partitions>\d+) Partitions and (?P<threads>\d+) Thread\(s\): written "
    r"(?P<tuple_bytes>\d+)B tuples: (?P<written_tuples>[\d\.]+) Mio"
)


def parse_log_file(file_path: str) -> pd.DataFrame:
    """Parses the benchmark log file and returns a sorted DataFrame."""
    data = []

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            if match := PATTERN.search(line):
                entry = match.groupdict()
                entry["synchronised"] = (
                    0 if entry["synchronised"] == "not-synchronised" else 1
                )
                entry["tuple_bytes"] = int(entry["tuple_bytes"])
                entry["partitions"] = int(entry["partitions"])
                entry["threads"] = int(entry["threads"])
                entry["written_tuples"] = int(
                    int(float(entry["written_tuples"]) * 100) * 1_000_000 / 100
                )  # Convert Mio to absolute
                data.append(entry)

    df = pd.DataFrame(data)
    return df.sort_values(
        by=["synchronised", "tuple_bytes", "partitions", "threads"], ascending=True
    )


def main():
    parser = argparse.ArgumentParser(description="Parse and sort benchmark log file.")
    parser.add_argument("file", type=str, help="Path to the log file")
    args = parser.parse_args()

    df = parse_log_file(args.file)
    query_result = df[
        (df["synchronised"] == 0)  # 0 means not-synchronised
        & (df["tuple_bytes"] == 4)  # 4B tuples
        & (df["partitions"] == 32)  # 32 partitions
        & (df["threads"] == 20)  # 20 threads
    ]
    if not query_result.empty:
        df.to_json(
            "plot/server-theoretical-slotted-page.json", orient="records", indent=2
        )

    else:
        df.to_json(
            "plot/laptop-theoretical-slotted-page.json", orient="records", indent=2
        )

    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
