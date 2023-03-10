import pandas as pd
import glob
import sys
import click
import re


@click.command()
@click.option('--cutoff', default=500,
              help='Minimum population size to include.')
@click.option('--time_regex', default="[0-9]+",
              help="""A regular expression that will extract the time step of
                      a data point from a filename.""")
@click.option('--output_file_root', default="all_phylogeny",
              help='String to start output file names with.')
@click.argument('pattern')
def aggregate_muller_data_to_file(pattern, cutoff,
                                  time_regex, output_file_root):
    all_edge_data, all_size_data = \
        aggregate_muller_data(pattern, cutoff, time_regex)
    all_edge_data.to_csv(output_file_root + "_edges.csv", index=False)
    all_size_data.to_csv(output_file_root + "_sizes.csv", index=False)


def aggregate_muller_data(pattern, cutoff, time_regex):
    edge_dfs = []
    size_dfs = []

    filenames = glob.glob(pattern)
    if len(filenames) == 0:
        print("Error: No file names matched pattern", sys.argv[1])
        exit(1)

    for filename in filenames:
        df = pd.read_csv(filename)
        size_df = pd.DataFrame()
        size_df["Id"] = df["id"]
        size_df["filename"] = filename
        size_df["Step"] = re.search(time_regex, filename).group(0)
        size_df["Pop"] = df["num_orgs"]
        size_df["info"] = df["info"]
        edge_dfs.append(df)
        size_dfs.append(size_df)

    all_edge_data = pd.concat(edge_dfs)
    agg_functions = {"id": "first",
                     "ancestor_list": "first",
                     "origin_time": "first",
                     "destruction_time": "min",
                     "info": "first",
                     "tot_orgs": "max"
                     }
    all_edge_data = all_edge_data.groupby(all_edge_data["id"]).aggregate(agg_functions)
    all_edge_data.reset_index(inplace=True, drop=True)
    all_size_data = pd.concat(size_dfs)

    all_edge_data.sort_values(axis=0, by="id", inplace=True)
    all_edge_data["ParentId"] = all_edge_data["ancestor_list"].str.strip("[]")
    all_edge_data.rename(columns={"id": "ChildId"}, inplace=True)
    all_size_data.sort_values(axis=0, by=["Id", "Step"], inplace=True)
    all_edge_data.replace("NONE", "0", inplace=True)
    all_edge_data["ParentId"] = pd.to_numeric(all_edge_data["ParentId"])
    all_edge_data["ChildId"] = pd.to_numeric(all_edge_data["ChildId"])
    all_size_data["Id"] = pd.to_numeric(all_size_data["Id"])
    all_size_data["Step"] = pd.to_numeric(all_size_data["Step"])
    all_size_data["Pop"] = pd.to_numeric(all_size_data["Pop"])
    all_size_data["info"] = pd.to_numeric(all_size_data["info"])

    count_df = all_edge_data.groupby(by="ParentId").count()["ChildId"]
    count_df.rename("total_offspring", inplace=True)
    count_df.rename_axis("ChildId")
    all_edge_data = all_edge_data.join(count_df, on="ChildId")
    all_edge_data.fillna(0, inplace=True)

    all_edge_data.set_index("ChildId", inplace=True)

    remove_df = all_edge_data[(all_edge_data["total_offspring"] == 0) & (all_edge_data["tot_orgs"] < cutoff) & (all_edge_data["ParentId"] != 0)]

    while remove_df.shape[0] > 0:

        all_edge_data.loc[remove_df["ParentId"], "total_offspring"] -= 1
        all_edge_data.drop(remove_df.index, inplace=True)

        remove_df = all_edge_data[(all_edge_data["total_offspring"] <= 0) & (all_edge_data["tot_orgs"] < cutoff) & (all_edge_data["ParentId"] != 0)]

    all_edge_data = all_edge_data[(all_edge_data["total_offspring"] > 0) | (all_edge_data["tot_orgs"] > cutoff)]

    all_edge_data.reset_index(inplace=True)
    all_size_data = all_size_data[all_size_data["Id"].isin(all_edge_data["ChildId"])]

    return all_edge_data, all_size_data


if __name__ == "__main__":
    aggregate_muller_data_to_file()
