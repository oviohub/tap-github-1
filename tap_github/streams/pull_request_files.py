from singer_sdk import typing as th

from tap_github.client import GitHubStream
from tap_github.streams.pull_requests import PullRequestsStream


class PullRequestFilesStream(GitHubStream):
    """Defines 'PullRequestFiles' stream."""

    name = "pull_request_files"
    path = "/repos/{org}/{repo}/pulls/{pull_number}/files"
    primary_keys = ["filename"]
    parent_stream_type = PullRequestsStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org", "pull_number"]

    schema = th.PropertiesList(
        # Key (including parent)
        th.Property("filename", th.StringType),
        th.Property("pull_number", th.IntegerType),
        # Rest
        th.Property("sha", th.StringType),
        th.Property("status", th.StringType),
        th.Property("additions", th.IntegerType),
        th.Property("deletions", th.IntegerType),
        th.Property("changes", th.IntegerType),
        th.Property("blob_url", th.StringType),
        th.Property("raw_url", th.StringType),
        th.Property("contents_url", th.StringType),
        th.Property("patch", th.StringType),
    ).to_dict()
