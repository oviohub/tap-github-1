from typing import Optional

from singer_sdk import typing as th

from tap_github.client import GitHubStream
from tap_github.streams.repository import RepositoryStream


class CommitsStream(GitHubStream):
    """
    Defines the 'Commits' stream.
    The stream is fetched per repository to maximize optimize for API quota
    usage.
    """

    name = "commits"
    path = "/repos/{org}/{repo}/commits"
    primary_keys = ["node_id"]
    replication_key = "commit_timestamp"
    parent_stream_type = RepositoryStream
    state_partitioning_keys = ["repo", "org"]
    ignore_parent_replication_key = True

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """
        Add a timestamp top-level field to be used as state replication key.
        It's not clear from github's API docs which time (author or committer)
        is used to compare to the `since` argument that the endpoint supports.
        """
        row["commit_timestamp"] = row["commit"]["committer"]["date"]
        return row

    schema = th.PropertiesList(
        th.Property("node_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("sha", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("commit_timestamp", th.DateTimeType),
        th.Property(
            "commit",
            th.ObjectType(
                th.Property(
                    "author",
                    th.ObjectType(
                        th.Property("name", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("date", th.DateTimeType),
                    ),
                ),
                th.Property(
                    "committer",
                    th.ObjectType(
                        th.Property("name", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("date", th.DateTimeType),
                    ),
                ),
                th.Property("message", th.StringType),
                th.Property(
                    "tree",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("sha", th.StringType),
                    ),
                ),
                th.Property(
                    "verification",
                    th.ObjectType(
                        th.Property("verified", th.BooleanType),
                        th.Property("reason", th.StringType),
                        th.Property("signature", th.StringType),
                        th.Property("payload", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property(
            "author",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("id", th.IntegerType),
                th.Property("node_id", th.StringType),
                th.Property("avatar_url", th.StringType),
                th.Property("gravatar_id", th.StringType),
                th.Property("html_url", th.StringType),
                th.Property("type", th.StringType),
                th.Property("site_admin", th.BooleanType),
            ),
        ),
        th.Property(
            "committer",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("id", th.IntegerType),
                th.Property("node_id", th.StringType),
                th.Property("avatar_url", th.StringType),
                th.Property("gravatar_id", th.StringType),
                th.Property("html_url", th.StringType),
                th.Property("type", th.StringType),
                th.Property("site_admin", th.BooleanType),
            ),
        ),
    ).to_dict()
