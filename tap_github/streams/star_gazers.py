from typing import Optional

from singer_sdk import typing as th

from tap_github.client import GitHubStream
from tap_github.streams.repository import RepositoryStream


class StargazersStream(GitHubStream):
    """Defines 'Stargazers' stream. Warning: this stream does NOT track star deletions."""

    name = "stargazers"
    path = "/repos/{org}/{repo}/stargazers"
    primary_keys = ["repo", "org", "user_id"]
    parent_stream_type = RepositoryStream
    state_partitioning_keys = ["repo", "org"]
    replication_key = "starred_at"

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Overridden to use an endpoint which includes starred_at property:
        https://docs.github.com/en/rest/reference/activity#custom-media-types-for-starring
        """
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.v3.star+json"
        return headers

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """
        Add a user_id top-level field to be used as state replication key.
        """
        row["user_id"] = row["user"]["id"]
        return row

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("user_id", th.StringType),
        # Stargazer Info
        th.Property("starred_at", th.DateTimeType),
        th.Property(
            "user",
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
