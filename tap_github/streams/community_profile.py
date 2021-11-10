from singer_sdk import typing as th

from tap_github.client import GitHubStream
from tap_github.streams.repository import RepositoryStream


class CommunityProfileStream(GitHubStream):
    """Defines 'CommunityProfile' stream."""

    name = "community_profile"
    path = "/repos/{org}/{repo}/community/profile"
    primary_keys = ["repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]
    tolerated_http_errors = [404]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        # Community Profile
        th.Property("health_percentage", th.IntegerType),
        th.Property("description", th.StringType),
        th.Property("documentation", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("content_reports_enabled", th.BooleanType),
        th.Property(
            "files",
            th.ObjectType(
                th.Property(
                    "code_of_conduct",
                    th.ObjectType(
                        th.Property("key", th.StringType),
                        th.Property("name", th.StringType),
                        th.Property("html_url", th.StringType),
                        th.Property("url", th.StringType),
                    ),
                ),
                th.Property(
                    "code_of_conduct_file",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
                th.Property(
                    "contributing",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
                th.Property(
                    "issue_template",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
                th.Property(
                    "pull_request_template",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
                th.Property(
                    "license",
                    th.ObjectType(
                        th.Property("key", th.StringType),
                        th.Property("name", th.StringType),
                        th.Property("spdx_id", th.StringType),
                        th.Property("node_id", th.StringType),
                        th.Property("html_url", th.StringType),
                        th.Property("url", th.StringType),
                    ),
                ),
                th.Property(
                    "readme",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
            ),
        ),
    ).to_dict()
