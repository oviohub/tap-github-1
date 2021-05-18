"""Stream type classes for tap-github."""

from typing import Any, Dict, Iterable, Optional

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_github.client import GitHubStream


class RepositoryStream(GitHubStream):
    """Defines 'Repository' stream."""

    # Search API max: 100 per page, 1,000 total
    MAX_PER_PAGE = 100
    MAX_RESULTS_LIMIT = 1000

    def __init__(
        self,
        tap,
        name: Optional[str] = None,
        schema=None,
        path: Optional[str] = None,
        query: str = None,
    ):
        super().__init__(tap=tap, name=name, schema=schema, path=path)
        self.query = query

    def get_url_params(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(partition, next_page_token)
        params["q"] = self.query
        return params

    def get_child_context(self, record: dict, context: dict = None) -> Optional[Dict]:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """

        return {
            "org": record["owner"]["login"],
            "repo": record["name"],
        }

    path = "/search/repositories"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("name", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property("description", th.StringType),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("type", th.StringType),
                th.Property("avatar_url", th.StringType),
            ),
        ),
        th.Property(
            "license",
            th.ObjectType(
                th.Property("key", th.StringType),
                th.Property("name", th.StringType),
                th.Property("url", th.StringType),
                th.Property("spdx_id", th.StringType),
            ),
        ),
        th.Property("master_branch", th.StringType),
        th.Property("default_branch", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("pushed_at", th.DateTimeType),
        th.Property("private", th.BooleanType),
        th.Property("size", th.IntegerType),
        th.Property("stargazers_count", th.IntegerType),
        th.Property("fork", th.BooleanType),
        # These `_count` metrics appear to be duplicates but have valid data
        # and are documented: https://docs.github.com/en/rest/reference/search
        th.Property("forks", th.IntegerType),
        th.Property("forks_count", th.IntegerType),
        th.Property("watchers", th.IntegerType),
        th.Property("watchers_count", th.IntegerType),
        th.Property("open_issues", th.IntegerType),
        th.Property("open_issues_count", th.IntegerType),
    ).to_dict()


class IssuesStream(GitHubStream):
    """Defines 'Issues' stream."""

    name = "Issues"
    path = "/repos/{org}/{repo}/issues"
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True

    def get_child_context(self, record: dict, context: dict = None) -> Optional[Dict]:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """
        if context is None:
            raise ValueError("Issue stream should not have blank context.")

        context["issue_number"] = record["number"]
        context["comments"] = record["comments"]  # If zero, comments can be skipped
        return context

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Overridden to use beta endpoint which includes reactions as described here:
        https://developer.github.com/changes/2016-05-12-reactions-api-preview/
        """
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.squirrel-girl-preview"
        return headers

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("issue_number", th.IntegerType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("closed_at", th.DateTimeType),
        th.Property("state", th.StringType),
        th.Property("title", th.StringType),
        th.Property("comments", th.IntegerType),
        th.Property("author_association", th.StringType),
        th.Property("body", th.StringType),
    ).to_dict()


class IssueCommentsStream(GitHubStream):
    """Defines 'Issues' stream."""

    name = "IssueComments"
    path = "/repos/{org}/{repo}/issues/{issue_number}/comments"
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = IssuesStream
    partition_keys = ["repo", "org"]
    ignore_parent_replication_key = False

    def get_records(self, partition: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        if partition and partition.get("comments", None) == 0:
            self.logger.debug(f"No comments detected. Skipping '{self.name}' sync.")
            return []

        return super().get_records(partition)

    def get_url_params(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(partition, next_page_token)
        if self.replication_key:
            since = self.get_starting_timestamp(partition)
            if since:
                params["since"] = since
        return params

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("issue_number", th.IntegerType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("author_association", th.StringType),
        th.Property("body", th.StringType),
    ).to_dict()
