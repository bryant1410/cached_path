import functools
import glob
import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from functools import wraps
from hashlib import sha256
from os import PathLike
from pathlib import Path
from typing import Any, Callable, IO, Literal, Optional, Tuple, Union
from urllib.parse import urlparse

import boto3
import botocore
import botocore.client
import requests
from botocore.exceptions import ClientError, EndpointConnectionError
from filelock import FileLock
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

TYPE_PATH = Union[PathLike, str, bytes]  # See https://stackoverflow.com/a/53418245/1165181

CACHE_DIR = os.path.join(os.getenv("CP_CACHE_ROOT", str(Path.home() / ".cache")), "cp")

TIMEOUT = 3.05  # For HTTP, it's gonna be double because there's connect and read timeout. For S3 we only use connect.

LOGGER = logging.getLogger(__name__)


def cached_path(url_or_filename: TYPE_PATH, cache_dir: Optional[TYPE_PATH] = None,
                extract_archive: Union[bool, Literal["auto"]] = "auto", force_extract: bool = False) -> str:
    """
    Given something that might be a URL (or might be a local path),
    determine which. If it's a URL, download the file and cache it, and
    return the path to the cached file. If it's already a local path,
    make sure the file exists and then return the path.
    # Parameters
    url_or_filename : `TYPE_PATH`
        A URL or local file to parse and possibly download.
    cache_dir : `TYPE_PATH`, optional (default = `None`)
        The directory to cache downloads.
    extract_archive : `bool`, optional (default = `"auto"`)
        If `True`, then zip or tar.gz archives will be automatically extracted.
        If `"auto"`, then it's extracted only if it contains a "!".
        In which case the directory is returned.
    force_extract : `bool`, optional (default = `False`)
        If `True` and the file is an archive file, it will be extracted regardless
        of whether or not the extracted directory already exists.

        !!! Warning
            Use this flag with caution! This can lead to race conditions if used
            from multiple processes on the same file.
    """
    cache_dir = cache_dir or CACHE_DIR

    if not isinstance(url_or_filename, str):
        url_or_filename = str(url_or_filename)

    # If we're using the /a/b/foo.zip!c/d/file.txt syntax, handle it here.
    exclamation_index = url_or_filename.find("!")

    if isinstance(extract_archive, str):
        if extract_archive == "auto":
            extract_archive = exclamation_index != -1
        else:
            raise ValueError(f"Invalid value for `extract_archive`: {extract_archive}")

    if extract_archive and exclamation_index >= 0:
        archive_path = url_or_filename[:exclamation_index]
        archive_path = cached_path(archive_path, cache_dir, True, force_extract)

        internal_path = url_or_filename[exclamation_index + 1:]
        if internal_path:
            if not os.path.isdir(archive_path):
                raise ValueError(f"{url_or_filename} uses the ! syntax, but does not specify an archive file.")
            return os.path.join(archive_path, internal_path)
        else:  # If it's just "!" without anything else then we just return the extracted path.
            return archive_path

    url_or_filename = os.path.expanduser(url_or_filename)
    parsed = urlparse(url_or_filename)

    if parsed.scheme in {"http", "https", "s3"}:
        # URL, so get it from the cache (downloading if necessary)
        file_path = _get_from_cache(url_or_filename, cache_dir)

        if extract_archive and _is_archive_file(file_path):
            # This is the path the file should be extracted to.
            # For example ~/.cache/cp/234234.21341 -> ~/.cache/cp/234234.21341-extracted
            extraction_path = file_path + "-extracted"
        else:
            extraction_path = None
    elif os.path.exists(url_or_filename):
        # File, and it exists.
        file_path = url_or_filename

        if extract_archive and _is_archive_file(file_path):
            # This is the path the file should be extracted to.
            # For example model.tar.gz -> model-tar-gz-extracted
            extraction_dir, extraction_name = os.path.split(file_path)
            extraction_name = f"{extraction_name.replace('.', '-')}-extracted"
            extraction_path = os.path.join(extraction_dir, extraction_name)
        else:
            extraction_path = None
    elif parsed.scheme == "":  # File, but it doesn't exist.
        raise FileNotFoundError(f"File {url_or_filename} not found.")
    else:
        raise ValueError(f"Unable to parse {url_or_filename} as a URL or as a local path.")

    if extraction_path:
        if not os.path.isdir(extraction_path) or not os.listdir(extraction_path) or force_extract:
            _extract(file_path, extraction_path, url_or_filename=url_or_filename, force_extract=force_extract)

        return extraction_path

    return file_path


def _url_to_filename(url: str, etag: Optional[str] = None) -> str:
    """Converts a URL into a hashed filename in a repeatable way. If an ETag is specified, append its hash to the
    URL's, delimited by a period."""
    url_bytes = url.encode("utf-8")
    url_hash = sha256(url_bytes)
    filename = url_hash.hexdigest()

    if etag:
        etag_bytes = etag.encode("utf-8")
        etag_hash = sha256(etag_bytes)
        filename += "." + etag_hash.hexdigest()

    return filename


def _is_archive_file(path: TYPE_PATH) -> bool:
    if zipfile.is_zipfile(path) or tarfile.is_tarfile(path):
        return True
    else:
        import rarfile
        return rarfile.is_rarfile(path)


def _extract(file_path: TYPE_PATH, extraction_path: TYPE_PATH, url_or_filename: str,
             force_extract: bool = False) -> None:
    with FileLock(f"{file_path}.lock"):
        # Check again if the directory exists now that we've acquired the lock.
        if os.path.isdir(extraction_path) and os.listdir(extraction_path):
            if force_extract:
                LOGGER.warning(f"Extraction directory for {url_or_filename} ({extraction_path}) already exists, "
                               "overwriting it since `force_extract` is `True`.")
            else:
                return

        shutil.rmtree(extraction_path, ignore_errors=True)
        os.makedirs(extraction_path)
        if zipfile.is_zipfile(file_path):
            with zipfile.ZipFile(file_path) as file:
                file.extractall(extraction_path)
        elif tarfile.is_tarfile(file_path):
            with tarfile.open(file_path) as file:
                file.extractall(extraction_path)
        else:
            import rarfile
            if rarfile.is_rarfile(file_path):
                with rarfile.RarFile(file_path) as file:
                    file.extractall(extraction_path)
            else:
                raise ValueError(f"Unsupported archive file (not ZIP, TAR, nor RAR): {file_path}")


def _split_s3_path(url: str) -> Tuple[str, str]:
    """Split a full S3 path into the bucket name and path."""
    parsed = urlparse(url)
    if not parsed.netloc or not parsed.path:
        raise ValueError(f"Bad S3 path {url}.")
    bucket_name = parsed.netloc
    s3_path = parsed.path
    # Remove "/" at beginning of the path.
    if s3_path.startswith("/"):
        s3_path = s3_path[1:]
    return bucket_name, s3_path


def _s3_request(func: Callable) -> Callable:
    """Wrapper function for S3 requests in order to create more helpful error messages."""

    @wraps(func)
    def wrapper(url: str, *args, **kwargs):
        try:
            return func(url, *args, **kwargs)
        except ClientError as exc:
            if int(exc.response["Error"]["Code"]) == 404:
                raise FileNotFoundError(f"File {url} not found.")
            else:
                raise

    return wrapper


@functools.lru_cache()
def _get_s3_resource():
    session = boto3.session.Session()
    kwargs = {"connect_timeout": TIMEOUT}
    if session.get_credentials() is None:
        kwargs["signature_version"] = botocore.UNSIGNED
    s3_resource = session.resource("s3", config=botocore.client.Config(**kwargs))
    return s3_resource


@_s3_request
def _s3_etag(url: str) -> Optional[str]:
    """Check ETag on S3 object."""
    bucket_name, s3_path = _split_s3_path(url)
    return _get_s3_resource().Object(bucket_name, s3_path).e_tag


# No `@_s3_request` annotation as that error shouldn't happen.
def _try_s3_prefix_sync(source_s3_path: str, dest_path: str) -> bool:
    bucket_name, s3_path = _split_s3_path(source_s3_path)
    bucket = _get_s3_resource().Bucket(bucket_name)

    try:
        bucket.Object(s3_path).load()
        is_object = True
    except ClientError as exc:
        if int(exc.response["Error"]["Code"]) == 404:
            is_object = False
        else:
            raise

    if is_object:
        return False

    LOGGER.info(f"Syncing from {source_s3_path}…")
    subprocess.run(["aws", "s3", "sync", source_s3_path, dest_path], check=True)
    LOGGER.info(f"{source_s3_path} synced.")

    return True


@_s3_request
def _s3_get(url: str, file: IO) -> None:
    """Pull a file directly from S3."""
    bucket_name, s3_path = _split_s3_path(url)
    _get_s3_resource().Bucket(bucket_name).download_fileobj(s3_path, file)


@functools.lru_cache()
def _session_with_backoff() -> requests.Session:
    """We ran into an issue where HTTP requests to S3 were timing out, possibly because we were making too many
    requests too quickly. This helper function returns a requests session that has retry-with-backoff
    builtin. See
    <https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library>."""
    session = requests.Session()
    retries = Retry(total=1, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))  # noqa
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _http_etag(url: str) -> Optional[str]:
    with _session_with_backoff() as session:
        response = session.head(url, allow_redirects=True, timeout=TIMEOUT)
    if response.status_code != 200:
        LOGGER.warning(f"HEAD request failed for URL {url} with status code {response.status_code}.")
        return None
    return response.headers.get("ETag")


def _http_get(url: str, file: IO) -> None:
    with _session_with_backoff() as session:
        request = session.get(url, stream=True)
        request.raise_for_status()
        content_length = request.headers.get("Content-Length")
        total = None if content_length is None else int(content_length)
        with tqdm(unit="B", total=total, desc="Downloading") as progress:
            for chunk in request.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    progress.update(len(chunk))
                    file.write(chunk)


def _find_latest_cached(url: str, cache_dir: Union[str, Path]) -> Optional[str]:
    return max((path
                for path in glob.glob(os.path.join(cache_dir, _url_to_filename(url)) + "*")
                if not path.endswith((".json", ".lock", "-extracted"))), key=os.path.getmtime, default=None)


class _CacheFile:
    """Context manager that makes robust caching easier.
    On `__enter__`, an IO handle to a temporarily file is returned, which can
    be treated as if it's the actual cache file.
    On `__exit__`, the temporarily file is renamed to the cache file. If anything
    goes wrong while writing to the temporary file, it will be removed."""

    def __init__(self, cache_filename: TYPE_PATH, mode: str = "w+b", suffix: str = ".tmp") -> None:
        self.cache_filename = cache_filename if isinstance(cache_filename, Path) else Path(cache_filename)
        self.cache_directory = os.path.dirname(self.cache_filename)
        self.mode = mode
        self.temp_file = tempfile.NamedTemporaryFile(self.mode, dir=self.cache_directory, delete=False, suffix=suffix)

    def __enter__(self) -> IO:
        return self.temp_file

    def __exit__(self, exc_type: Any, exc_value: Optional[Any], traceback: Any) -> bool:
        self.temp_file.close()
        if exc_value is None:
            LOGGER.debug(f"Renaming temp file {self.temp_file.name} to cache at {self.cache_filename}.")
            # Rename the temp file to the actual cache filename.
            os.replace(self.temp_file.name, self.cache_filename)
            return True
        # Something went wrong, remove the temp file.
        LOGGER.debug(f"Removing temp file {self.temp_file.name}.")
        os.remove(self.temp_file.name)
        return False


# TODO(joelgrus): do we want to do checksums or anything like that?
def _get_from_cache(url: str, cache_dir: Optional[TYPE_PATH] = None) -> str:
    """Given a URL, look for the corresponding dataset in the local cache. If it's not there, download it. Then
    return the path to the cached file. """
    cache_dir = cache_dir or CACHE_DIR

    os.makedirs(cache_dir, exist_ok=True)

    # Get ETag to add to filename, if it exists.
    try:
        if url.startswith("s3://"):
            etag = _s3_etag(url)
        else:
            etag = _http_etag(url)
    except (ConnectionError, EndpointConnectionError, requests.exceptions.Timeout,
            botocore.exceptions.requests.exceptions.Timeout):
        # We might be offline, in which case we don't want to throw an error
        # just yet. Instead, we'll try to use the latest cached version of the
        # target resource, if it exists. We'll only throw an exception if we
        # haven't cached the resource at all yet.
        LOGGER.warning(f"Connection error occurred while trying to fetch ETag for {url}. "
                       "Will attempt to use latest cached version of resource")
        latest_cached = _find_latest_cached(url, cache_dir)
        if latest_cached:
            LOGGER.info("ETag request failed with connection error, using latest cached "
                        f"version of {url}: {latest_cached}")
            return latest_cached
        else:
            LOGGER.error(f"Connection failed while trying to fetch ETag, and no cached version of {url} could be found")
            raise
    except OSError:
        # OSError may be triggered if we were unable to fetch the ETag.
        # If this is the case, try to proceed without ETag check.
        etag = None

    filename = _url_to_filename(url, etag)

    cache_path = os.path.join(cache_dir, filename)  # Get cache path to put the file.

    # Multiple processes may be trying to cache the same file at once, so we need
    # to be a little careful to avoid race conditions. We do this using a lock file.
    # Only one process can own this lock file at a time, and a process will block
    # on the call to `lock.acquire()` until the process currently holding the lock
    # releases it.
    LOGGER.debug(f"Waiting to acquire lock on {cache_path}…")
    with FileLock(f"{cache_path}.lock"):
        LOGGER.debug(f"Lock acquired for {cache_path}.")

        if not url.startswith("s3://") or not _try_s3_prefix_sync(url, cache_path):
            if os.path.exists(cache_path):
                # FIXME: if there's no ETag, we don't actually know if it's up-to-date.
                LOGGER.info(f"Cache of {url} is up-to-date.")
            else:
                with _CacheFile(cache_path) as cache_file:
                    LOGGER.info(f"{url} not found in cache, downloading to {cache_path}.")

                    if url.startswith("s3://"):
                        _s3_get(url, cache_file)
                    else:
                        _http_get(url, cache_file)

                LOGGER.debug(f"Creating metadata file for {cache_path}.")
                with open(f"{cache_path}.json", "w") as file:
                    json.dump({"url": url, "etag": etag}, file)

    return cache_path
