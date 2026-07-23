# global imports
import random
import sys
import time
import logging
from urllib.parse import urlparse, parse_qs
import webbrowser
from collections import Counter

import requests
from fake_useragent import UserAgent

from requests_ratelimiter import LimiterAdapter

# local imports
from getmyancestors.classes.translation import translations

DEFAULT_CLIENT_ID = "a02j000000KTRjpAAH"
DEFAULT_REDIRECT_URI = "https://misbach.github.io/fs-auth/index_raw.html"

logger = logging.getLogger("getmyancestors")


class Stats:
    """Track export statistics"""

    def __init__(self):
        self.retry_count = 0
        self.status_codes = Counter()
        self.max_retries_reached = 0
        self.start_time = time.time()

    def record_status(self, status_code):
        self.status_codes[status_code] += 1

    def record_retry(self):
        self.retry_count += 1

    def record_max_retries(self):
        self.max_retries_reached += 1

    def elapsed(self):
        return time.time() - self.start_time

    def summary(self):
        lines = []
        lines.append("  Retries: %d" % self.retry_count)
        lines.append("  Max retries reached: %d" % self.max_retries_reached)
        if self.status_codes:
            lines.append("  Status codes:")
            for code, count in sorted(self.status_codes.items()):
                lines.append("    %d: %d" % (code, count))
        return "\n".join(lines)


class Session(requests.Session):
    """Create a FamilySearch session
    :param username and password: valid FamilySearch credentials
    :param verbose: True to active verbose mode
    :param logfile: a file object or similar
    :param timeout: time before retry a request
    """

    def __init__(
        self,
        username,
        password,
        client_id=None,
        redirect_uri=None,
        verbose=False,
        logfile=False,
        timeout=60,
        rate_limit=None,
        initial_backoff=10,
    ):
        super().__init__()
        self.username = username
        self.password = password
        self.client_id = client_id or DEFAULT_CLIENT_ID
        self.redirect_uri = redirect_uri or DEFAULT_REDIRECT_URI
        self.verbose = verbose
        self.logfile = logfile
        self.timeout = timeout
        self.initial_backoff = initial_backoff
        self.fid = self.lang = self.display_name = None
        self.counter = 0
        self.stats = Stats()
        self.headers = {"User-Agent": UserAgent().firefox}
        self.write_log(
            "Config: timeout=%ds, initial_backoff=%ds, rate_limit=%s"
            % (timeout, initial_backoff, rate_limit or "unlimited")
        )

        # Apply a rate-limit (max # requests per second) to all endpoints
        if rate_limit:
            adapter = LimiterAdapter(per_second=rate_limit)
            self.mount('http://', adapter)
            self.mount('https://', adapter)

        self.login()

    @property
    def logged(self):
        return bool(self.cookies.get("fssessionid"))

    def write_log(self, text, level=logging.INFO):
        """write text in the log file"""
        logger.log(level, text)

    def login(self):
        """retrieve FamilySearch session ID
        (https://familysearch.org/developers/docs/guides/oauth2)
        """
        while True:
            try:
                url = "https://www.familysearch.org/auth/familysearch/login"
                self.write_log("Downloading: " + url)
                self.get(url, headers=self.headers)
                xsrf = self.cookies["XSRF-TOKEN"]
                url = "https://ident.familysearch.org/login"
                self.write_log("Logging in: " + url)
                res = self.post(
                    url,
                    data={
                        "_csrf": xsrf,
                        "username": self.username,
                        "password": self.password,
                    },
                    headers=self.headers,
                )
                res.raise_for_status()

                url = f"https://ident.familysearch.org/cis-web/oauth2/v3/authorization"
                params = {
                    "response_type": "code",
                    "scope": "openid profile email qualifies_for_affiliate_account country",
                    "client_id": self.client_id,
                    "redirect_uri": self.redirect_uri,
                    "username": self.username,
                }
                self.write_log("Getting an authorization code: " + url)
                response = self.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                try:
                    code = parse_qs(urlparse(response.url).query).get("code")[0]
                except Exception as e:
                    webbrowser.open(response.url)
                    print(
                        "Please log in to the web page that just opened and try again."
                    )
                    sys.exit(2)

                url = "https://ident.familysearch.org/cis-web/oauth2/v3/token"
                self.write_log("Exchanging for an access token: " + url)
                res = self.post(
                    url,
                    data={
                        "grant_type": "authorization_code",
                        "client_id": self.client_id,
                        "code": code,
                        "redirect_uri": self.redirect_uri,
                    },
                    headers=self.headers,
                )

                try:
                    data = res.json()
                except ValueError:
                    self.write_log("Invalid auth request")
                    continue

                if "access_token" not in data:
                    self.write_log(res.text)
                    continue
                access_token = data["access_token"]
                self.headers.update({"Authorization": f"Bearer {access_token}"})

            except requests.exceptions.ReadTimeout:
                self.write_log("Read timed out")
                continue
            except requests.exceptions.ConnectionError:
                self.write_log("Connection aborted")
                time.sleep(self.timeout)
                continue
            except requests.exceptions.HTTPError:
                self.write_log("HTTPError")
                time.sleep(self.timeout)
                continue
            except KeyError:
                self.write_log("KeyError")
                time.sleep(self.timeout)
                continue
            except ValueError:
                self.write_log("ValueError")
                time.sleep(self.timeout)
                continue
            if self.logged:
                self.set_current()
                break

    def get_url(self, url, headers=None, no_api=False):
        """retrieve JSON structure from a FamilySearch URL"""
        self.counter += 1
        if headers is None:
            headers = {"Accept": "application/x-gedcomx-v1+json"}
        headers.update(self.headers)
        base = "https://api.familysearch.org"
        if no_api:
            base = "https://familysearch.org"
        max_retries = 5
        retry_count = 0
        while True:
            try:
                r = self.get(base + url, timeout=self.timeout, headers=headers)
                self.write_log("GET %s -> %d" % (url, r.status_code))
            except requests.exceptions.ReadTimeout:
                retry_count += 1
                self.stats.record_retry()
                if retry_count >= max_retries:
                    self.stats.record_max_retries()
                    self.write_log(
                        "GET %s -> Max retries (%d) reached, giving up"
                        % (url, max_retries),
                        level=logging.WARNING,
                    )
                    return None
                backoff = random.uniform(0, min(self.initial_backoff * (2 ** (retry_count - 1)), 300))
                self.write_log(
                    "GET %s -> Read timed out (retry %d/%d in %ds)"
                    % (url, retry_count, max_retries, backoff)
                )
                time.sleep(backoff)
                continue
            except requests.exceptions.ConnectionError:
                retry_count += 1
                self.stats.record_retry()
                if retry_count >= max_retries:
                    self.stats.record_max_retries()
                    self.write_log(
                        "GET %s -> Max retries (%d) reached, giving up"
                        % (url, max_retries),
                        level=logging.WARNING,
                    )
                    return None
                backoff = random.uniform(0, min(self.initial_backoff * (2 ** (retry_count - 1)), 300))
                self.write_log(
                    "GET %s -> Connection aborted (retry %d/%d in %ds)"
                    % (url, retry_count, max_retries, backoff)
                )
                time.sleep(backoff)
                continue
            self.stats.record_status(r.status_code)
            if r.status_code == 204:
                return None
            if r.status_code in {404, 405, 410, 500}:
                logger.warning("GET %s -> HTTP %d", url, r.status_code)
                return None
            if r.status_code == 401:
                self.login()
                continue
            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError:
                if r.status_code == 403:
                    if (
                        "message" in r.json()["errors"][0]
                        and r.json()["errors"][0]["message"]
                        == "Unable to get ordinances."
                    ):
                        logger.warning(
                            "Unable to get ordinances. "
                            "Try with an LDS account or without option -c."
                        )
                        return "error"
                    logger.warning(
                        "GET %s -> HTTP 403: %s",
                        url,
                        r.json()["errors"][0]["message"] or "",
                    )
                    return None
                retry_count += 1
                self.stats.record_retry()
                if retry_count >= max_retries:
                    self.stats.record_max_retries()
                    self.write_log(
                        "GET %s -> Max retries (%d) reached (HTTP %d), giving up"
                        % (url, max_retries, r.status_code),
                        level=logging.WARNING,
                    )
                    return None
                backoff = random.uniform(0, min(self.initial_backoff * (2 ** (retry_count - 1)), 300))
                self.write_log(
                    "GET %s -> HTTP %d (retry %d/%d in %ds)"
                    % (url, r.status_code, retry_count, max_retries, backoff)
                )
                time.sleep(backoff)
                continue
            try:
                return r.json()
            except Exception as e:
                self.write_log("GET %s -> Corrupted response: %s" % (url, e), level=logging.WARNING)
                return None

    def set_current(self):
        """retrieve FamilySearch current user ID, name and language"""
        url = "/platform/users/current"
        data = self.get_url(url)
        if data:
            self.fid = data["users"][0]["personId"]
            self.lang = data["users"][0]["preferredLanguage"]
            self.display_name = data["users"][0]["displayName"]

    def _(self, string):
        """translate a string into user's language
        TODO replace translation file for gettext format
        """
        if string in translations and self.lang in translations[string]:
            return translations[string][self.lang]
        return string
