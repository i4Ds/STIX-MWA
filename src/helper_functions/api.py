import os
import ssl
import time
import json
import requests
from urllib.request import urlretrieve

try:
    from urllib.parse import urlencode, urlparse
except:
    from urllib import urlencode

from websocket import create_connection, WebSocketConnectionClosedException, WebSocketTimeoutException
from requests.auth import HTTPBasicAuth
import pkg_resources  # part of setuptools


def get_api_version_number():
    """
    return api version number
    """
    
    # this is what we send to the server when we confirm version compatibility.
    version = pkg_resources.require("mantaray-client")[0].version  # format major.minor.revision
    version_parts = version.split(".")
    return "mantaray-clientv{0}.{1}".format(version_parts[0], version_parts[1])


def get_version_number():
    return pkg_resources.require("mantaray-client")[0].version


def get_pretty_version_string():
    return "manta-ray-client version {0}".format(get_version_number())


"""
notify wrapper around /api/job_results.
keeps the websocket alive with pings and can reconnect automatically.
"""

import json, ssl, time, threading
from websocket import (
    create_connection,
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
)

# ----------------------------------------------------------------------
# notify
# ----------------------------------------------------------------------
class Notify(object):
    """
    sends/receives job status messages over a websocket.
    """

    # ------------------------------------------------------------------
    # construction helpers
    # ------------------------------------------------------------------
    def __init__(
        self,
        session,             # Session wrapper (provides .session -> requests.Session)
        ws,                  # websocket-client connection
        *,
        https, host, port, api_key, sslopt, ping_interval, autopings
    ):
        self._session = session
        self._ws = ws

        # params needed for future reconnects
        self._https = https
        self._host = host
        self._port = port
        self._api_key = api_key
        self._sslopt = sslopt
        self._ping_interval = ping_interval
        self._autopings = autopings

        # manual ping loop for websocket-client < 1.0
        self._ping_thread = None
        if not autopings:
            self._start_ping_thread()

    # context-manager sugar
    def __enter__(self): return self
    def __exit__(self, *_): self.close()

    def close(self):
        try:
            self._ws.close()
        finally:
            self._session.close()

    # ------------------------------------------------------------------
    # public api
    # ------------------------------------------------------------------
    def recv(self, retries: int = 3, backoff: int = 5) -> dict | None:
        """
        receive one json message; silently reconnect on socket drop.
        returns None after exhausting retries.
        """
        for attempt in range(retries):
            try:
                frame = self._ws.recv()
                if frame:
                    return json.loads(frame)
                raise WebSocketConnectionClosedException()
            except (WebSocketConnectionClosedException, WebSocketTimeoutException):
                time.sleep(backoff * (attempt + 1))
                if self._reconnect():
                    continue          # try receive again immediately
        return None

    # ------------------------------------------------------------------
    # class factory
    # ------------------------------------------------------------------
    @classmethod
    def login(
        cls,
        https: str,
        host: str,
        port: str,
        api_key: str,
        *,
        sslopt: dict | None = None,
        ping_interval: int = 30,
    ) -> "Notify":
        """
        open websocket and return a ready notify object.
        https = "1" for https/wss, "0" for http/ws
        """
        # 1. authenticated http session (gets MWA_JOB_COOKIE)
        session = Session.login(https, host, port, api_key, verify=False)

        # 2. cookie header (extract from the underlying requests.Session)
        cookie_jar = session.session.cookies
        cookie_str = "; ".join(f"{k}={v}" for k, v in cookie_jar.items())
        if "MWA_JOB_COOKIE" not in cookie_str:
            raise RuntimeError("mwa_job_cookie missing after api_login")

        # 3. open websocket, prefer built-in ping_interval if available
        ws_scheme = "wss" if https == "1" else "ws"
        ws_url = f"{ws_scheme}://{host}:{port}/api/job_results"

        autopings = True
        try:
            ws = create_connection(
                ws_url,
                header={"Cookie": cookie_str},
                sslopt=sslopt or {"cert_reqs": ssl.CERT_NONE},
                ping_interval=ping_interval,
                ping_timeout=10,
            )
        except TypeError:            # websocket-client < 1.0
            autopings = False
            ws = create_connection(
                ws_url,
                header={"Cookie": cookie_str},
                sslopt=sslopt or {"cert_reqs": ssl.CERT_NONE},
            )

        return cls(
            session, ws,
            https=https, host=host, port=port, api_key=api_key,
            sslopt=sslopt, ping_interval=ping_interval, autopings=autopings
        )

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------
    def _reconnect(self) -> bool:
        """try to reopen the websocket; returns True on success."""
        try:
            new = Notify.login(
                self._https, self._host, self._port, self._api_key,
                sslopt=self._sslopt, ping_interval=self._ping_interval,
            )
            # swap objects
            self._session.close()
            self._session, self._ws = new._session, new._ws
            self._autopings = new._autopings
            if not self._autopings:
                self._start_ping_thread()
            return True
        except Exception:
            return False

    def _start_ping_thread(self):
        """manual ping every self._ping_interval seconds."""
        if self._ping_thread and self._ping_thread.is_alive():
            return

        def _ping():
            while True:
                time.sleep(self._ping_interval)
                try:
                    self._ws.ping()
                except Exception:
                    break

        self._ping_thread = threading.Thread(target=_ping, daemon=True)
        self._ping_thread.start()




class Session(object):

    def __init__(self,
                 https,
                 host,
                 port,
                 session,
                 verify):

        self.protocol = 'https' if https == '1' else 'http'
        self.websocket = 'wss' if https == '1' else 'ws'
        self.host = host
        self.port = port
        self.session = session
        self.verify = verify

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def close(self):
        self.session.close()

    @classmethod
    def login(cls,
              https,
              host,
              port,
              api_key,
              verify=False):

        requests.packages.urllib3.disable_warnings()

        session = requests.session()
        protocol = 'https' if https == '1' else 'http'
        url = "{0}://{1}:{2}/api/api_login".format(protocol, host, port)
        with session.post(url,
                          auth=HTTPBasicAuth(get_api_version_number(), api_key),
                          verify=verify) as r:
            r.raise_for_status()

            return Session(https,
                           host,
                           port,
                           session,
                           verify=verify)

    def submit_conversion_job(self,
                              obs_id,
                              time_res,
                              freq_res,
                              edge_width,
                              conversion,
                              calibrate,
                              flags=[]):
        data = {'obs_id': obs_id,
                'timeres': time_res,
                'freqres': freq_res,
                'edgewidth': edge_width,
                'conversion': conversion,
                'calibrate': calibrate}
        data.update(dict.fromkeys(flags, 1))
        return self.submit_conversion_job_direct(data)

    def submit_conversion_job_direct(self, parameters):
        url = "{0}://{1}:{2}/api/conversion_job".format(self.protocol, self.host, self.port)
        with self.session.post(url,
                               parameters,
                               verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def submit_download_job(self,
                            obs_id,
                            download_type):
        data = {'obs_id': obs_id,
                'download_type': download_type}
        return self.submit_download_job_direct(data)

    def submit_download_job_direct(self, parameters):
        url = "{0}://{1}:{2}/api/download_vis_job".format(self.protocol, self.host, self.port)
        with self.session.post(url,
                               parameters,
                               verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def submit_voltage_job_direct(self, parameters):
        url = "{0}://{1}:{2}/api/voltage_job".format(self.protocol, self.host, self.port)
        with self.session.post(url,
                               parameters,
                               verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def get_jobs(self):
        url = "{0}://{1}:{2}/api/get_jobs".format(self.protocol, self.host, self.port)
        with self.session.get(url, verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def cancel_job(self, job_id):
        url = "{0}://{1}:{2}/api/cancel_job".format(self.protocol, self.host, self.port)
        with self.session.get(url,
                              params=urlencode({'job_id': job_id}),
                              verify=self.verify) as r:
            r.raise_for_status()

    def download_file_product(self,
                              job_id,
                              url,
                              output_path):

        with requests.get(url, stream=True, timeout=10) as r:
            r.raise_for_status()

            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        return output_path