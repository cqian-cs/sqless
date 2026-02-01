import requests
import orjson
import time
import io
import os
from typing import Any, Generator, Optional, Union


class RDB:
    """
    Remote key-value FS + simple table API.
    """

    def __init__(self, host: str, secret: str, timeout = (10, 600)):
        self.host = host.rstrip("/")
        self.secret = secret
        self.timeout = timeout
        self.headers = {"Authorization": f"Bearer {self.secret}"}

    # -------------------------------
    # File Storage
    # -------------------------------

    def fs_set(self, key: str, data_or_path: Union[str, Any], retry: int = 5) -> bool:
        url = f"{self.host}/fs/{key}"

        for i in range(retry):
            try:
                if isinstance(data_or_path, str) and os.path.exists(data_or_path):
                    with open(data_or_path, "rb") as f:
                        files = {"file": (key, f)}
                        r = requests.post(url, files=files, headers=self.headers, timeout=self.timeout)
                else:
                    if type(data_or_path)==str:
                        buf = io.BytesIO(data_or_path.encode())
                    elif type(data_or_path)==bytes:
                        buf = io.BytesIO(data_or_path)
                    else:
                        buf = io.BytesIO(orjson.dumps(data_or_path))
                    files = {"file": (key, buf)}
                    r = requests.post(url, files=files, headers=self.headers, timeout=self.timeout)

                ret = r.json()
                return bool(ret.get("suc"))

            except Exception as e:
                print(f"fs_set exception: {e} (retry {retry-i})")
                time.sleep(1)

        return False


    def fs_get(
        self,
        key: str,
        path: Optional[str] = None,
        overwrite: bool = False,
        retry: int = 3
    ) -> Union[bool, bytes, None]:
        """
        Get remote FS content. Save to path if provided.
        """
        url = f"{self.host}/fs/{key}"

        if path and not overwrite and os.path.exists(path):
            return True

        for _ in range(retry):
            try:
                r = requests.get(url, headers=self.headers, timeout=self.timeout)

                if r.status_code == 200 and isinstance(r.content, bytes):
                    if path is None:
                        return r.content

                    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                    with open(path, "wb") as f:
                        f.write(r.content)
                    return True

                return None if path is None else False

            except Exception:
                time.sleep(1)

        return None if path is None else False

    def fs_check(self, key: str, retry: int = 3) -> dict:
        """
        Check FS item existence.
        """
        check_key = f"{key}?check"

        try:
            content = self.fs_get(check_key, path=None, overwrite=False, retry=retry)
            if not content:
                return {"suc": False, "data": "no response"}

            return orjson.loads(content)

        except Exception:
            return {"suc": False, "data": "Remote server down"}

    # -------------------------------
    # Database
    # -------------------------------

    def db_set(self, table: str, data: Any, retry: int = 5) -> dict:
        """
        Insert or update remote DB data.
        """
        url = f"{self.host}/db/{table}"
        payload = orjson.dumps(data)

        for _ in range(retry):
            try:
                r = requests.post(
                    url,
                    data=payload,
                    headers={
                        **self.headers,
                        "Content-Type": "application/json",
                    },
                    timeout=self.timeout,
                )
                return orjson.loads(r.content)
            except Exception as e:
                print(f"db_set failed: {e}, {r.content}")
                print(url)
                print(payload)
                print({
                        **self.headers,
                        "Content-Type": "application/json",
                    })
                time.sleep(1)

        return {"suc": False, "data": "Remote server down"}

    def db_get(
        self,
        table: str,
        where: str,
        page: int = 1,
        limit: int = 20
    ) -> dict:
        """
        Query remote DB.
        """
        url = f"{self.host}/db/{table}/{where}?page={page}&limit={limit}"

        for _ in range(5):
            try:
                r = requests.get(url, headers=self.headers, timeout=self.timeout)
                return orjson.loads(r.content)
            except Exception:
                time.sleep(1)

        return {"suc": False, "data": "Remote server down"}

    def db_iter(self, table: str, where: str) -> Generator[dict, None, None]:
        """
        Iterate through paged DB results.
        """
        first = self.db_get(table, where, 1, 20)
        if not first.get("suc"):
            return

        for item in first.get("data", []):
            yield item

        max_page = first.get("max_page", 1)

        for page in range(2, max_page + 1):
            ret = self.db_get(table, where, page, 20)
            if not ret.get("suc"):
                return
            for item in ret.get("data", []):
                yield item
