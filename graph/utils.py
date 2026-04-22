import asyncio
import json
import os

import aiofiles
import httpx
import yaml
from tqdm import tqdm
import csv

from qbrain.utils.serialize_complex import deserialize_complex
from qbrain.utils.file.aread_json import aread_content


class Utils:

    def __init__(self, info=None):
        #self.bucket = GBucket("bestbrain")
        self.info = info
        #self.local_dest_base = LOCAL_DEST_BASE
        #self.bucket_dest_base = BUCKET_DEST_BASE



    def getr(self, attrs, key, s=False):
        v = attrs[key]
        if s is True:
            deserialize_complex(v)
        return v



    async def get_file_metadata_async(self, url: str, just_size=True) -> dict or None:
        """Retrieves file metadata asynchronously using a HEAD request."""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.head(url, follow_redirects=True)
                response.raise_for_status()
                h_info = response.headers
                if response and h_info and just_size:
                    if 'content-length' in h_info:
                        size = h_info['content-length']
                        print(f"File Size: {size} bytes")
                        return size
                return dict(response.headers)
            except httpx.RequestError as e:
                print(f"Error: {e}")
                return None

    async def load_content(self, path, layer, local, single=False, save_to=None):
        """
        Set save url
        check exists
        fetch
        prep
        save
        return
        """
        if local:
            path = local

        print("Fetch content from", path)
        if not os.path.exists(path):
            content = json.loads(self.bucket.download_blob(path))
            content = self.structure_content_save(content, layer=layer, save_to=save_to, single=single)
        else:
            # Path exists, -> handle file types
            print("Load file local")
            if path.endswith("tsv"):
                content = []
                with open(path, 'r', newline='', encoding='utf-8') as tsvfile:
                    reader = csv.DictReader(tsvfile, delimiter='\t')  # Important to define the delimeter.
                    for row in reader:
                        content.append(row)
            elif path.endswith("json"):
                content = await aread_content(save_to)

            with open(path, "rb") as f:
                content = f.read()
        print("Content load successful")
        return content

    def structure_content_save(self, content, layer, single, save_to):
        if isinstance(content, dict):
            if layer.lower() == "gene":
                content = content["genes"]
            elif layer.lower() == "protein":
                content = content["results"]
        else:
            print("first_item", content[0])

        if single:
            content = content[0]
        print("content set")

        dir_path = os.path.dirname(save_to)
        if not os.path.exists(dir_path) or os.path.isfile(dir_path):
            os.makedirs(dir_path, exist_ok=True)

        print("save_to", save_to)
        with open(save_to, "w") as f:
            json.dump(content, f)
        print("content saved")
        return content



    async def aget(self, url, j: str or bool = True):
        #print("get content from ", url)

        size = None
        if j:
            headers = {
                "Content-Type": "application/json"
            }
        i = 0
        async with httpx.AsyncClient() as client:
            while i !=3:
                try:
                    if j:
                        r = await client.get(url, headers=headers, timeout=999.0)
                    else:
                        r = await client.get(url, timeout=999.0)

                    if r.is_success:
                        content = r.text
                        if j is True:
                            content = r.json()

                        elif j == "b":
                            content = r.content

                        elif j=="y":
                            content = yaml.safe_load(r.content)

                        else:
                            r = r.content
                            size = len(r)
                        print("Content gathered ")
                        return content, size

                except Exception as e:
                    print(f"Request failed: {e}, retrying...")
                    await asyncio.sleep(1)
                    i += 1
                    continue  # try again


    async def apost(self, url: str, data=dict) -> dict or None:
        headers = {'Content-Type': 'application/json'}
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=data, headers=headers, timeout=20.0)
                if response.is_success:
                    jd = response.json()
                    print(f"JsonResponse: {jd}")
                    return jd
        except Exception as e:
            print(f'Request failed: {e}...')
        return None



    async def req(self, url: str, method: str = "GET", data=None):
        async with httpx.AsyncClient() as client:
            if method.upper() == "GET":
                r = await client.get(url, params=data)
            elif method.upper() == "POST":
                r = await client.post(url, json=data)
            elif method.upper() == "DELETE":
                r = await client.delete(url, params=data)
            else:
                raise ValueError("Unsupported method")

            # return simple info
            return r.status_code, r.json()









    async def apost_gather(self, url: str, data=list) -> dict or None:
        headers = {'Content-Type': 'application/json'}
        try:
            async with httpx.AsyncClient() as client:
                response = await asyncio.gather(
                    *[
                        client.post(
                            url,
                            json=data_item,
                            headers=headers,
                            timeout=20.0
                        )
                        for data_item in data
                    ]
                )
                print(f"Gather successful : {response}")
                return response
        except Exception as e:
            print(f'Request failed: {e}...')
        return None

    async def download_json_content(
            self,
            url,
            j=True,
            save: str or None = None,  # file_name
            save_layer: str or None = None
    ):
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", url) as response:
                # Check if the response is successful
                if response.status_code != 200:
                    print(f"Failed to download JSON: {response.status_code}")
                    return None
                total_size = int(response.headers.get("content-length", 0))
                print(f"File size: {total_size}")
                progress = tqdm(total=total_size, unit="B", unit_scale=True, desc="Downloading JSON")
                print("Downloading JSON", progress)
                # Gather content
                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=1024):
                    content.extend(chunk)
                    progress.update(len(chunk))

                progress.close()

                # Decode JSON content
                try:
                    if j:
                        content = json.loads(content.decode('utf-8'))  # Parse JSON
                        # print("JSON content", json_content)
                        if save:
                            await self.save_process(file_name=save, content=content, layer=save_layer)
                    else:
                        content = content.decode('utf-8')
                    return content
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON: {e}")
                    return None

    async def aread_content(self, path, mode="r", j=True):
        print("READ LOCAL CONTENT FROM", path)
        if not path:
            return None
        async with aiofiles.open(path, mode) as file:
            content = await file.read()
        if j:
            return json.loads(content)
        return content

    async def asave_ckpt_local(self, path, content, mode="w"):

        # Ensure the directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Write JSON content asynchronously
        async with aiofiles.open(path, mode=mode) as json_file:
            await json_file.write(json.dumps(content, indent=2))

        print(f"Checkpoint saved successfully at {path}")


"""

    async def aadd_col(self, keys: Dict, table, type_from_val=True):
        table_schema = await self.afetch_table_schema(table_name=table)
        print("Working schema for table")
        cols_to_insert = {}
        for k, v in keys.items():
            if k not in table_schema:
                if type_from_val is True:
                    v = self.get_spanner_type(v)
                cols_to_insert[k] = v

        all_queries = self.add_col_batch_query(
                        table=table,
                        col_data=cols_to_insert
        )
        if all_queries is None:
            return None
        await self.update_db(all_queries)
        #print("All cols added")
"""