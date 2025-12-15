import requests
from pathlib import Path

UPLOAD_URL = "http://localhost:8001/upload-multiple"


def upload_files(file_paths):
    files = []

    for path in file_paths:
        path = Path(path)
        files.append(
            ("files", (path.name, open(path, "rb")))
        )

    response = requests.post(UPLOAD_URL, files=files)

    if response.status_code != 200:
        raise Exception(response.text)

    return response.json()


if __name__ == "__main__":
    result = upload_files([
        "a.txt",
        "b.csv",
        "c.json"
    ])

    print(result)
