import requests
import zipfile
import io

url = "https://www.gtt.to.it/open_data/gtt_gtfs.zip"
print("Downloading...")
response = requests.get(url)
print("Status:", response.status_code)
print("Size:", len(response.content) / 1024 / 1024, "MB")

z = zipfile.ZipFile(io.BytesIO(response.content))
print("\nFiles inside:")
for name in z.namelist():
    info = z.getinfo(name)
    print(f"  {name}  ({info.file_size / 1024:.1f} KB)")

# Preview routes.txt
print("\n--- routes.txt (first 5 lines) ---")
with z.open('routes.txt') as f:
    for i, line in enumerate(f):
        if i >= 5:
            break
        print(line.decode('utf-8', errors='replace').strip())

# Preview stops.txt
print("\n--- stops.txt (first 5 lines) ---")
with z.open('stops.txt') as f:
    for i, line in enumerate(f):
        if i >= 5:
            break
        print(line.decode('utf-8', errors='replace').strip())