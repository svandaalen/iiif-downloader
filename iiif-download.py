import argparse
import os
import time
import requests
from tqdm import tqdm


def download_image(url, folder, filename):
    max_retries = 10
    timeout = 120
    base_wait = 60
    for attempt in range(1, max_retries + 1):
        try:
            file_path = os.path.join(folder, filename)
            if os.path.exists(file_path):
                print(f"Skipping '{filename}' because it already exists.")
                return
            else:
                print(f"Downloading {filename} from {url}")
                response = requests.get(url, stream=True, timeout=timeout)
                response.raise_for_status()
                total = int(response.headers.get("content-length", 0))

                with open(file_path, "wb") as out_file, tqdm(
                    desc=filename,
                    total=total,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as progress_bar:
                    for chunk in response.iter_content(1024):
                        size = out_file.write(chunk)
                        progress_bar.update(size)
                print(f"Downloaded {filename}")
                break
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            if attempt < max_retries:
                wait_time = base_wait * (2 ** (attempt - 1))
                print(
                    f"Error occurred: {str(e)}. Attempt {attempt} of {max_retries}. "
                    f"Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)
            else:
                print(f"Max retries reached. Last error: {str(e)}")
                raise Exception(
                    f"Failed to download {filename} after {max_retries} attempts: {str(e)}"
                ) from e


def scrape_images_from_iiif_manifest(manifest_url, download_folder="iiif_images"):
    # Fetch the manifest
    response = requests.get(manifest_url, timeout=120)
    if response.status_code != 200:
        print("Failed to fetch manifest")
        return

    manifest = response.json()

    # Create download folder if it doesn't exist
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    if "items" in manifest:
        print("IIIF v3")
        print(f"Downloading {len(manifest['items'])} images")
        for item in manifest["items"]:
            label = item.get("label", {}).get("none", [""])[0]
            filename = f"{label}.jpg"
            for canvas in item["items"]:
                for annotation_page in canvas["items"]:
                    body = annotation_page["body"]
                    if isinstance(body, list):
                        body = body[0]  # Assuming the first item if it's a list
                    image_url = body["id"]
                    if image_url:
                        download_image(image_url, download_folder, filename)
                    else:
                        print("No image URL found in annotation")

    elif "sequences" in manifest:
        print("IIIF v2")
        index = 1
        for sequence in manifest["sequences"]:
            print(f"Downloading {len(sequence['canvases'])} images")
            for canvas in sequence["canvases"]:
                filename = (
                    f"{canvas['label']}.jpg" if "label" in canvas else f"{index}.jpg"
                )
                index += 1
                for image in canvas["images"]:
                    resource = image["resource"]
                    if "vatlib" in resource["@id"]:
                        service = resource["service"]
                        image_url = service["@id"] + "/full/full/0/default.jpg"
                        download_image(image_url, download_folder, filename)
                    else:
                        image_url = resource["@id"]
                        if not "default" in image_url:
                            new_image_url = (
                                image_url + "/full/full/0/default/default.jpg"
                            )
                            download_image(new_image_url, download_folder, filename)
                        if "default" in image_url:
                            download_image(image_url, download_folder, filename)
                        else:
                            print("No image URL found")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download images from a IIIF manifest")
    parser.add_argument(
        "-m", "--manifest", type=str, required=True, help="URL of the IIIF manifest"
    )
    parser.add_argument(
        "-o", "--output", type=str, required=False, help="Output dir images"
    )
    args = parser.parse_args()

    iiif_manifest_url = args.manifest
    output_folder = args.output
    scrape_images_from_iiif_manifest(iiif_manifest_url, output_folder)
