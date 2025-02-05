import requests, json, subprocess, os, math, random, uuid, io
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload

from flask import Flask, request, jsonify, send_file, after_this_request, make_response
from celery import Celery
from celery.result import AsyncResult

app = Flask(__name__)

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_SHORT_FORMAT_TABLE_ID = os.getenv("AIRTABLE_SHORT_FORMAT_TABLE_ID")
AIRTABLE_LONG_FORMAT_TABLE_ID = os.getenv("AIRTABLE_LONG_FORMAT_TABLE_ID")
AIRTABLE_LONG_FORMAT_VIEW_ID = os.getenv("AIRTABLE_LONG_FORMAT_VIEW_ID")

GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
SPLIT_VIDEO_LENGTH = os.getenv("SPLIT_VIDEO_LENGTH")

baseUrl = "https://api.airtable.com/v0"
driveDownloadBaseUrl = "https://drive.google.com/uc?export=download&id="

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['result_backend'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)
    return celery

app.config.update(
    CELERY_BROKER_URL='redis://redis:6379/0',
    result_backend='redis://redis:6379/0'
)

celery = make_celery(app)

def getAirtableRecords(offset, tableId):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{tableId}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    params = {"view": AIRTABLE_LONG_FORMAT_VIEW_ID, 'filterByFormula': '{Processed} = False()'}
    if offset is not None:
        params["offset"] = offset

    print(f"Records Offset: {offset}")

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429: # Request rate limit case
            time.sleep(30)
            response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        airtableData = {
            "records": data.get("records", []),
            "offset": data.get("offset"),
        }
        return airtableData

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response content: {response.text}")
        return []

    except requests.exceptions.RequestException as e:
        print(f"Request Exception: {e}")
        return []


def downloadVideo(processedVideos, fileId, fileName):
    try:
        fileExtension = fileName.split(".")[-1]
        fileName = f"{fileId}.{fileExtension}"
        filePath = f"{processedVideos}/{fileName}"

        SERVICE_ACCOUNT_FILE = "creds.json"
        SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
        
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build("drive", "v3", credentials=credentials)

        request = service.files().get_media(fileId=fileId)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {filePath}: {int(status.progress() * 100)}%.")
        
        with open(filePath, 'wb') as f:
            f.write(fh.getvalue())
        return fileName
    except Exception as e:
        print(f"An error occurred while downloading {filePath}: {e}")


def checkDir(folderName):
    currentDirectory = os.getcwd()
    folderPath = os.path.join(currentDirectory, folderName)
    if not os.path.exists(folderPath):
        os.makedirs(folderPath)
        print(f"Folder '{folderPath}' created.")


def removeFiles(folderName):
    currentDirectory = os.getcwd()
    folderPath = os.path.join(currentDirectory, folderName)
    for fileName in os.listdir(folderPath):
        filePath = os.path.join(folderPath, fileName)
        try:
            if os.path.isfile(filePath) or os.path.islink(filePath):
                os.unlink(filePath)
            elif os.path.isdir(filePath):
                shutil.rmtree(filePath)
        except Exception as e:
            print(f"Failed to delete {filePath}. Reason: {e}")


def removeFile(filePath):
    try:
        if os.path.isfile(filePath) or os.path.islink(filePath):
            os.unlink(filePath)
            print(f"File: {filePath} removed")
    except Exception as e:
        print(f"Failed to delete {filePath}. Reason: {e}")


def uploadToDrive(filePath, fileName, shortFormatFolder):
    SERVICE_ACCOUNT_FILE = "creds.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build("drive", "v3", credentials=credentials)
    media = MediaFileUpload(filePath, resumable=True)
    fileMetadata = {"name": fileName, "parents": [shortFormatFolder]}
    file = service.files().create(body=fileMetadata, media_body=media, fields="id").execute()
    fileUrl = driveDownloadBaseUrl + file.get("id")
    return fileUrl


def splitVideo(folderName, fileName, splitLength):
    filePath = f"{folderName}/{fileName}"
    fileExtension = fileName.split(".")[-1]
    fileName = fileName.split(".")[0]

    ffprobeCommand = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", filePath
    ]
    output = subprocess.check_output(ffprobeCommand).decode("utf-8").strip()
    duration = float(output)

    numSegments = math.ceil(duration / splitLength)
    splittedVideos = []
    for i in range(numSegments):
        startTime = i * splitLength

        splittedFileName = f"{fileName}_{i:03d}.{fileExtension}"
        splittedVideos.append(splittedFileName)
        outputFile = f"{folderName}/{splittedFileName}"
        
        ffmpegCommand = [
            "ffmpeg", "-i", filePath, "-ss", str(startTime),
            "-t", str(splitLength), "-c", "copy", outputFile
        ]
        subprocess.run(ffmpegCommand, check=True)
    return splittedVideos


def updateRecordStatus(recordId):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_LONG_FORMAT_TABLE_ID}/{recordId}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", 'Content-Type': 'application/json',}

    payload = json.dumps({
        "fields": {
            "Processed": True
        }
    })

    try:
        response = requests.request("PATCH", url, headers=headers, data=payload)
        if response.status_code == 429: # Request rate limit case
            time.sleep(30)
            response = requests.request("PATCH", url, headers=headers, data=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response content: {response.text}")
        return False


def addDataToAirTable(newRecord):
    url = f"{baseUrl}/{AIRTABLE_BASE_ID}/{AIRTABLE_SHORT_FORMAT_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    records = [{ "fields": newRecord}]
    payload = json.dumps({"records": records})

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 429: # Request rate limit case
            time.sleep(30)
            response = requests.request("POST", url, headers=headers, data=payload)
        response.raise_for_status()

        data = response.json()

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response content: {response.text}")
        return []

    except requests.exceptions.RequestException as e:
        print(f"Request Exception: {e}")
        return []


@celery.task()
def processLongVideos(record, processedVideos):
    recordId = record["id"]
    recordFields = record["fields"]

    driveVideoUrl = record["fields"]["Google Drive URL"]

    parsedUrl = urlparse(driveVideoUrl)
    queryParams = parse_qs(parsedUrl.query)
    fileId = queryParams.get('id', [None])[0]
    if fileId is None:
        print(f"No id found in {driveVideoUrl}")
        return

    shortFormatFolder = record["fields"]["drive folder ShortFormat"][0]
    fileName = record["fields"]["Name"]
    splitLength = float(SPLIT_VIDEO_LENGTH)

    downloadedFileName = downloadVideo(processedVideos, fileId, fileName)

    splittedVideos = splitVideo(processedVideos, downloadedFileName, splitLength)
    os.remove(f"{processedVideos}/{downloadedFileName}")
    fileNamePrefix = fileName.split(".")[0]

    for video in splittedVideos:
        filePath = f"{processedVideos}/{video}"
        fileIndex = video.split(".")[0].split("_")[-1]
        fileExtension = video.split(".")[-1]
        fileName = f"{fileNamePrefix}_{fileIndex}.{fileExtension}"
        fileUrl = uploadToDrive(filePath, fileName, shortFormatFolder)

        shortFormatRecord = {
            "Name": fileName,
            "Google Drive URL": fileUrl,
            "LongFormat": [recordId],
        }
        addDataToAirTable(shortFormatRecord)
        removeFile(filePath)
    updateRecordStatus(recordId)


@app.route('/splitVideos')
def splitVideos():
    processedVideos = "SplitVideos"

    checkDir(processedVideos)
    longFormatTableId = f"{AIRTABLE_LONG_FORMAT_TABLE_ID}"

    offset = None
    firstRequest = True
    while offset is not None or firstRequest:
        data = getAirtableRecords(offset, longFormatTableId) # Getting data of long format videos
        records = data["records"]
        offset = data["offset"]

        if records:
            for record in records:
                if record["fields"].get("drive folder LongFormat") is not None:
                    # processLongVideos.delay(record, processedVideos)
                    processLongVideos(record, processedVideos)
        firstRequest = False

    return jsonify({"status": 200, "message": "Processing started!!"})


@app.route('/<path:path>')
def defaultRoute(path):
    return make_response(jsonify({"status": 404, "message": "Invalid route"}), 404)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    print(f"App running at port 8080")
