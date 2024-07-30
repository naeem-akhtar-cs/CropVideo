import requests, json, subprocess, os, math, random, uuid, io
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

AIRTABLE_MODEL_TABLE_ID = os.getenv("AIRTABLE_MODEL_TABLE_ID")
AIRTABLE_SHORT_FORMAT_TABLE_ID = os.getenv("AIRTABLE_SHORT_FORMAT_TABLE_ID")
AIRTABLE_LONG_FORMAT_TABLE_ID = os.getenv("AIRTABLE_LONG_FORMAT_TABLE_ID")

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

    params = {}
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


def getModelFiles(folderId):
    SERVICE_ACCOUNT_FILE = "creds.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build("drive", "v3", credentials=credentials)

    results = []
    pageToken = None

    while True:
        try:
            response = service.files().list(
                q=f"'{folderId}' in parents",
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType)',
                pageToken=pageToken
            ).execute()

            results.extend(response.get('files', []))
            pageToken = response.get('nextPageToken', None)

            if pageToken is None:
                break

        except Exception as e:
            print(f"An error occurred: {e}")
            break

    return results


def uploadToDrive(filePath, fileName):
    SERVICE_ACCOUNT_FILE = "creds.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build("drive", "v3", credentials=credentials)

    media = MediaFileUpload(filePath, resumable=True)

    fileMetadata = {"name": fileName, "parents": [AIRTABLE_SHORT_FORMAT_TABLE_ID]}

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


@celery.task()
def processModelVideos(record, processedVideos):
    recordId = record["id"]
    recordFields = record["fields"]

    driveFolderId = record["fields"]["drive folder LongFormat"]
    splitLength = float(SPLIT_VIDEO_LENGTH)

    modelFiles = getModelFiles(driveFolderId)

    for file in modelFiles:
        if file["mimeType"] != "video/quicktime":
            continue
        fileId = file["id"]
        fileDownloadLink = f"https://drive.usercontent.google.com/download?id={fileId}&export=download"

        # print(file)

        # makeFilePublic(fileId)
        # fileName = downloadVideo(processedVideos, fileId, file["name"])
        # removePermissions(fileId)

        # splittedVideos = splitVideo(processedVideos, fileName, splitLength)
        # os.remove(f"{processedVideos}/{fileName}")
        # print(json.dumps(splittedVideos))

        splittedVideos = ["1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_000.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_001.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_002.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_003.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_004.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_005.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_006.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_007.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_008.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_009.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_010.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_011.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_012.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_013.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_014.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_015.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_016.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_017.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_018.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_019.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_020.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_021.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_022.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_023.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_024.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_025.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_026.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_027.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_028.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_029.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_030.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_031.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_032.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_033.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_034.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_035.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_036.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_037.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_038.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_039.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_040.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_041.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_042.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_043.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_044.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_045.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_046.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_047.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_048.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_049.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_050.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_051.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_052.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_053.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_054.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_055.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_056.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_057.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_058.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_059.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_060.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_061.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_062.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_063.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_064.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_065.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_066.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_067.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_068.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_069.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_070.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_071.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_072.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_073.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_074.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_075.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_076.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_077.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_078.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_079.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_080.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_081.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_082.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_083.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_084.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_085.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_086.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_087.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_088.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_089.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_090.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_091.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_092.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_093.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_094.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_095.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_096.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_097.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_098.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_099.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_100.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_101.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_102.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_103.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_104.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_105.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_106.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_107.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_108.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_109.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_110.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_111.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_112.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_113.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_114.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_115.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_116.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_117.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_118.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_119.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_120.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_121.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_122.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_123.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_124.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_125.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_126.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_127.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_128.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_129.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_130.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_131.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_132.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_133.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_134.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_135.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_136.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_137.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_138.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_139.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_140.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_141.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_142.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_143.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_144.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_145.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_146.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_147.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_148.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_149.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_150.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_151.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_152.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_153.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_154.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_155.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_156.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_157.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_158.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_159.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_160.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_161.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_162.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_163.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_164.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_165.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_166.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_167.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_168.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_169.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_170.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_171.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_172.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_173.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_174.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_175.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_176.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_177.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_178.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_179.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_180.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_181.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_182.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_183.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_184.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_185.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_186.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_187.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_188.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_189.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_190.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_191.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_192.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_193.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_194.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_195.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_196.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_197.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_198.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_199.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_200.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_201.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_202.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_203.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_204.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_205.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_206.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_207.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_208.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_209.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_210.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_211.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_212.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_213.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_214.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_215.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_216.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_217.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_218.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_219.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_220.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_221.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_222.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_223.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_224.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_225.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_226.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_227.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_228.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_229.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_230.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_231.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_232.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_233.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_234.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_235.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_236.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_237.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_238.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_239.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_240.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_241.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_242.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_243.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_244.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_245.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_246.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_247.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_248.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_249.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_250.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_251.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_252.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_253.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_254.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_255.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_256.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_257.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_258.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_259.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_260.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_261.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_262.MOV", "1oTWY7jnvhFCqUkZpl_c8QVjC3FUbMiRT_263.MOV"]

        for video in splittedVideos:
            uploadToDrive(video)


@app.route('/splitVideos')
def splitVideos():
    processedVideos = "VideosDir"

    checkDir(processedVideos)
    removeFiles(processedVideos)

    modelTableId = f"{AIRTABLE_MODEL_TABLE_ID}"

    offset = None
    firstRequest = True
    while offset is not None or firstRequest:
        data = getAirtableRecords(offset, modelTableId)
        records = data["records"]
        offset = data["offset"]

        if records:
            for record in records:
                if record["fields"].get("drive folder LongFormat") is not None:
                    # processModelVideos.delay(record, processedVideos)
                    processModelVideos(record, processedVideos)
        firstRequest = False

    return jsonify({"status": 200, "message": "Processing started!!"})


@app.route('/<path:path>')
def defaultRoute(path):
    return make_response(jsonify({"status": 404, "message": "Invalid route"}), 404)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    print(f"App running at port 8080")
