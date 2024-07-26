import requests, json, subprocess, os, math, random, uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from flask import Flask, request, jsonify, send_file, after_this_request
from celery import Celery
from celery.result import AsyncResult

app = Flask(__name__)

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_LONG_FORMAT_TABLE_ID = os.getenv("AIRTABLE_LONG_FORMAT_TABLE_ID")
AIRTABLE_SHORT_FORMAT_TABLE_ID = os.getenv("AIRTABLE_SHORT_FORMAT_TABLE_ID")

GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

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

    params = {"view": AIRTABLE_VIEW_ID, 'filterByFormula': '{Video Processed} = False()'}
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


def downloadVideo(videoUrl, folderName, recordId):
    fileName = f"{recordId}"
    response = requests.get(url=videoUrl, stream=True)
    with open(f"{folderName}/{fileName}.mp4", "wb") as writer:
        for chunk in response.iter_content(chunk_size=8192):
            writer.write(chunk)
    print(f"Video: {fileName}.mp4 downloaded")
    return fileName


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


def uploadToDrive(filePath, fileName):
    SERVICE_ACCOUNT_FILE = "creds.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build("drive", "v3", credentials=credentials)

    media = MediaFileUpload(filePath, resumable=True)

    fileMetadata = {"name": fileName, "parents": [GOOGLE_DRIVE_FOLDER_ID]}

    file = service.files().create(body=fileMetadata, media_body=media, fields="id").execute()

    fileUrl = driveDownloadBaseUrl + file.get("id")
    return fileUrl


@celery.task()
def processVideoTask(record, processedVideos, processingSpecs):
    recordId = record["id"]
    recordFields = record["fields"]
    fileName = downloadVideo(recordFields["Google Drive URL"], processedVideos, recordId)

    variantsList = []
    for specs in processingSpecs:
        processVideo(processedVideos, fileName, specs)

        randomNumber = random.randint(1000, 9999)
        fileUrl = uploadToDrive(f"{processedVideos}/{fileName}_{specs['VariantId']}.mov", f"IMG_{randomNumber}.MOV")

        variant = {
            "variantId": specs["VariantId"],
            "fileUrl": fileUrl,
            "fileName": f"IMG_{randomNumber}.MOV",
            "randomNumber": randomNumber
        }
        variantsList.append(variant)

    newRecordData = {
        "recordId": recordId,
        "tiktokUrl": record["fields"]["Video URL"],
        "soundUrl": record["fields"]["short sound url"],
        "variantsList": variantsList
    }

    addDataToAirTable(newRecordData)
    status = updateRecordStatus({"recordId": recordId})
    if not status:
        print(f"Could not update status in linked table for record: {recordId}")

    for variant in variantsList:
        filePath = f"{processedVideos}/{fileName}_{variant['variantId']}.mov"
        removeFile(filePath)



@app.route('/cropVideos')
def cropVideos():
    processedVideos = "ProcessedVideos"

    checkDir(processedVideos)
    removeFiles(processedVideos)

    longFormatTableId = f"{AIRTABLE_LONG_FORMAT_TABLE_ID}"

    offset = None
    firstRequest = True
    while offset is not None or firstRequest:
        data = getAirtableRecords(offset, longFormatTableId)
        records = data["records"]
        offset = data["offset"]

        if records:
            for record in records:
                processVideoTask.delay(record, processedVideos, processingSpecs)
        firstRequest = False

    return jsonify({"status": 200, "message": "Processing started!!"})


@app.route('/*')
def cropVideos():
    return jsonify({"status": 404, "message": "Invalid route"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    print(f"App running at port 8080")
