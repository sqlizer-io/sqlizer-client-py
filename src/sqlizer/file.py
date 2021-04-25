import requests
from datetime import datetime
from time import sleep

from . import config
from .conversionstatus import ConversionStatus
from .propertynames import PropertyNames
from .apiurls import ApiUrls


class File:
    SQLIZER_MULTIPART_UPLOAD_MAX_CHUNK_SIZE = 10000000

    def __init__(self, file, database_type, file_type, file_name, table_name, file_has_headers=True,
                 delimiter=',', sheet_name=None, cell_range=None, check_table_exists=True, insert_spacing=250):
        self._file = file
        self._database_type = database_type
        self._file_type = file_type
        self._file_name = file_name
        self._table_name = table_name
        self._file_has_headers = file_has_headers
        self._delimiter = delimiter
        self._sheet_name = sheet_name
        self._cell_range = cell_range
        self._check_table_exists = check_table_exists
        self._insert_spacing = insert_spacing
        self._id = None
        self._status = ConversionStatus.NotCreated
        self._percent_complete = None
        self._result_url = None
        self._result_rows = None
        self._message = None

    @property
    def database_type(self):
        return self._database_type

    @property
    def file_type(self):
        return self._file_type

    @property
    def file_name(self):
        return self._file_name

    @property
    def table_name(self):
        return self._table_name

    @property
    def file_has_headers(self):
        return self._file_has_headers

    @property
    def delimiter(self):
        return self._delimiter

    @property
    def sheet_name(self):
        return self._sheet_name

    @property
    def cell_range(self):
        return self._cell_range

    @property
    def check_table_exists(self):
        return self._check_table_exists

    @property
    def insert_spacing(self):
        return self._insert_spacing

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        return self._status

    @property
    def percent_complete(self):
        return self._percent_complete

    @property
    def result_url(self):
        return self._result_url

    @property
    def result_rows(self):
        return self._result_rows

    @property
    def message(self):
        return self._message

    def _get_headers(self):
        if config.API_KEY is not None:
            return {'Authorization': 'Bearer %s' % config.API_KEY}

    def _get_post_data(self):
        return {
            PropertyNames.FileType: self.file_type,
            PropertyNames.FileName: self.file_name,
            PropertyNames.TableName: self.table_name,
            PropertyNames.DatabaseType: self.database_type,
            PropertyNames.FileHasHeaders: self.file_has_headers,
            PropertyNames.Delimiter: self.delimiter,
            PropertyNames.SheetName: self.sheet_name,
            PropertyNames.CellRange: self.cell_range,
            PropertyNames.CheckTableExists: self.check_table_exists,
            PropertyNames.InsertSpacing: self.insert_spacing
        }

    def _create(self):
        req = requests.post(
            ApiUrls.Create(), headers=self._get_headers(), data=self._get_post_data())
        req.raise_for_status()
        self._set_data(req.json())

    def _upload_data_part(self, file_content, part_number):
        req = requests.post(ApiUrls.UploadData(self.id, part_number), headers=self._get_headers(), files={
            'file': file_content
        })
        req.raise_for_status()

    def _upload_data(self):
        buffer = self._file.read(self.SQLIZER_MULTIPART_UPLOAD_MAX_CHUNK_SIZE)
        part_number = 1
        while len(buffer) > 0:
            self._upload_data_part(buffer, part_number)
            buffer = self._file.read(
                self.SQLIZER_MULTIPART_UPLOAD_MAX_CHUNK_SIZE)
            part_number += 1

    def _update_status(self, status):
        req = requests.put(ApiUrls.GetOrUpdate(self.id), headers=self._get_headers(), data={
            PropertyNames.Status: status
        })
        req.raise_for_status()

    def _get_data(self):
        req = requests.get(ApiUrls.GetOrUpdate(self.id),
                           headers=self._get_headers())
        req.raise_for_status()
        return req.json()

    def _set_data(self, response_data):
        self._id = response_data[PropertyNames.ID]
        self._status = response_data[PropertyNames.Status]
        if PropertyNames.PercentComplete in response_data:
            self._percent_complete = response_data[PropertyNames.PercentComplete]
        if PropertyNames.ResultUrl in response_data:
            self._result_url = response_data[PropertyNames.ResultUrl]
        if PropertyNames.ResultRows in response_data:
            self._result_rows = response_data[PropertyNames.ResultRows]
        if PropertyNames.Message in response_data:
            self._message = response_data[PropertyNames.Message]

    def refresh(self):
        self._set_data(self._get_data())

    def has_finished(self):
        return self.status == ConversionStatus.Complete \
            or self.status == ConversionStatus.Failed \
            or self.status == ConversionStatus.SubscriptionRequired \
            or self.status == ConversionStatus.PaymentRequired

    def wait_for_completion(self, timeout_seconds=0):
        start_time = datetime.now().timestamp()
        check_interval = 0.5
        while timeout_seconds == 0 or datetime.now().timestamp() < start_time + timeout_seconds:
            self.refresh()
            if self.has_finished():
                break
            sleep(check_interval)
            check_interval *= 1.01  # Gradually back off the frequency of checks

    def convert(self, wait=True):
        self._create()
        self._upload_data()
        self._update_status(ConversionStatus.Uploaded)
        if wait:
            self.wait_for_completion()

    def download_result_file(self):
        req = requests.get(self.result_url)
        req.raise_for_status()
        return req
