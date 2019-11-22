import gzip
import json
import logging
import os
import time

import boa
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator, Variable
from google_sheets_plugin.hooks.google_hook import GoogleHook

from six import BytesIO


class GoogleSheetsToS3Operator(BaseOperator):
    """
    Google Sheets To S3 Operator

    :param google_conn_id:    The Google connection id. Please see the
                              GoogleHook for more information about this.
    :type google_conn_id:     string
    :param sheet_id:          The id for associated report. This is in the URL
                              of the sheet.
    :type sheet_id:           string
    :param s3_conn_id:        The s3 connection id. This is defined in airflow
                              connections in the UI.
    :type s3_conn_id:         string
    :param s3_path:           The S3 file path for where the data should go.
                              Cannot be NULL.
    :type s3_path:            string
    :param include_schema:    If set to true, infer the schema of the data and
                              output to S3 as a separate file
    :type include_schema:     boolean
    :param sheet_names:       The names for the relevent sheets in the report.
    :type sheet_names:        string/array
    :param range:             The range of of cells containing the relevant data.
                              This must be the same for all sheets if multiple
                              are being pulled together.
                              Example: Sheet1!A2:E80
    :type range:              string
    :param s3_bucket:         The s3 bucket this file should be dropped into.
    :type s3_bucket:          string
    """

    template_fields = ('s3_path',)

    def __init__(self,
                 google_conn_id,
                 sheet_id,
                 s3_conn_id,
                 s3_path,
                 s3_bucket,
                 compression_bound=10000,
                 include_schema=False,
                 sheet_names=[],
                 range=None,
                 output_format='json',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.google_conn_id = google_conn_id
        self.sheet_id = sheet_id
        self.sheet_names = sheet_names
        self.s3_conn_id = s3_conn_id
        self.s3_path = s3_path.strip('/')
        self.include_schema = include_schema
        self.range = range
        self.output_format = output_format.lower()
        self.compression_bound = compression_bound
        self.s3_bucket = s3_bucket
        if self.output_format not in ('json'):
            raise Exception('Acceptable output formats are: json.')

        if self.sheet_names and not isinstance(self.sheet_names, (str, list, tuple)):
            raise Exception('Please specify the sheet names as a string or list.')

    def execute(self, context):
        g_conn = GoogleHook(self.google_conn_id)

        if isinstance(self.sheet_names, str) and ',' in self.sheet_names:
            sheet_names = self.sheet_names.split(',')
        else:
            sheet_names = self.sheet_names

        sheets_object = g_conn.get_service_object('sheets', 'v4',
                                                    ['https://spreadsheets.google.com/feeds',
                                                     'https://www.googleapis.com/auth/drive'])
        print('Retrieved Sheets Object')

        response = sheets_object.spreadsheets().get(spreadsheetId=self.sheet_id,
                                                    includeGridData=True).execute()

        title = response.get('properties').get('title')
        sheets = response.get('sheets')

        final_output = dict()

        total_sheets = []
        for sheet in sheets:
            name = sheet.get('properties').get('title')

            total_sheets.append(name)

            if self.sheet_names:
                if name not in sheet_names:
                    print('{} is not found in available sheet names.'.format(name))
                    continue

            table_name = name
            data = sheet.get('data')[0].get('rowData')
            output = []

            for row in data:
                row_data = []
                values = row.get('values')
                for value in values:
                    ev = value.get('effectiveValue')
                    if ev is None:
                        row_data.append(None)
                    else:
                        for v in ev.values():
                            row_data.append(v)

                output.append(row_data)

            if self.output_format == 'json':
                headers = output.pop(0)
                output = [dict(zip(headers, row)) for row in output]

            final_output[table_name] = output

        s3 = S3Hook(self.s3_conn_id)

        for sheet in final_output:
            output_data = final_output.get(sheet)

            file_name = os.path.splitext(self.s3_path)[0]

            sheet = boa.constrict(sheet)

            output_name = ''.join([self.s3_path, '/', sheet,
                                   '.', self.output_format])

            if self.include_schema is True:
                schema_name = ''.join([self.s3_path, '/', sheet,
                                      '_schema', '.', self.output_format])
            else:
                schema_name = None

            self.output_manager(s3, output_name, output_data, context,
                                sheet, schema_name)

        dag_id = context['ti'].dag_id

        var_key = '_'.join([dag_id, self.sheet_id])
        Variable.set(key=var_key, value=json.dumps(total_sheets))
        time.sleep(10)

        return boa.constrict(title)

    def output_manager(self, s3, output_name, output_data, context,
                        sheet_name, schema_name):
        if self.output_format == 'json':
            output = '\n'.join([json.dumps({str(k): v
                                            for k, v in record.items()})
                                for record in output_data])

            enc_output = str.encode(output, 'utf-8')

            # if file is more than bound then apply gzip compression
            if len(enc_output) / 1024 / 1024 >= self.compression_bound:
                print("File is more than {}MB, gzip compression will be applied".format(self.compression_bound))
                output = gzip.compress(enc_output, compresslevel=5)
                self.xcom_push(context, key='is_compressed_{}'.format(sheet_name), value="compressed")
                self.load_bytes(s3,
                                bytes_data=output,
                                key=output_name,
                                bucket_name=self.s3_bucket,
                                replace=True
                                )
            else:
                print("File is less than {}MB, compression will not be applied".format(self.compression_bound))
                self.xcom_push(context, key='is_compressed_{}'.format(sheet_name), value="non-compressed")
                s3.load_string(
                    string_data=output,
                    key=output_name,
                    bucket_name=self.s3_bucket,
                    replace=True
                )

            if self.include_schema is True:
                output_keys = output_data[0].keys()
                schema = [{'name': boa.constrict(a),
                           'type': 'varchar(512)'} for a in output_keys if a is not None]
                schema = {'columns': schema}

                s3.load_string(
                    string_data=json.dumps(schema),
                    key=schema_name,
                    bucket_name=self.s3_bucket,
                    replace=True
                )

            print('Successful output of "{}" to S3.'.format(output_name))

        # TODO -- Add support for csv output

        # elif self.output_format == 'csv':
        #     with NamedTemporaryFile("w") as f:
        #         writer = csv.writer(f)
        #         writer.writerows(output_data)
        #         s3.load_file(
        #             filename=f.name,
        #             key=output_name,
        #             bucket_name=self.s3_bucket,
        #             replace=True
        #         )
        #
        #     if self.include_schema is True:
        #         pass

    # TODO: remove when airflow version is upgraded to 1.10
    def load_bytes(self, s3,
                   bytes_data,
                   key,
                   bucket_name=None,
                   replace=False):
        if not bucket_name:
            (bucket_name, key) = s3.parse_s3_url(key)

        if not replace and s3.check_for_key(key, bucket_name):
            raise ValueError("The key {key} already exists.".format(key=key))

        filelike_buffer = BytesIO(bytes_data)

        client = s3.get_conn()
        client.upload_fileobj(filelike_buffer, bucket_name, key, ExtraArgs={})
