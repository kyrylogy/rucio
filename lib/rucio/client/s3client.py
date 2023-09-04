# -*- coding: utf-8 -*-
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import boto3
import os
import logging
from dogpile.cache.api import NO_VALUE

from rucio.common.config import get_s3_credentials
from rucio.common.cache import make_region_memcached
from botocore.exceptions import ClientError
from rucio.client.client import Client


SUCCESS = 0
FAILURE = 1

REGION = make_region_memcached(expiration_time=900)


class S3Client:
    """S3 client class"""

    S3_BASEURL = "s3"

    def __init__(self, _client=None, logger=None, config: dict = None):
        # TODO: convert dict config to S3Credentials object?
        """
        Initialises the basic settings for an S3Client object

        :param _client:     - Optional:  rucio.client.client.Client object. If None, a new object will be created.
        :param logger:      - Optional:  logging.Logger object. If None, default logger will be used.
        :param config:      - Optional:  dict object with S3 credentials. If None, credentials will be loaded from default path.

        """

        if not logger:
            self.logger = logging.log
        else:
            self.logger = logger.log

        self.client = _client if _client else Client()
        account = self.client.account
        cred = REGION.get("s3client-%s" % account)
        if cred is NO_VALUE:
            if config:
                cred = config
            else:
                cred = get_s3_credentials()
            REGION.set("s3client-%s" % account, cred)
        try:
            self.s3 = boto3.client("s3", **cred, verify=False)
        except Exception as error:
            self.logger(logging.ERROR, error)
            raise error

    def bucket_create(self, path):
        """Create an S3 bucket.

        param bucket_name: Bucket path, e.g. user.dquijote:/mybucket/
        :return: True if bucket created, else False
        """
        # TODO: IAM policy structure
        bucket_name, bucket_path = path.split(":")

        try:
            self.s3.head_bucket(Bucket=bucket_name)
        except ClientError as error:
            # TODO: catch 409 BucketAlreadyOwnedByYou and BucketAlreadyExists (bucket can be owned by another user)
            if error.response["Error"]["Code"] == "404":
                self.logger(logging.DEBUG, "Creating bucket %s" % bucket_name)
                self.s3.create_bucket(Bucket=bucket_name)
            elif error.response:
                self.logger(logging.ERROR, error.response["Error"]["Message"])
                return FAILURE

        try:
            self.s3.put_object(Bucket=bucket_name, Body="", Key=bucket_path)
        except ClientError as error:
            self.logger(logging.ERROR, error.response["Error"]["Message"])
            return FAILURE
        return SUCCESS

    def bucket_upload(self, from_path, to_path):
        """Upload a file/folder to an S3 bucket.

        :param from_path: Path to the file/folder to upload
        :param to_path: Bucket path, e.g. user.dquijote:/mybucket/file.ext or user.dquijote:/mybucket/folder/
        :return: True if file/folder uploaded, else False
        """
        bucket_name, bucket_path = to_path.split(":")

        if os.path.isdir(from_path):
            for root, dirs, files in os.walk(from_path):
                for file in files:
                    with open(os.path.join(root, file), "rb") as f:
                        # TODO: keep folder structure when uploading
                        # TODO: add exceptions
                        destination = (
                            bucket_path + file if to_path.endswith("/") else bucket_path
                        )
                        try:
                            self.s3.upload_fileobj(
                                Fileobj=f, Bucket=bucket_name, Key=str(destination)
                            )
                        except ClientError as error:
                            self.logger(logging.ERROR, error)
                            return FAILURE
                        return SUCCESS

        else:
            f = open(from_path, "rb")
            # write file to folder_name/filename or file to filename if bucket_path is a file
            destination = (
                bucket_path + os.path.basename(from_path)
                if bucket_path.endswith("/")
                else bucket_path
            )
            try:
                self.s3.upload_fileobj(
                    Fileobj=f, Bucket=bucket_name, Key=str(destination)
                )
            except ClientError as error:
                self.logger(logging.ERROR, error)
                return FAILURE
            return SUCCESS
