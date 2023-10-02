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

import logging
import os
import pytest

from rucio.client.downloadclient import DownloadClient
from rucio.client.s3client import S3Client
from rucio.core.did import add_did, delete_dids
from rucio.db.sqla.constants import DIDType
from rucio.tests.common import load_test_conf_file, did_name_generator
from tests.conftest import mock_scope, root_account


@pytest.fixture
def s3_client():
    logger = logging.getLogger("s3_client")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    config = load_test_conf_file("s3client.cfg.template")
    return S3Client(logger=logger, config=config)


@pytest.fixture
def download_client():
    return DownloadClient()


def test_create_bucket(s3_client):
    """S3CLIENT: Create a bucket"""
    # TODO: add more scopes for validation
    scope = "user.dquijote:/folder/subfolder/"
    status = s3_client.bucket_create(scope)
    assert status == 0


def test_upload_bucket(s3_client, file_factory):
    """S3CLIENT: Upload a bucket"""
    # TODO: download the file using DownloadClient
    scope = "user.dquijote:/folder/"
    local_file = file_factory.file_generator()
    fn = os.path.basename(local_file)
    s3_client.bucket_upload(from_path=local_file, to_path=scope)
