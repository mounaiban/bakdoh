"""
Bakdoh Infohoardi Test Modules: Sample Data Creation Kit

"""
# Copyright 2021 Mounaiban
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import TestCase
from infohoardi import index_dup, RecOffsetIndex

from infohoardi import RecOffsetIndex

def mk_ihstr(seq, roi, **kwargs):
    """
    Make an InfoHoardi text DB string from a sequence, using record
    markers from RecOffsetIndex ``roi``.

    Keyword Arguments
    -----------------
    * first_id: id of the first record

    * ihfmt: InfoHoardi format

      * 0x00: REC_START only

      * 0x02: REC_START and REC_END only

      * 0x03: REC_START, REC_META_END and REC_END

      NOTE: first record is always REC_START, REC_META_END, REC_END

    """
    # TODO: release standard for id's
    DEFAULT_DB_ID = 0xBEEF_FACE

    ihfmt = kwargs.get('ihfmt', 0x03)
    id_zero = kwargs.get('id0', DEFAULT_DB_ID)

    s_chr = roi.rec_start_chr
    m_chr = ''
    e_chr = ''

    if ihfmt & roi.REC_META_END_USED != 0:
        m_chr = roi.rec_meta_end_chr
    if ihfmt & roi.REC_END_USED != 0:
        e_chr = roi.rec_end_chr
    out = f'{s_chr}{id_zero}{roi.rec_meta_end_chr}{seq[0]}{roi.rec_end_chr}'
    i = 0
    for d in seq[1:]:
        rec_id = ''
        if len(m_chr) > 0:
            rec_id = f'{id_zero+i}{m_chr}'
        rec_str = f'{s_chr}{rec_id}{d}{e_chr}'
        out = out.join(('', rec_str))
        i += 1
    return out

