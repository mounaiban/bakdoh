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

class RecId:
    LEN_RUNTIME_STAT = 8
    LEN_RUNTIME_XSTAT = 16
    LEN_LOCAL = 64
    LEN_GLOBAL = 256
    # PREFIX_RUNTIME_STAT = 0x0
    PREFIX_RUNTIME_XSTAT = 0b01
    PREFIX_LOCAL = 0b10
    PREFIX_GLOBAL = 0b11

    def __init__(self, id_bits, id_val, **kwargs):
        # TODO: document kwarg: 'db_id'
        self.value = None
        handlers = {
            self.LEN_RUNTIME_STAT: self._do_new_runtime_stat_id,
            self.LEN_RUNTIME_XSTAT: self._do_new_runtime_xstat_id,
            self.LEN_LOCAL: self._do_new_local_id,
            self.LEN_GLOBAL: self._do_new_global_id,
        }
        if not isinstance(id_bits, int) or not isinstance(id_val, int):
            details = {
                'msg': 'id_bits and id_val must be int',
                'id_bits_type': type(id_bits),
                'id_val_type': type(id_val)
            }
            raise TypeError(details)
        if id_bits not in handlers:
            details = {
                'msg': 'invalid value for id_bits',
                'id_bits_supported_values': tuple(handlers.keys()),
            }
            raise ValueError(details)
        fn = handlers[id_bits]
        self.value = fn(id_val, **kwargs)

    def __repr__(self):
        return f'{type(self)}({self.value})'

    @classmethod
    def from_int(self):
        pass

    def _do_ck_neg_id(self, id_val, id_db=0):
        if id_val < 0 or id_db < 0:
            details = {
                'msg': 'id_val, id_db must be 0 or larger'
            }
            raise ValueError(details)

    def _do_new_runtime_stat_id(self, id_val, **kwargs):
        maxv = 2**(self.LEN_RUNTIME_STAT-2) - 1
        if id_val > maxv:
            details = {
                'msg': f'runtime stat RecId exceeds max value',
                'id_max': maxv,
            }
            raise ValueError(details)
        self._do_ck_neg_id(id_val)
        return id_val

    def _do_new_runtime_xstat_id(self, id_val, **kwargs):
        maxv = 2**(self.LEN_RUNTIME_XSTAT-2) - 1
        if id_val > maxv:
            details = {
                'msg': f'runtime xstat RecId exceeds max value',
                'max_id': maxv,
            }
            raise ValueError(details)
        self._do_ck_neg_id(id_val)
        return (self.PREFIX_RUNTIME_XSTAT << 14) | id_val

    def _do_new_local_id(self, id_val, **kwargs):
        maxv = 2**44 - 1
        maxv_db = 2**18 - 1
        id_db = kwargs.get('id_db', 0)
        if id_val > maxv:
            details = {
                'msg': f'local RecId exceeds max value',
                'max_id': maxv,
            }
            raise ValueError(details)
        if id_db > maxv_db:
            details = {
                'msg': f'local RecId db_id exceeds max value',
                'max_id_db': maxv_db,
            }
            raise ValueError(details)
        self._do_ck_neg_id(id_val, id_db)
        return (self.PREFIX_LOCAL << 62) | (id_db << 44) | id_val

    def _do_new_global_id(self, id_val, **kwargs):
        maxv = 2**126 - 1
        maxv_db = 2**128 - 1
        id_db = kwargs.get('id_db', 0xfc00 << 112)
        if id_val > maxv:
            details = {
                'msg': f'global RecId exceeds max value',
                'max_id': maxv,
            }
            raise ValueError(details)
        if id_db > maxv_db:
            details = {
                'msg': f'global RecId db_id exceeds max value',
                'max_id_db': maxv_db,
            }
            raise ValueError(details)
        self._do_ck_neg_id(id_val, id_db)
        return (self.PREFIX_GLOBAL << 254) | (id_db << 126) | id_val


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
    # TODO: confirm standard for id's
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

