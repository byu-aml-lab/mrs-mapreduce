# Mrs
# Copyright 2008-2012 Brigham Young University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from amlpso.standardpso import StandardPSO
from collections import defaultdict

from .conftest import run_serial, run_mockparallel, run_master_slave


def test_pso(mrs_impl, tmpdir, capfd):
    args = ['-i', '20', '-n', '5', '-d', '2', '--out-freq', '3', '--mrs-seed',
            '42', '--hey-im-testing']

    if mrs_impl == 'serial':
        run_serial(StandardPSO, args)
    elif mrs_impl == 'mockparallel':
        run_mockparallel(StandardPSO, args, tmpdir)
    elif mrs_impl == 'master_slave':
        run_master_slave(StandardPSO, args, tmpdir)
    else:
        raise RuntimeError('Unknown mrs_impl: %s' % mrs_impl)

    out, err = capfd.readouterr()
    assert err == ''

    lines = [line.strip() for line in out.splitlines()
            if line and not line.startswith('#')]
    assert lines == ['202.330512851', '202.330512851', '74.0008930067',
            '32.3155678366', '2.9191713839', '2.9191713839', '2.9191713839']


# vim: et sw=4 sts=4
