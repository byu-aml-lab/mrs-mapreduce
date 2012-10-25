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

"""Mrs: MapReduce - a Simple implementation

Your Mrs MapReduce program might look something like this:

import mrs

class Mrs_Program(mrs.MapReduce):
    def map(key, value):
        yield newkey, newvalue

    def reduce(key, values):
        yield newvalue

if __name__ == '__main__':
    mrs.main(Mrs_Program)
"""

# Set up the default logging configuration.
import logging, sys
logger = logging.getLogger('mrs')
logger.setLevel(logging.WARNING)
handler = logging.StreamHandler(sys.stderr)
format = '%(asctime)s: %(levelname)s: %(message)s'
formatter = logging.Formatter(format)
handler.setFormatter(formatter)
logger.addHandler(handler)

# rather than importing all submodules, we just import the ones that are
# expected to be useful outside of Mrs internals.
from . import registry
from . import version
from .fileformats import HexWriter, TextWriter, BinWriter, ZipWriter
from .main import main
from .mapreduce import MapReduce, IterativeMR
from .serializers import (Serializer, output_serializers, raw_serializer,
        str_serializer, int_serializer, make_struct_serializer,
        make_primitive_serializer, make_protobuf_serializer)

__version__ = version.__version__

# We need to set __all__ to make sure that pydoc has everything:
__all__ = ['MapReduce', 'main', 'logger', 'BinWriter', 'HexWriter',
    'TextWriter', 'Serializer', 'output_serializers', 'raw_serializer',
    'str_serializer', 'int_serializer', 'make_struct_serializer',
    'make_primitive_serializer', 'make_protobuf_serializer']

# vim: et sw=4 sts=4
