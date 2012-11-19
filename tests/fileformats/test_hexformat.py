from mrs.fileformats import HexReader, HexWriter
from mrs.serializers import raw_serializer, Serializers

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO


def test_raw_roundtrip():
    kv_pairs = [(b'key 1', b'value 1'),
            (b'hello', b'world'),
            (b'the', b'end')]
    # Each key-value pair adds a space and a newline.
    expected_size = sum(2 + 2 * len(k) + 2 * len(v) for k, v in kv_pairs)

    serializers = Serializers(raw_serializer, '', raw_serializer, '')

    f = BytesIO()
    writer = HexWriter(f, serializers=serializers)
    for pair in kv_pairs:
        writer.writepair(pair)
    writer.finish()

    size = f.tell()
    assert size == expected_size

    f.seek(0)

    reader = HexReader(f, serializers=serializers)
    new_pairs = list(reader)

    assert new_pairs == kv_pairs


def test_pickle_roundtrip():
    kv_pairs = [(b'key 1', b'value 1'),
            (b'hello', b'world'),
            (b'the', b'end')]
    # Each key-value pair adds a space and a newline.
    # Pickling a bytes object requires 7 bytes plus the length.
    expected_size = sum(2 + 2 * (7 + len(k)) + 2 * (7 + len(v))
            for k, v in kv_pairs)

    f = BytesIO()
    writer = HexWriter(f)
    for pair in kv_pairs:
        writer.writepair(pair)
    writer.finish()

    size = f.tell()
    assert size == expected_size

    f.seek(0)

    reader = HexReader(f)
    new_pairs = list(reader)

    assert new_pairs == kv_pairs

# vim: et sw=4 sts=4
