import struct
from io import BytesIO
import lz4.block

MAGIC_NUMBER = b"\xE5\xA1\xED\xFE"
DEFAULT_CHUNK_SIZE = 524288

def _decompress_file(f):
    """Decompress a file-like object and return the decompressed data as bytes.
File should be opened in binary read mode."""
    output = b""
    # look for magic number -- indicates more to read
    while f.read(4) == MAGIC_NUMBER:
        packed_size, unpacked_size = struct.unpack("ii", f.read(8))
        # skip 4 bytes
        f.read(4)
        # compressed data starts now
        output += lz4.block.decompress(f.read(packed_size), uncompressed_size=unpacked_size)

    return output

def decompress_file_to_bytes(fp):
    """Decompress a file and return the decompressed file as
a bytes object."""
    with open(fp, "rb") as f:
        return _decompress_file(f)

def decompress_file_to_file(in_fp, out_fp):
    """Decompress a file and write the decompressed file to a new file."""
    with open(in_fp, "rb") as f:
        data = _decompress_file(f)
    with open(out_fp, "wb") as f:
        f.write(data)

def _compress_bytes(b, f, chunk_size=DEFAULT_CHUNK_SIZE, strip_newlines=False):
    """Compress bytes and write it to the provided file-like object.
File should be opened in binary write mode."""
    # take a copy of the input data to avoid clobbering
    data = b[:]

    # strip any newlines added by the editor if needed
    if strip_newlines:
        data.rstrip(b"\r\n")
    # append a null byte if it is not already there
    if data[-1] != 0:
        data += b"\0"
    
    chunk = data[:chunk_size]
    data = data[chunk_size:]
    while chunk:
        compressed = lz4.block.compress(chunk, mode="fast", store_size=False)
        f.write(MAGIC_NUMBER)
        f.write(struct.pack("i", len(compressed)))
        f.write(struct.pack("i", len(chunk)))
        f.write(b"\0\0\0\0")
        f.write(compressed)

        chunk = data[:chunk_size]
        data = data[chunk_size:]

def compress_bytes_to_file(b, fp, chunk_size=DEFAULT_CHUNK_SIZE, strip_newlines=False):
    """Compress a bytes object and write it to a file at the given filepath."""
    with open(fp, "wb") as f:
        _compress_bytes(b, f, chunk_size=chunk_size, strip_newlines=strip_newlines)

def compress_bytes_to_bytes(b, chunk_size=DEFAULT_CHUNK_SIZE, strip_newlines=False):
    """Compress a bytes object and return the compressed version."""
    with BytesIO() as f:
        _compress_bytes(b, f, chunk_size=chunk_size, strip_newlines=strip_newlines)
        return f.getvalue()
