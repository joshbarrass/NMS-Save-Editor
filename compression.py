import struct
import lz4.block

MAGIC_NUMBER = b"\xE5\xA1\xED\xFE"
DEFAULT_CHUNK_SIZE = 524288

def decompress_file_to_bytes(fp):
    """Decompress a file and return the decompressed file as
a bytes object."""
    with open(fp, "rb") as f:
        output = b""
        # look for magic number -- indicates more to read
        while f.read(4) == MAGIC_NUMBER:
            packed_size, unpacked_size = struct.unpack("ii", f.read(8))
            # skip 4 bytes
            f.read(4)
            # compressed data starts now
            output += lz4.block.decompress(f.read(packed_size), uncompressed_size=unpacked_size)

    return output

def compress_bytes_to_file(b, fp, chunk_size=DEFAULT_CHUNK_SIZE, strip_newlines=False):
    """Compress a bytes object and write it to a file."""
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
    with open(fp, "wb") as f:
        while chunk:
            compressed = lz4.block.compress(chunk, mode="fast", store_size=False)
            f.write(MAGIC_NUMBER)
            f.write(struct.pack("i", len(compressed)))
            f.write(struct.pack("i", len(chunk)))
            f.write(b"\0\0\0\0")
            f.write(compressed)

            chunk = data[:chunk_size]
            data = data[chunk_size:]
