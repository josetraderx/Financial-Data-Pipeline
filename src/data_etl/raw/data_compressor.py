"""
Data compression module for optimizing storage of market data.
"""

import bz2
import lzma
import zlib
from pathlib import Path


class DataCompressor:
    """Handles compression and decompression of market data files."""

    COMPRESSION_METHODS = {
        'zlib': {
            'compress': zlib.compress,
            'decompress': zlib.decompress,
            'extension': '.zlib'
        },
        'lzma': {
            'compress': lzma.compress,
            'decompress': lzma.decompress,
            'extension': '.xz'
        },
        'bz2': {
            'compress': bz2.compress,
            'decompress': bz2.decompress,
            'extension': '.bz2'
        }
    }

    def __init__(self, compression_method: str = 'zlib', compression_level: int = 6):
        """
        Initialize the DataCompressor.

        Args:
            compression_method (str): Compression method ('zlib', 'lzma', or 'bz2')
            compression_level (int): Compression level (0-9, higher = better compression)
        """
        if compression_method not in self.COMPRESSION_METHODS:
            raise ValueError(f"Unsupported compression method: {compression_method}")

        self.method = compression_method
        self.level = compression_level

    def compress_file(
        self,
        file_path: str | Path,
        output_path: str | Path | None = None,
        chunk_size: int = 1024 * 1024  # 1MB chunks
    ) -> dict:
        """
        Compress a file using the specified method.

        Args:
            file_path (Union[str, Path]): Path to file to compress
            output_path (Union[str, Path], optional): Custom output path
            chunk_size (int): Size of chunks to process at once

        Returns:
            Dict: Compression statistics
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Determine output path
        if output_path:
            output_path = Path(output_path)
        else:
            extension = self.COMPRESSION_METHODS[self.method]['extension']
            output_path = file_path.with_suffix(extension)

        # Create output directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        original_size = file_path.stat().st_size
        compressed_size = 0

        with open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
            while chunk := f_in.read(chunk_size):
                compressed_chunk = self.COMPRESSION_METHODS[self.method]['compress'](
                    chunk,
                    level=self.level if self.method == 'zlib' else None
                )
                compressed_size += len(compressed_chunk)
                f_out.write(compressed_chunk)

        compression_ratio = (compressed_size / original_size) * 100
        space_saved = original_size - compressed_size

        stats = {
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': compression_ratio,
            'space_saved': space_saved,
            'method': self.method,
            'level': self.level,
            'input_path': str(file_path),
            'output_path': str(output_path)
        }

        return stats

    def decompress_file(
        self,
        file_path: str | Path,
        output_path: str | Path | None = None,
        chunk_size: int = 1024 * 1024  # 1MB chunks
    ) -> Path:
        """
        Decompress a file.

        Args:
            file_path (Union[str, Path]): Path to compressed file
            output_path (Union[str, Path], optional): Custom output path
            chunk_size (int): Size of chunks to process at once

        Returns:
            Path: Path to decompressed file
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Determine output path
        if output_path:
            output_path = Path(output_path)
        else:
            # Remove compression extension
            output_path = file_path.with_suffix('')

        # Create output directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
            compressed_data = f_in.read()
            decompressed_data = self.COMPRESSION_METHODS[self.method]['decompress'](
                compressed_data
            )
            f_out.write(decompressed_data)

        return output_path

    def compress_directory(
        self,
        dir_path: str | Path,
        output_dir: str | Path | None = None,
        pattern: str = '*.*'
    ) -> list[dict]:
        """
        Compress all matching files in a directory.

        Args:
            dir_path (Union[str, Path]): Directory path
            output_dir (Union[str, Path], optional): Output directory
            pattern (str): File pattern to match

        Returns:
            List[Dict]: List of compression statistics for each file
        """
        dir_path = Path(dir_path)
        if not dir_path.is_dir():
            raise NotADirectoryError(f"Not a directory: {dir_path}")

        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

        stats = []
        for file_path in dir_path.glob(pattern):
            if file_path.is_file():
                if output_dir:
                    output_path = output_dir / file_path.name
                else:
                    output_path = None

                try:
                    result = self.compress_file(file_path, output_path)
                    stats.append(result)
                except Exception as e:
                    print(f"Error compressing {file_path}: {e}")

        return stats

    def decompress_directory(
        self,
        dir_path: str | Path,
        output_dir: str | Path | None = None
    ) -> list[Path]:
        """
        Decompress all compressed files in a directory.

        Args:
            dir_path (Union[str, Path]): Directory with compressed files
            output_dir (Union[str, Path], optional): Output directory

        Returns:
            List[Path]: List of paths to decompressed files
        """
        dir_path = Path(dir_path)
        if not dir_path.is_dir():
            raise NotADirectoryError(f"Not a directory: {dir_path}")

        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

        decompressed_files = []
        extension = self.COMPRESSION_METHODS[self.method]['extension']

        for file_path in dir_path.glob(f'*{extension}'):
            if file_path.is_file():
                if output_dir:
                    output_path = output_dir / file_path.stem
                else:
                    output_path = None

                try:
                    result = self.decompress_file(file_path, output_path)
                    decompressed_files.append(result)
                except Exception as e:
                    print(f"Error decompressing {file_path}: {e}")

        return decompressed_files
