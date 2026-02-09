"""
Chunked CSV reader con gestione errori completa e memory-safe.

Questo modulo fornisce una classe per leggere file CSV di grandi dimensioni
in modo incrementale, mantenendo l'uso della RAM sotto controllo.

Memory Safety:
- Legge il file in chunks di dimensione configurabile (default 500K righe)
- Ogni chunk viene processato e rilasciato prima di caricare il successivo
- Gestisce encoding errors e righe malformate senza crashare
- Non mantiene in memoria più di un chunk alla volta

Author: Scenario Generation System
Date: 2025-02-02
"""

import pandas as pd
from pathlib import Path
import logging
from typing import Optional, Iterator, List

# Setup logging
logger = logging.getLogger(__name__)


class ChunkedReader:
    """
    Reader incrementale per file CSV di grandi dimensioni.
    
    Legge il file in chunks per mantenere l'uso della RAM sotto controllo.
    Ogni chunk viene processato e rilasciato prima di caricare il successivo.
    
    Args:
        filepath: Path al file CSV da leggere
        chunksize: Numero di righe per chunk (default: 500000)
        usecols: Lista di colonne da leggere (None = tutte)
        encoding: Encoding del file (default: 'utf-8', fallback: 'latin-1')
        delimiter: Delimitatore CSV (default: ',')
        skip_bad_lines: Se True, salta righe malformate invece di crashare
    
    Memory Safety:
        - Peak RAM usage: ~chunksize * bytes_per_row
        - Con chunksize=500K e ~100 bytes/row: ~50MB per chunk
        - Total peak: ~50MB (solo un chunk in memoria alla volta)
    
    Example:
        >>> reader = ChunkedReader('data/dataset.csv', chunksize=100000)
        >>> for chunk in reader:
        ...     process(chunk)
    """
    
    def __init__(
        self,
        filepath: str | Path,
        chunksize: int = 500000,
        usecols: Optional[List[str]] = None,
        encoding: str = 'utf-8',
        delimiter: str = ',',
        skip_bad_lines: bool = True
    ):
        self.filepath = Path(filepath)
        self.chunksize = chunksize
        self.usecols = usecols
        self.encoding = encoding
        self.delimiter = delimiter
        self.skip_bad_lines = skip_bad_lines
        
        # Validate file exists
        if not self.filepath.exists():
            raise FileNotFoundError(f"File not found: {self.filepath}")
        
        # Validate chunksize
        if chunksize <= 0:
            raise ValueError(f"chunksize must be positive, got {chunksize}")
        
        logger.info(f"Initialized ChunkedReader for {self.filepath}")
        logger.info(f"Chunk size: {chunksize:,} rows")
        logger.info(f"Expected peak RAM: ~{chunksize * 100 / 1024 / 1024:.1f} MB")
    
    def __iter__(self) -> Iterator[pd.DataFrame]:
        """
        Iterator che yielda chunks del CSV.
        
        Yields:
            pd.DataFrame: Chunk del dataset con le colonne specificate
            
        Raises:
            ValueError: Se il file è vuoto o non contiene colonne valide
            UnicodeDecodeError: Se l'encoding fallisce e non ci sono fallback
        """
        # Try primary encoding first
        encodings_to_try = [self.encoding]
        if self.encoding != 'latin-1':
            encodings_to_try.append('latin-1')
        if self.encoding != 'utf-8':
            encodings_to_try.append('utf-8')
        
        last_error = None
        for encoding in encodings_to_try:
            try:
                yield from self._read_with_encoding(encoding)
                return  # Success, exit
            except UnicodeDecodeError as e:
                last_error = e
                logger.warning(f"Encoding {encoding} failed, trying next...")
                continue
            except Exception as e:
                # Other errors (not encoding-related) should be raised immediately
                logger.error(f"Error reading {self.filepath} with encoding {encoding}: {e}")
                raise
        
        # If we get here, all encodings failed
        if last_error:
            raise last_error
        else:
            raise ValueError(
                f"Failed to read {self.filepath} with any encoding. "
                f"Tried: {', '.join(encodings_to_try)}"
            )
    
    def _read_with_encoding(self, encoding: str) -> Iterator[pd.DataFrame]:
        """
        Legge il file con un encoding specifico.
        
        Args:
            encoding: Encoding da usare
            
        Yields:
            pd.DataFrame: Chunks del dataset
        """
        # Configure pandas read_csv parameters
        read_params = {
            'filepath_or_buffer': self.filepath,
            'chunksize': self.chunksize,
            'encoding': encoding,
            'delimiter': self.delimiter,
            'low_memory': False,  # Better type inference
        }
        
        # Handle header: if usecols is specified but file has no header, use names
        # This is needed for files like tianchi_2014002_rec_tmall_log_parta.txt
        if self.usecols is not None:
            # Try to read first line to check if header exists
            try:
                with open(self.filepath, 'r', encoding=encoding, errors='ignore') as test_file:
                    first_line = test_file.readline()
                    if first_line:
                        # Split by delimiter and check first column
                        first_col = first_line.split(self.delimiter)[0].strip()
                        # Check if first column looks like data (starts with digit) or header
                        # Also check if it matches one of the expected column names
                        is_data = first_col.isdigit() or (len(first_col) > 0 and first_col[0].isdigit())
                        is_header = first_col in self.usecols or first_col.lower() in [c.lower() for c in self.usecols]
                        
                        if is_data and not is_header:
                            # No header: specify column names
                            read_params['names'] = self.usecols
                            read_params['header'] = None
                            logger.debug(f"Detected no header, using names: {self.usecols}")
                        else:
                            # Has header: use usecols normally
                            read_params['usecols'] = self.usecols
                            logger.debug(f"Detected header, using usecols: {self.usecols}")
                    else:
                        # Empty file, assume no header
                        read_params['names'] = self.usecols
                        read_params['header'] = None
            except Exception as e:
                # Fallback: assume no header if we can't check
                logger.warning(f"Could not detect header, assuming no header: {e}")
                read_params['names'] = self.usecols
                read_params['header'] = None
        
        # Handle bad lines
        if self.skip_bad_lines:
            # pandas 2.0+ uses on_bad_lines
            # Check if pandas supports on_bad_lines by inspecting signature
            import inspect
            sig = inspect.signature(pd.read_csv)
            if 'on_bad_lines' in sig.parameters:
                read_params['on_bad_lines'] = 'skip'
            else:
                # pandas < 2.0 uses error_bad_lines
                read_params['error_bad_lines'] = False
                read_params['warn_bad_lines'] = False
        
        # Create chunk iterator
        try:
            chunk_iterator = pd.read_csv(**read_params)
        except pd.errors.EmptyDataError:
            raise ValueError(f"File {self.filepath} is empty")
        except pd.errors.ParserError as e:
            raise ValueError(f"Failed to parse {self.filepath}: {e}")
        
        # Yield chunks with error handling
        chunk_count = 0
        total_rows = 0
        
        for chunk in chunk_iterator:
            chunk_count += 1
            
            # Validate chunk is not empty
            if len(chunk) == 0:
                logger.warning(f"Empty chunk #{chunk_count} encountered, skipping")
                continue
            
            # Drop rows with missing required columns
            if self.usecols is not None:
                chunk = chunk.dropna(subset=self.usecols)
            else:
                # Drop rows where all columns are NaN
                chunk = chunk.dropna(how='all')
            
            # Log progress every 10 chunks
            if chunk_count % 10 == 0:
                total_rows += len(chunk)
                logger.debug(f"Processed {chunk_count} chunks, ~{total_rows:,} rows so far")
            
            yield chunk
        
        logger.info(f"Completed reading {chunk_count} chunks from {self.filepath}")
    
    def get_total_rows(self) -> int:
        """
        Conta il numero totale di righe nel file (escluso header).
        
        Returns:
            int: Numero totale di righe
            
        Note:
            Questo metodo legge il file due volte (una per contare, una per processare).
            Usa solo se necessario per progress tracking accurato.
        """
        count = 0
        encodings_to_try = [self.encoding, 'latin-1', 'utf-8']
        
        for encoding in encodings_to_try:
            try:
                with open(self.filepath, 'r', encoding=encoding, errors='ignore') as f:
                    # Skip header
                    try:
                        next(f)
                    except StopIteration:
                        return 0
                    
                    # Count lines
                    for _ in f:
                        count += 1
                
                logger.info(f"Total rows in {self.filepath}: {count:,}")
                return count
            except UnicodeDecodeError:
                continue
        
        # Fallback: try without encoding specification
        try:
            with open(self.filepath, 'r', errors='ignore') as f:
                try:
                    next(f)
                except StopIteration:
                    return 0
                count = sum(1 for _ in f)
            logger.info(f"Total rows in {self.filepath}: {count:,} (fallback count)")
            return count
        except Exception as e:
            logger.error(f"Failed to count rows in {self.filepath}: {e}")
            return 0
