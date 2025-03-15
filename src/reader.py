import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

class Reader:
    CHUNK_SIZE = 5000  # Process 5000 rows at a time
    
    def read_files(self, path, max_workers=4):
        """Read multiple files with performance optimization"""
        path = Path(path)
        if path.is_file():
            return self._read_single_file(path)
        
        # Process multiple files in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for file_path in path.glob("*.csv"):
                futures.append(executor.submit(self._read_single_file, file_path))
            
            results = {}
            for future in futures:
                data = future.result()
                results.update(data)
                
        return results
        
    def _read_single_file(self, file_path):
        """Read a single file in chunks"""
        if file_path.suffix == '.csv':
            chunks = pd.read_csv(file_path, chunksize=self.CHUNK_SIZE)
            return pd.concat(chunks)
        elif file_path.suffix in ['.xlsx', '.xls']:
            return pd.read_excel(file_path, engine='openpyxl')
