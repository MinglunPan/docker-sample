"""
Advanced Parquet IOManager with Data Quality Checks
===================================================

This module provides a comprehensive IOManager for storing DataFrames as Parquet files
with automatic data quality checks, metadata generation, and preview functionality.
"""

import os
import json
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    IOManager,
    ConfigurableIOManager,
    AssetMaterialization,
    MetadataValue,
    OutputContext,
    InputContext,
    get_dagster_logger,
    TableSchema,
    TableColumn,
    TableRecord,
)


class DataQualityChecker:
    """Utility class for performing data quality checks on DataFrames"""
    
    @staticmethod
    def check_missing_values(df: pd.DataFrame) -> Dict[str, Any]:
        """Check for missing values in the DataFrame"""
        missing_count = df.isnull().sum()
        missing_percent = (missing_count / len(df) * 100).round(2)
        
        return {
            "total_missing": int(missing_count.sum()),
            "missing_by_column": {
                col: {
                    "count": int(count),
                    "percentage": float(percent)
                }
                for col, count, percent in zip(missing_count.index, missing_count.values, missing_percent.values)
                if count > 0
            },
            "columns_with_missing": len([col for col in missing_count.index if missing_count[col] > 0]),
            "missing_data_ratio": float((missing_count.sum() / (len(df) * len(df.columns)) * 100).round(2))
        }
    
    @staticmethod
    def check_duplicates(df: pd.DataFrame) -> Dict[str, Any]:
        """Check for duplicate rows in the DataFrame"""
        duplicate_count = df.duplicated().sum()
        return {
            "duplicate_rows": int(duplicate_count),
            "duplicate_percentage": float((duplicate_count / len(df) * 100).round(2)),
            "unique_rows": int(len(df) - duplicate_count)
        }
    
    @staticmethod
    def get_column_statistics(df: pd.DataFrame) -> Dict[str, Any]:
        """Get detailed statistics for each column"""
        stats = {}
        
        for col in df.columns:
            col_stats = {
                "dtype": str(df[col].dtype),
                "non_null_count": int(df[col].count()),
                "null_count": int(df[col].isnull().sum()),
                "unique_count": int(df[col].nunique()),
            }
            
            # Add numeric statistics for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_stats.update({
                    "mean": float(df[col].mean()) if not df[col].isna().all() else None,
                    "std": float(df[col].std()) if not df[col].isna().all() else None,
                    "min": float(df[col].min()) if not df[col].isna().all() else None,
                    "max": float(df[col].max()) if not df[col].isna().all() else None,
                    "median": float(df[col].median()) if not df[col].isna().all() else None,
                })
            
            # Add string statistics for object columns
            elif df[col].dtype == 'object':
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    col_stats.update({
                        "avg_length": float(non_null_values.astype(str).str.len().mean()),
                        "max_length": int(non_null_values.astype(str).str.len().max()),
                        "min_length": int(non_null_values.astype(str).str.len().min()),
                    })
            
            stats[col] = col_stats
        
        return stats
    
    @staticmethod
    def generate_data_preview(df: pd.DataFrame, n_rows: int = 5) -> Dict[str, Any]:
        """Generate a preview of the data"""
        return {
            "head": df.head(n_rows).to_dict('records'),
            "tail": df.tail(n_rows).to_dict('records'),
            "sample": df.sample(min(n_rows, len(df))).to_dict('records') if len(df) > 0 else [],
        }
    
    @staticmethod
    def check_data_consistency(df: pd.DataFrame) -> Dict[str, Any]:
        """Check for data consistency issues"""
        consistency_checks = {}
        
        # Check for columns with all null values
        all_null_columns = [col for col in df.columns if df[col].isnull().all()]
        consistency_checks["all_null_columns"] = all_null_columns
        
        # Check for columns with single unique value
        single_value_columns = [col for col in df.columns if df[col].nunique() <= 1]
        consistency_checks["single_value_columns"] = single_value_columns
        
        # Check for potential date columns that might need parsing
        potential_date_columns = []
        for col in df.columns:
            if df[col].dtype == 'object':
                sample_values = df[col].dropna().head(10).astype(str)
                if len(sample_values) > 0:
                    # Simple heuristic for date-like strings
                    date_like_count = sum(1 for val in sample_values 
                                        if any(sep in val for sep in ['-', '/', '.']) 
                                        and any(char.isdigit() for char in val))
                    if date_like_count / len(sample_values) > 0.5:
                        potential_date_columns.append(col)
        
        consistency_checks["potential_date_columns"] = potential_date_columns
        
        return consistency_checks


class ParquetIOManagerWithQuality(ConfigurableIOManager):
    """
    IOManager that stores DataFrames as Parquet files with data quality checks.
    """
    
    base_path: str = "data"
    enable_preview: bool = True
    preview_rows: int = 5
    enable_quality_checks: bool = True
    max_preview_size_mb: float = 10.0
    
    def __post_init__(self):
        # Ensure base path exists
        Path(self.base_path).mkdir(parents=True, exist_ok=True)
    
    @property
    def logger(self):
        return get_dagster_logger()
    
    def _get_file_path(self, context: Union[OutputContext, InputContext]) -> Path:
        """Generate the file path for storing the asset"""
        # Create path based on asset key
        asset_key_path = "/".join(context.asset_key.path)
        file_path = Path(self.base_path) / asset_key_path / "data.parquet"
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        return file_path
    
    def _get_metadata_path(self, context: Union[OutputContext, InputContext]) -> Path:
        """Generate the metadata file path"""
        file_path = self._get_file_path(context)
        return file_path.parent / "metadata.json"
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of the file for data integrity"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def _generate_comprehensive_metadata(self, df: pd.DataFrame, file_path: Path) -> Dict[str, Any]:
        """Generate comprehensive metadata about the DataFrame and file"""
        
        # Basic DataFrame info
        basic_info = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "memory_usage_mb": float(df.memory_usage(deep=True).sum() / 1024 / 1024),
            "shape": df.shape,
        }
        
        # File info
        file_stats = file_path.stat()
        file_info = {
            "file_path": str(file_path),
            "file_size_mb": float(file_stats.st_size / 1024 / 1024),
            "file_hash": self._calculate_file_hash(file_path),
            "created_at": datetime.fromtimestamp(file_stats.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
        }
        
        metadata = {
            "basic_info": basic_info,
            "file_info": file_info,
            "generated_at": datetime.now().isoformat(),
        }
        
        # Add data quality checks if enabled
        if self.enable_quality_checks:
            quality_checker = DataQualityChecker()
            
            self.logger.info("Running data quality checks...")
            metadata["data_quality"] = {
                "missing_values": quality_checker.check_missing_values(df),
                "duplicates": quality_checker.check_duplicates(df),
                "column_statistics": quality_checker.get_column_statistics(df),
                "consistency_checks": quality_checker.check_data_consistency(df),
            }
        
        # Always generate preview in metadata (for JSON storage), but limit for UI display
        if self.enable_preview:
            self.logger.info("Generating data preview...")
            metadata["preview"] = DataQualityChecker.generate_data_preview(df, self.preview_rows)
        
        return metadata
    
    def _save_metadata(self, metadata: Dict[str, Any], metadata_path: Path):
        """Save metadata to JSON file"""
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
    
    def _load_metadata(self, metadata_path: Path) -> Optional[Dict[str, Any]]:
        """Load metadata from JSON file"""
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to load metadata: {e}")
        return None
    
    def _create_dagster_metadata(self, metadata: Dict[str, Any], obj: pd.DataFrame) -> Dict[str, MetadataValue]:
        """Convert metadata to Dagster MetadataValue format"""
        dagster_metadata = {}
        
        # Create table schema
        schema = TableSchema(
            columns=[TableColumn(name=c, type=str(obj[c].dtype)) for c in obj.columns]
        )
        
        # Basic info with special Dagster keys
        basic_info = metadata.get("basic_info", {})
        dagster_metadata.update({
            "dagster/row_count": MetadataValue.int(basic_info.get("row_count", 0)),
            "dagster/column_schema": schema,
            "column_count": MetadataValue.int(basic_info.get("column_count", 0)),
            "memory_usage_mb": MetadataValue.float(basic_info.get("memory_usage_mb", 0.0)),
        })
        
        # Add sample data as table - always show preview regardless of file size
        if self.enable_preview and len(obj) > 0:
            sample_rows = min(self.preview_rows, len(obj))
            # Convert DataFrame rows to TableRecord objects
            sample_data = obj.head(sample_rows)
            records = []
            for _, row in sample_data.iterrows():
                # Convert row to dict and handle NaN values
                record_dict = {}
                for col in sample_data.columns:
                    value = row[col]
                    # Convert NaN/None to string representation
                    if pd.isna(value):
                        record_dict[col] = "null"
                    else:
                        # Truncate long strings to avoid memory issues
                        str_value = str(value)
                        if len(str_value) > 100:
                            record_dict[col] = str_value[:97] + "..."
                        else:
                            record_dict[col] = str_value
                records.append(TableRecord(record_dict))
            
            dagster_metadata["sample"] = MetadataValue.table(
                records=records,
                schema=schema,
            )
        
        # File info
        file_info = metadata.get("file_info", {})
        dagster_metadata.update({
            "file_path": MetadataValue.path(file_info.get("file_path", "")),
            "file_size_mb": MetadataValue.float(file_info.get("file_size_mb", 0.0)),
            "file_hash": MetadataValue.text(file_info.get("file_hash", "")),
        })
        
        # Data quality summary
        if "data_quality" in metadata:
            dq = metadata["data_quality"]
            
            # Missing values summary
            if "missing_values" in dq:
                mv = dq["missing_values"]
                dagster_metadata.update({
                    "total_missing_values": MetadataValue.int(mv.get("total_missing", 0)),
                    "missing_data_ratio": MetadataValue.float(mv.get("missing_data_ratio", 0.0)),
                    "columns_with_missing": MetadataValue.int(mv.get("columns_with_missing", 0)),
                })
            
            # Duplicates summary
            if "duplicates" in dq:
                dup = dq["duplicates"]
                dagster_metadata.update({
                    "duplicate_rows": MetadataValue.int(dup.get("duplicate_rows", 0)),
                    "duplicate_percentage": MetadataValue.float(dup.get("duplicate_percentage", 0.0)),
                })
            
            # Data quality score (simple heuristic)
            quality_score = 100.0
            if "missing_values" in dq:
                quality_score -= min(dq["missing_values"].get("missing_data_ratio", 0) * 2, 30)
            if "duplicates" in dq:
                quality_score -= min(dq["duplicates"].get("duplicate_percentage", 0) * 1.5, 20)
            
            dagster_metadata["data_quality_score"] = MetadataValue.float(max(0, round(quality_score, 2)))
        
        return dagster_metadata
    
    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        """Store DataFrame as Parquet file with quality checks and metadata"""
        
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"Expected pandas DataFrame, got {type(obj)}")
        
        file_path = self._get_file_path(context)
        metadata_path = self._get_metadata_path(context)
        
        self.logger.info(f"Saving DataFrame to {file_path}")
        
        # Save DataFrame as Parquet
        try:
            obj.to_parquet(file_path, index=False, engine='pyarrow')
            
            self.logger.info(f"Successfully saved {len(obj)} rows to {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save DataFrame: {e}")
            raise
        
        # Generate and save comprehensive metadata
        try:
            self.logger.info("Generating metadata...")
            comprehensive_metadata = self._generate_comprehensive_metadata(obj, file_path)
            self._save_metadata(comprehensive_metadata, metadata_path)
            
            # Convert to Dagster metadata format
            dagster_metadata = self._create_dagster_metadata(comprehensive_metadata, obj)
            
            # Log materialization with metadata
            context.add_output_metadata(dagster_metadata)
            
            self.logger.info(f"Metadata saved to {metadata_path}")
            
        except Exception as e:
            self.logger.warning(f"Failed to generate metadata: {e}")
    
    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load DataFrame from Parquet file"""
        
        file_path = self._get_file_path(context)
        metadata_path = self._get_metadata_path(context)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")
        
        self.logger.info(f"Loading DataFrame from {file_path}")
        
        try:
            df = pd.read_parquet(file_path, engine='pyarrow')
            
            self.logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
            
            # Load and log metadata if available
            metadata = self._load_metadata(metadata_path)
            if metadata:
                self.logger.info("Metadata loaded successfully")
                
                # Log some key metrics
                if "data_quality" in metadata:
                    dq = metadata["data_quality"]
                    if "missing_values" in dq:
                        self.logger.info(f"Data quality - Missing values: {dq['missing_values'].get('total_missing', 0)}")
                    if "duplicates" in dq:
                        self.logger.info(f"Data quality - Duplicate rows: {dq['duplicates'].get('duplicate_rows', 0)}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load DataFrame: {e}")
            raise


# Factory function for easy instantiation
def create_parquet_io_manager(**kwargs) -> ParquetIOManagerWithQuality:
    """Factory function to create a ParquetIOManagerWithQuality instance"""
    return ParquetIOManagerWithQuality(**kwargs)