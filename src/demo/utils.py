"""
Dagster Helpers for Jupyter Analysis
=====================================

This module provides utility functions to easily access and analyze
Dagster assets from Jupyter notebooks.
"""

import sys
import os
import pandas as pd
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from dagster import DagsterInstance, AssetKey
from dagster._core.events import DagsterEventType

# Add project source to path
sys.path.append('/app/src')
sys.path.append('/app/src/stackadapt-business-analytics')

DEFAULT_DATA_DIR = '/app/data'


class AssetDataReader:
    """Helper class for reading local asset data with metadata validation"""
    
    def __init__(self, data_dir: str = DEFAULT_DATA_DIR):
        """Initialize the asset reader with data directory path"""
        self.data_dir = Path(data_dir)
        self.asset_registry = self._discover_assets()
        self.asset_by_name = self._build_name_registry()
        
    def _discover_assets(self) -> Dict[str, Dict[str, Any]]:
        """Discover all assets and their metadata in the data directory"""
        asset_registry = {}
        
        if not self.data_dir.exists():
            print(f"Data directory {self.data_dir} not found")
            return asset_registry
        
        # Recursively discover assets with key prefixes
        self._discover_assets_recursive(self.data_dir, "", asset_registry)
                        
        return asset_registry
    
    def _discover_assets_recursive(self, current_dir: Path, key_prefix: str, asset_registry: Dict[str, Dict[str, Any]]):
        """Recursively discover assets in nested directories"""
        for item in current_dir.iterdir():
            if item.is_dir():
                data_file = item / "data.parquet"
                metadata_file = item / "metadata.json"
                
                # Check if this directory contains asset data
                if data_file.exists() and metadata_file.exists():
                    try:
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                        
                        # Build full asset path with key prefix
                        full_asset_path = f"{key_prefix}/{item.name}" if key_prefix else item.name
                        
                        asset_registry[full_asset_path] = {
                            'asset_name': item.name,
                            'key_prefix': key_prefix,
                            'full_path': full_asset_path,
                            'data_path': str(data_file),
                            'metadata_path': str(metadata_file),
                            'metadata': metadata
                        }
                    except Exception as e:
                        print(f"Error loading metadata for {full_asset_path}: {e}")
                else:
                    # Continue recursion into subdirectory
                    new_prefix = f"{key_prefix}/{item.name}" if key_prefix else item.name
                    self._discover_assets_recursive(item, new_prefix, asset_registry)
    
    def _build_name_registry(self) -> Dict[str, List[str]]:
        """Build a registry mapping asset names to their full paths (for conflict resolution)"""
        name_registry = {}
        
        for full_path, asset_info in self.asset_registry.items():
            asset_name = asset_info['asset_name']
            
            if asset_name not in name_registry:
                name_registry[asset_name] = []
            name_registry[asset_name].append(full_path)
                
        return name_registry
    
    def list_assets(self, layer: Optional[str] = None) -> List[str]:
        """List all available assets with full key prefix paths, optionally filtered by layer"""
        assets = list(self.asset_registry.keys())
        
        if layer:
            layer = layer.lower()
            # Filter by asset name (not full path) for backward compatibility
            assets = [asset for asset in assets if self.asset_registry[asset]['asset_name'].startswith(layer)]
            
        return sorted(assets)
    
    def get_asset_info(self, asset_identifier: str, key_prefix: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed information about an asset
        
        Args:
            asset_identifier: Can be either asset name or full path (key_prefix/asset_name)
            key_prefix: Optional key prefix to disambiguate assets with same name
        """
        resolved_path = self._resolve_asset_path(asset_identifier, key_prefix)
        
        asset_info = self.asset_registry[resolved_path]
        metadata = asset_info['metadata']
        
        return {
            'asset_name': asset_info['asset_name'],
            'full_path': asset_info['full_path'],
            'key_prefix': asset_info['key_prefix'],
            'data_path': asset_info['data_path'],
            'row_count': metadata.get('basic_info', {}).get('row_count', 'Unknown'),
            'column_count': metadata.get('basic_info', {}).get('column_count', 'Unknown'),
            'columns': metadata.get('basic_info', {}).get('columns', []),
            'dtypes': metadata.get('basic_info', {}).get('dtypes', {}),
            'generated_at': metadata.get('generated_at', 'Unknown'),
            'data_quality': metadata.get('data_quality', {}),
            'file_info': metadata.get('file_info', {})
        }
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA256 hash of a file"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _validate_data(self, data: pd.DataFrame, resolved_path: str) -> Dict[str, Any]:
        """Validate data against metadata"""
        asset_info_data = self.asset_registry[resolved_path]
        metadata = asset_info_data['metadata']
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Validate column count
        expected_columns = metadata.get('basic_info', {}).get('column_count', 'Unknown')
        actual_columns = len(data.columns)
        
        if expected_columns != 'Unknown' and expected_columns != actual_columns:
            validation_result['valid'] = False
            validation_result['errors'].append(
                f"Column count mismatch: expected {expected_columns}, got {actual_columns}"
            )
        
        # Validate schema (dtypes)
        expected_dtypes = metadata.get('basic_info', {}).get('dtypes', {})
        if expected_dtypes:
            for col_name, expected_dtype in expected_dtypes.items():
                if col_name in data.columns:
                    actual_dtype = str(data[col_name].dtype)
                    # Simple dtype comparison (could be made more sophisticated)
                    if expected_dtype == 'object' and actual_dtype not in ['object', 'string']:
                        validation_result['warnings'].append(
                            f"Column {col_name}: expected {expected_dtype}, got {actual_dtype}"
                        )
                    elif expected_dtype == 'bool' and actual_dtype not in ['bool', 'boolean']:
                        validation_result['warnings'].append(
                            f"Column {col_name}: expected {expected_dtype}, got {actual_dtype}"
                        )
                else:
                    validation_result['errors'].append(f"Expected column '{col_name}' not found in data")
        
        # Validate file hash if available in metadata
        
        
        return validation_result
    
    def _resolve_asset_path(self, asset_identifier: str, key_prefix: Optional[str] = None) -> str:
        """Resolve asset identifier to full asset path
        
        Args:
            asset_identifier: Can be asset name or full path
            key_prefix: Optional key prefix to disambiguate assets
            
        Returns:
            Full asset path in registry
        """
        # First, check if it's already a full path in the registry
        if asset_identifier in self.asset_registry:
            return asset_identifier
        
        # Check if it's an asset name that needs resolution
        if asset_identifier in self.asset_by_name:
            matching_paths = self.asset_by_name[asset_identifier]
            
            if len(matching_paths) == 1:
                return matching_paths[0]
            
            elif len(matching_paths) > 1:
                if key_prefix is None:
                    raise ValueError(
                        f"Multiple assets found with name '{asset_identifier}'. "
                        f"Please specify key_prefix. Available paths: {matching_paths}"
                    )
                
                # Find matching path with the specified key prefix
                target_path = f"{key_prefix}/{asset_identifier}" if key_prefix else asset_identifier
                if target_path in matching_paths:
                    return target_path
                else:
                    raise ValueError(
                        f"No asset found with name '{asset_identifier}' and key_prefix '{key_prefix}'. "
                        f"Available paths: {matching_paths}"
                    )
        
        raise ValueError(
            f"Asset '{asset_identifier}' not found. Available assets: {list(self.asset_registry.keys())}"
        )
    
    def read_asset(self, asset_identifier: str, key_prefix: Optional[str] = None, validate: bool = True, show_info: bool = True) -> pd.DataFrame:
        """
        Read asset data with optional validation
        
        Args:
            asset_identifier: Asset name or full path (key_prefix/asset_name)
            key_prefix: Optional key prefix to disambiguate assets with same name
            validate: Whether to validate data against metadata
            show_info: Whether to display asset information
        
        Returns:
            pandas.DataFrame containing the asset data
        """
        resolved_path = self._resolve_asset_path(asset_identifier, key_prefix)
        asset_info_data = self.asset_registry[resolved_path]
        data_path = asset_info_data['data_path']
        
        try:
            # Read the parquet file
            data = pd.read_parquet(data_path)
            
            if show_info:
                asset_info = self.get_asset_info(asset_identifier, key_prefix)
                print(f"📊 Asset: {asset_info['asset_name']}")
                if asset_info['key_prefix']:
                    print(f"🗂️ Full Path: {asset_info['full_path']}")
                print(f"📁 Path: {data_path}")
                print(f"📏 Shape: {data.shape}")
                print(f"📅 Generated: {asset_info['generated_at']}")
                
            if validate:
                validation = self._validate_data(data, resolved_path)
                
                if validation['valid']:
                    print("✅ Data validation passed")
                else:
                    print("❌ Data validation failed:")
                    for error in validation['errors']:
                        print(f"   • {error}")
                
                if validation['warnings']:
                    print("⚠️  Warnings:")
                    for warning in validation['warnings']:
                        print(f"   • {warning}")
                        
            return data
            
        except Exception as e:
            raise Exception(f"Error reading asset '{asset_identifier}': {e}")
    
    def get_asset_summary(self) -> pd.DataFrame:
        """Get a summary of all available assets"""
        summary_data = []
        
        for full_path in self.list_assets():
            asset_info_data = self.asset_registry[full_path]
            asset_name = asset_info_data['asset_name']
            metadata = asset_info_data['metadata']
            
            # Determine layer
            layer = "Unknown"
            if asset_name.startswith('raw_'):
                layer = "Bronze"
            elif asset_name.startswith('silver_'):
                layer = "Silver" 
            elif asset_name.startswith('gold_'):
                layer = "Gold"
                
            summary_data.append({
                'Asset': full_path,  # Show full path with key prefix
                'Layer': layer,
                'Rows': metadata.get('basic_info', {}).get('row_count', 'Unknown'),
                'Columns': metadata.get('basic_info', {}).get('column_count', 'Unknown'),
                'Generated': metadata.get('generated_at', 'Unknown')
            })
            
        return pd.DataFrame(summary_data)



def setup_asset_reader(data_dir=DEFAULT_DATA_DIR) -> AssetDataReader:
    """Quick setup function for AssetDataReader"""
    print("Setting up Asset Data Reader...")
    reader = AssetDataReader(data_dir)
    
    assets_count = len(reader.asset_registry)
    if assets_count > 0:
        print(f"✅ Asset reader ready!")
        print(f"📁 Data directory: {reader.data_dir}")
        print(f"📊 Available assets: {assets_count}")
        
        # Show layer breakdown
        bronze = len(reader.list_assets('raw'))
        silver = len(reader.list_assets('silver'))  
        gold = len(reader.list_assets('gold'))
        
        print(f"   🥉 Bronze: {bronze}")
        print(f"   🥈 Silver: {silver}")
        print(f"   🥇 Gold: {gold}")
    else:
        print("❌ No assets found")
        
    return reader


def quick_read(asset_identifier: str, key_prefix: Optional[str] = None, validate: bool = True, data_dir=DEFAULT_DATA_DIR) -> pd.DataFrame:
    """Quick function to read an asset
    
    Args:
        asset_identifier: Asset name or full path
        key_prefix: Optional key prefix for disambiguation
        validate: Whether to validate data
        data_dir: Data directory path
    """
    reader = AssetDataReader(data_dir)
    return reader.read_asset(asset_identifier, key_prefix=key_prefix, validate=validate)


def list_available_assets(data_dir=DEFAULT_DATA_DIR) -> List[str]:
    """Quick function to list all available assets"""
    reader = AssetDataReader(data_dir)
    return reader.list_assets()