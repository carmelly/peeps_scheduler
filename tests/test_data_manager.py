"""
Test DataManager functionality with focus on practical path management.

Following testing philosophy:
- Test what could actually break
- Use inline object creation for simple tests
- Use fixtures only for infrastructure (temp directories)
- Clear, focused test names that explain what's being verified
"""

import pytest
import tempfile
from pathlib import Path
from data_manager import DataManager, get_data_manager


@pytest.mark.unit
class TestDataManager:
    """Test DataManager core functionality."""
    
    def test_basic_path_resolution(self):
        """Test that DataManager resolves paths correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dm = DataManager(submodule_root=temp_dir)
            
            # Test basic path resolution
            assert dm.get_original_data_path() == Path(temp_dir) / "original"
            assert dm.get_period_path("2025-09") == Path(temp_dir) / "original" / "2025-09"
            assert dm.get_original_data_path("2025-09") == Path(temp_dir) / "original" / "2025-09"
    
    def test_directory_creation(self):
        """Test that DataManager creates directories as needed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dm = DataManager(submodule_root=temp_dir)
            
            # Should create original directory during init
            assert (Path(temp_dir) / "original").exists()
            
            # Should create period directory when requested
            period_path = dm.ensure_period_exists("2025-09")
            assert period_path.exists()
            assert period_path == Path(temp_dir) / "original" / "2025-09"
    
    def test_period_listing(self):
        """Test that DataManager correctly lists existing periods."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dm = DataManager(submodule_root=temp_dir)
            
            # Initially no periods
            assert dm.list_periods() == []
            
            # Create some periods
            dm.ensure_period_exists("2025-09")
            dm.ensure_period_exists("2025-08")
            dm.ensure_period_exists("2025-10")
            
            # Should list them sorted
            periods = dm.list_periods()
            assert periods == ["2025-08", "2025-09", "2025-10"]
    
    def test_uses_default_submodule_root(self):
        """Test that DataManager uses 'peeps_data' as default submodule root."""
        dm = DataManager()
        assert str(dm.submodule_root) == "peeps_data"
    
    def test_custom_submodule_root(self):
        """Test that DataManager accepts custom submodule root."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dm = DataManager(submodule_root=temp_dir)
            assert str(dm.submodule_root) == temp_dir


@pytest.mark.unit
class TestGlobalDataManager:
    """Test global DataManager singleton functionality."""
    
    def test_singleton_behavior(self):
        """Test that get_data_manager returns the same instance."""
        dm1 = get_data_manager()
        dm2 = get_data_manager()
        assert dm1 is dm2
    
    def test_singleton_uses_default_root(self):
        """Test that global DataManager uses default peeps_data root."""
        dm = get_data_manager()
        assert str(dm.submodule_root) == "peeps_data"


@pytest.mark.unit
class TestDataManagerErrorHandling:
    """Test DataManager error handling and edge cases."""
    
    def test_handles_nonexistent_base_directory(self):
        """Test that DataManager handles nonexistent base directories gracefully."""
        # This should work - DataManager creates directories as needed
        with tempfile.TemporaryDirectory() as temp_dir:
            nonexistent_path = str(Path(temp_dir) / "does_not_exist")
            dm = DataManager(submodule_root=nonexistent_path)
            
            # Should create the directory structure
            assert dm.get_original_data_path().exists()
    
    def test_period_path_with_special_characters(self):
        """Test that period paths work with reasonable special characters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dm = DataManager(submodule_root=temp_dir)
            
            # Test period with hyphens (common case)
            period_path = dm.ensure_period_exists("2025-09-special")
            assert period_path.exists()
            assert "2025-09-special" in dm.list_periods()
