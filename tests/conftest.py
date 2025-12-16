"""Global pytest fixtures"""
import pytest
from unittest.mock import Mock


@pytest.fixture
def mock_htcondor_schedd():
    """Mock HTCondor schedd object"""
    return Mock()


@pytest.fixture
def sample_job_data():
    """Sample job data for testing"""
    return [
        {'ClusterId': 1, 'ProcId': 0, 'Owner': 'user1',
         'RequestMemory': 1000, 'MemoryUsage': 800, 'JobStatus': 4},
        {'ClusterId': 1, 'ProcId': 1, 'Owner': 'user1',
         'RequestMemory': 1000, 'MemoryUsage': 750, 'JobStatus': 4},
        {'ClusterId': 2, 'ProcId': 0, 'Owner': 'user2',
         'RequestMemory': 2000, 'MemoryUsage': 1900, 'JobStatus': 4},
    ]
