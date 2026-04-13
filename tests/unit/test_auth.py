"""Unit tests for Authentication component"""

import pytest

from vegard.auth import AuthManager, AuthConfig, User


@pytest.fixture
def auth_manager():
    """Create auth manager for testing"""
    config = AuthConfig(enable_auth=True, secret_key="test-secret-key")
    return AuthManager(config)


def test_auth_manager_initialization(auth_manager):
    """Test auth manager initialization"""
    assert auth_manager is not None
    assert auth_manager.config.enable_auth is True
    assert len(auth_manager._users) > 0


def test_authenticate_user(auth_manager):
    """Test user authentication"""
    user = auth_manager.authenticate("admin", "password")
    assert user is not None
    assert user.username == "admin"
    assert user.role == "admin"


def test_authenticate_invalid_user(auth_manager):
    """Test authentication with invalid user"""
    user = auth_manager.authenticate("invalid", "password")
    assert user is None


def test_generate_token(auth_manager):
    """Test JWT token generation"""
    user = auth_manager.authenticate("admin", "password")
    token = auth_manager.generate_token(user)
    assert token is not None
    assert isinstance(token, str)


def test_verify_token(auth_manager):
    """Test JWT token verification"""
    user = auth_manager.authenticate("admin", "password")
    token = auth_manager.generate_token(user)
    
    verified_user = auth_manager.verify_token(token)
    assert verified_user is not None
    assert verified_user.username == "admin"


def test_verify_invalid_token(auth_manager):
    """Test verification of invalid token"""
    verified_user = auth_manager.verify_token("invalid-token")
    assert verified_user is None


def test_check_permission_admin(auth_manager):
    """Test permission check for admin user"""
    user = auth_manager.authenticate("admin", "password")
    assert auth_manager.check_permission(user, "any:permission") is True


def test_check_permission_operator(auth_manager):
    """Test permission check for operator user"""
    user = auth_manager.authenticate("operator", "password")
    assert auth_manager.check_permission(user, "read:entities") is True
    assert auth_manager.check_permission(user, "admin:action") is False


def test_add_user(auth_manager):
    """Test adding a new user"""
    auth_manager.add_user("newuser", "custom", ["read:entities"])
    user = auth_manager.authenticate("newuser", "any")
    assert user is not None
    assert user.role == "custom"


def test_auth_disabled():
    """Test auth manager with auth disabled"""
    config = AuthConfig(enable_auth=False)
    auth_manager = AuthManager(config)
    
    assert auth_manager.config.enable_auth is False
    assert auth_manager.check_permission(None, "any:permission") is True
