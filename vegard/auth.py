"""Authentication and authorization for Vegard

Provides JWT-based authentication with role-based access control.
"""

import time
from dataclasses import dataclass
from typing import Optional

import structlog
from pydantic import BaseModel

logger = structlog.get_logger()


@dataclass
class User:
    """User with role information"""
    
    username: str
    role: str
    permissions: list[str]


class AuthConfig(BaseModel):
    """Authentication configuration"""
    
    secret_key: str = "vegard-secret-key-change-in-production"
    algorithm: str = "HS256"
    token_expiration_hours: int = 24
    enable_auth: bool = False


class AuthManager:
    """Authentication manager with JWT token support"""
    
    def __init__(self, config: Optional[AuthConfig] = None):
        self.config = config or AuthConfig()
        self._users: dict[str, User] = {}
        
        # Initialize default users if auth is enabled
        if self.config.enable_auth:
            self._init_default_users()
    
    def _init_default_users(self) -> None:
        """Initialize default users for development"""
        # Admin user
        self._users["admin"] = User(
            username="admin",
            role="admin",
            permissions=["*"],  # All permissions
        )
        
        # Operator user
        self._users["operator"] = User(
            username="operator",
            role="operator",
            permissions=[
                "read:entities",
                "read:tasks",
                "create:tasks",
                "read:drift",
                "read:fop",
            ],
        )
        
        # Read-only user
        self._users["readonly"] = User(
            username="readonly",
            role="readonly",
            permissions=[
                "read:entities",
                "read:tasks",
                "read:drift",
                "read:fop",
            ],
        )
        
        logger.info("Initialized default users", count=len(self._users))
    
    def authenticate(self, username: str, password: str) -> Optional[User]:
        """Authenticate user with username and password"""
        # For now, just check username exists
        # In production, verify password hash
        if username in self._users:
            return self._users[username]
        return None
    
    def generate_token(self, user: User) -> str:
        """Generate JWT token for user"""
        try:
            import jwt
            
            payload = {
                "username": user.username,
                "role": user.role,
                "permissions": user.permissions,
                "exp": int(time.time()) + self.config.token_expiration_hours * 3600,
                "iat": int(time.time()),
            }
            
            token = jwt.encode(payload, self.config.secret_key, algorithm=self.config.algorithm)
            return token
        except ImportError:
            logger.warning("PyJWT not installed, using mock token")
            return f"mock-token-{user.username}"
    
    def verify_token(self, token: str) -> Optional[User]:
        """Verify JWT token and return user"""
        if not self.config.enable_auth:
            return None
        
        try:
            import jwt
            
            payload = jwt.decode(
                token,
                self.config.secret_key,
                algorithms=[self.config.algorithm]
            )
            
            username = payload.get("username")
            if username in self._users:
                return self._users[username]
            return None
        except ImportError:
            logger.warning("PyJWT not installed, token verification disabled")
            return None
        except Exception as e:
            logger.warning("Token verification failed", error=str(e))
            return None
    
    def check_permission(self, user: Optional[User], required_permission: str) -> bool:
        """Check if user has required permission"""
        if not self.config.enable_auth:
            return True
        
        if not user:
            return False
        
        # Admin has all permissions
        if "*" in user.permissions:
            return True
        
        return required_permission in user.permissions
    
    def add_user(self, username: str, role: str, permissions: list[str]) -> None:
        """Add a new user"""
        self._users[username] = User(
            username=username,
            role=role,
            permissions=permissions,
        )
        logger.info("User added", username=username, role=role)


# Global auth manager instance
_auth_manager: Optional[AuthManager] = None


def get_auth_manager() -> AuthManager:
    """Get global auth manager instance"""
    global _auth_manager
    if _auth_manager is None:
        _auth_manager = AuthManager()
    return _auth_manager


def set_auth_manager(auth_manager: AuthManager) -> None:
    """Set global auth manager instance"""
    global _auth_manager
    _auth_manager = auth_manager
