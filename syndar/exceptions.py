"""Custom exceptions for Syndar

Provides domain-specific exception types for better error handling.
"""


class SyndarError(Exception):
    """Base exception for all Syndar errors"""

    pass


class TransportError(SyndarError):
    """Transport layer errors (gRPC, MQTT)"""

    pass


class MeshError(SyndarError):
    """Mesh layer errors (gossip, state management)"""

    pass


class TaskError(SyndarError):
    """Task allocation errors"""

    pass


class NodeError(SyndarError):
    """Node agent errors"""

    pass


class ConfigurationError(SyndarError):
    """Configuration loading errors"""

    pass


class AttestationError(SyndarError):
    """Cryptographic attestation errors"""

    pass


class SpectralError(SyndarError):
    """Spectral processing errors"""

    pass


class AIPBridgeError(SyndarError):
    """AIP integration errors"""

    pass


class DriftError(SyndarError):
    """Drift monitoring errors"""

    pass


class ValidationError(SyndarError):
    """Data validation errors"""

    pass
