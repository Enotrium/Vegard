"""Custom exceptions for Vegard

Provides domain-specific exception types for better error handling.
"""


class VegardError(Exception):
    """Base exception for all Vegard errors"""

    pass


class TransportError(VegardError):
    """Transport layer errors (gRPC, MQTT)"""

    pass


class MeshError(VegardError):
    """Mesh layer errors (gossip, state management)"""

    pass


class TaskError(VegardError):
    """Task allocation errors"""

    pass


class NodeError(VegardError):
    """Node agent errors"""

    pass


class ConfigurationError(VegardError):
    """Configuration loading errors"""

    pass


class AttestationError(VegardError):
    """Cryptographic attestation errors"""

    pass


class SpectralError(VegardError):
    """Spectral processing errors"""

    pass


class AIPBridgeError(VegardError):
    """AIP integration errors"""

    pass


class DriftError(VegardError):
    """Drift monitoring errors"""

    pass


class ValidationError(VegardError):
    """Data validation errors"""

    pass
