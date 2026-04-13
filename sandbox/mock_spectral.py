"""Mock Spectral Generator - Synthetic hyperspectral cubes

Generates realistic HSI data for testing without live drone hardware.
Configurable soil chemistry patterns for controlled testing.
"""

import argparse
import hashlib
import json
import random
from pathlib import Path
from typing import Optional

import numpy as np
import structlog

logger = structlog.get_logger()

# SWIR band ranges (wavelengths in nm)
SWIR_BANDS = list(range(1000, 2501, 10))  # 1000-2500nm, 10nm steps

# Spectral signatures for common soil components
SPECTRAL_SIGNATURES = {
    "nitrogen_high": {
        "peaks": [1500, 2000, 2200],
        "intensity": 0.8,
        "width": 50,
    },
    "nitrogen_low": {
        "peaks": [1500, 2000, 2200],
        "intensity": 0.3,
        "width": 50,
    },
    "carbon_high": {
        "peaks": [1700, 2100, 2300],
        "intensity": 0.9,
        "width": 60,
    },
    "carbon_low": {
        "peaks": [1700, 2100, 2300],
        "intensity": 0.4,
        "width": 60,
    },
    "moisture_high": {
        "peaks": [1450, 1940],
        "intensity": 0.7,
        "width": 40,
    },
    "moisture_low": {
        "peaks": [1450, 1940],
        "intensity": 0.2,
        "width": 40,
    },
    "microplastic_pe": {
        "peaks": [1700, 2300, 2350],
        "intensity": 0.5,
        "width": 30,
    },
    "microplastic_pp": {
        "peaks": [1650, 2250],
        "intensity": 0.4,
        "width": 35,
    },
    "pfas": {
        "peaks": [1800, 2400],
        "intensity": 0.3,
        "width": 45,
    },
}


class SpectralCubeGenerator:
    """Generator for synthetic hyperspectral cubes"""

    def __init__(
        self,
        width: int = 128,
        height: int = 128,
        bands: Optional[list[int]] = None,
        seed: Optional[int] = None,
    ):
        self.width = width
        self.height = height
        self.bands = bands or SWIR_BANDS
        self.seed = seed or random.randint(0, 2**32)
        self.rng = random.Random(self.seed)
        self.np_rng = np.random.default_rng(self.seed)

    def generate_cube(
        self,
        field_id: str,
        soil_type: str = "normal",
        contamination_type: Optional[str] = None,
        contamination_level: float = 0.0,
        noise_level: float = 0.05,
    ) -> np.ndarray:
        """Generate synthetic spectral cube"""
        logger.info(
            "Generating spectral cube",
            field_id=field_id,
            soil_type=soil_type,
            contamination=contamination_type,
            level=contamination_level,
        )

        # Initialize cube with baseline
        cube = np.zeros((self.width, self.height, len(self.bands)), dtype=np.float32)

        # Add soil baseline
        cube += self._generate_baseline()

        # Add soil-specific patterns
        if soil_type == "high_nitrogen":
            cube += self._apply_signature("nitrogen_high", coverage=0.8)
        elif soil_type == "low_nitrogen":
            cube += self._apply_signature("nitrogen_low", coverage=0.8)
        elif soil_type == "high_carbon":
            cube += self._apply_signature("carbon_high", coverage=0.7)
        elif soil_type == "low_carbon":
            cube += self._apply_signature("carbon_low", coverage=0.7)
        elif soil_type == "wet":
            cube += self._apply_signature("moisture_high", coverage=0.9)
        elif soil_type == "dry":
            cube += self._apply_signature("moisture_low", coverage=0.9)

        # Add contamination patterns
        if contamination_type and contamination_level > 0:
            if contamination_type in SPECTRAL_SIGNATURES:
                cube += self._apply_signature(
                    contamination_type,
                    coverage=contamination_level,
                    pattern="clustered",
                )

        # Add noise
        if noise_level > 0:
            cube += self.np_rng.normal(0, noise_level, cube.shape)

        # Normalize
        cube = np.clip(cube, 0, 1)

        return cube

    def _generate_baseline(self) -> np.ndarray:
        """Generate baseline soil reflectance"""
        # General soil reflectance curve
        baseline = np.zeros((self.width, self.height, len(self.bands)))

        for i, wavelength in enumerate(self.bands):
            # Simplified soil reflectance curve
            # Higher reflectance in SWIR, with some variation
            base = 0.2 + 0.3 * (wavelength - 1000) / 1500
            # Add spatial variation
            variation = self.np_rng.uniform(-0.05, 0.05, (self.width, self.height))
            baseline[:, :, i] = base + variation

        return baseline

    def _apply_signature(
        self,
        signature_name: str,
        coverage: float = 0.5,
        pattern: str = "random",
    ) -> np.ndarray:
        """Apply spectral signature to cube regions"""
        signature = SPECTRAL_SIGNATURES[signature_name]
        layer = np.zeros((self.width, self.height, len(self.bands)))

        # Create spatial mask
        if pattern == "random":
            mask = self.np_rng.random((self.width, self.height)) < coverage
        elif pattern == "clustered":
            # Create clustered contamination
            mask = self._generate_clustered_mask(coverage)
        else:
            mask = np.ones((self.width, self.height), dtype=bool)

        # Apply signature to spectral bands
        for peak_wavelength in signature["peaks"]:
            # Find closest band
            band_idx = min(range(len(self.bands)), key=lambda i: abs(self.bands[i] - peak_wavelength))

            # Create gaussian peak
            for i in range(len(self.bands)):
                distance = abs(self.bands[i] - peak_wavelength)
                intensity = signature["intensity"] * np.exp(-(distance ** 2) / (2 * signature["width"] ** 2))
                layer[:, :, i] += mask * intensity

        return layer

    def _generate_clustered_mask(self, coverage: float) -> np.ndarray:
        """Generate spatially clustered mask"""
        mask = np.zeros((self.width, self.height), dtype=bool)

        # Seed clusters
        num_clusters = max(1, int(coverage * 10))
        for _ in range(num_clusters):
            cx = self.np_rng.integers(0, self.width)
            cy = self.np_rng.integers(0, self.height)
            radius = self.np_rng.integers(5, 20)

            y, x = np.ogrid[:self.height, :self.width]
            dist_from_center = np.sqrt((x - cx) ** 2 + (y - cy) ** 2)
            cluster_mask = dist_from_center <= radius
            mask |= cluster_mask

        return mask

    def add_spatial_gradient(
        self,
        cube: np.ndarray,
        direction: str = "north_south",
        magnitude: float = 0.2,
    ) -> np.ndarray:
        """Add directional gradient to simulate field variability"""
        if direction == "north_south":
            gradient = np.linspace(0, magnitude, self.height).reshape(1, -1, 1)
        elif direction == "east_west":
            gradient = np.linspace(0, magnitude, self.width).reshape(-1, 1, 1)
        else:
            return cube

        return cube * (1 + gradient)

    def export_cube(
        self,
        cube: np.ndarray,
        output_path: Path,
        metadata: Optional[dict] = None,
    ) -> None:
        """Export cube to file"""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save numpy array
        np.save(output_path.with_suffix(".npy"), cube)

        # Save metadata
        meta = {
            "shape": cube.shape,
            "dtype": str(cube.dtype),
            "bands": self.bands,
            "seed": self.seed,
            **(metadata or {}),
        }

        # Calculate spectral hash
        meta["spectral_hash"] = hashlib.sha256(cube.tobytes()).hexdigest()

        with open(output_path.with_suffix(".json"), "w") as f:
            json.dump(meta, f, indent=2)

        logger.info(
            "Cube exported",
            path=str(output_path),
            shape=cube.shape,
            hash=meta["spectral_hash"][:16] + "...",
        )


def generate_test_suite(output_dir: Path) -> None:
    """Generate comprehensive test dataset"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    generator = SpectralCubeGenerator(width=64, height=64, seed=42)

    test_cases = [
        ("normal", None, 0.0),
        ("high_nitrogen", None, 0.0),
        ("low_nitrogen", None, 0.0),
        ("high_carbon", None, 0.0),
        ("wet", None, 0.0),
        ("dry", None, 0.0),
        ("normal", "microplastic_pe", 0.3),
        ("normal", "microplastic_pp", 0.2),
        ("high_nitrogen", "pfas", 0.1),
    ]

    for soil_type, contam_type, contam_level in test_cases:
        name = f"{soil_type}"
        if contam_type:
            name += f"_{contam_type}_{int(contam_level * 100)}"

        cube = generator.generate_cube(
            field_id=f"test-{name}",
            soil_type=soil_type,
            contamination_type=contam_type,
            contamination_level=contam_level,
        )

        generator.export_cube(
            cube,
            output_dir / name,
            metadata={
                "soil_type": soil_type,
                "contamination_type": contam_type,
                "contamination_level": contam_level,
            },
        )

    logger.info("Test suite generated", cases=len(test_cases), dir=str(output_dir))


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description="Generate synthetic spectral cubes")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("sandbox/data/cubes"),
        help="Output directory",
    )
    parser.add_argument(
        "--test-suite",
        action="store_true",
        help="Generate comprehensive test suite",
    )
    parser.add_argument(
        "--soil-type",
        default="normal",
        choices=["normal", "high_nitrogen", "low_nitrogen", "high_carbon", "wet", "dry"],
    )
    parser.add_argument(
        "--contamination",
        choices=["microplastic_pe", "microplastic_pp", "pfas"],
    )
    parser.add_argument(
        "--contamination-level",
        type=float,
        default=0.0,
        help="Contamination coverage 0.0-1.0",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=128,
    )
    parser.add_argument(
        "--height",
        type=int,
        default=128,
    )
    parser.add_argument(
        "--field-id",
        default="test-field",
    )

    args = parser.parse_args()

    if args.test_suite:
        generate_test_suite(args.output)
    else:
        generator = SpectralCubeGenerator(width=args.width, height=args.height)
        cube = generator.generate_cube(
            field_id=args.field_id,
            soil_type=args.soil_type,
            contamination_type=args.contamination,
            contamination_level=args.contamination_level,
        )

        generator.export_cube(
            cube,
            args.output / args.soil_type,
            metadata={
                "soil_type": args.soil_type,
                "contamination": args.contamination,
                "contamination_level": args.contamination_level,
            },
        )


if __name__ == "__main__":
    main()
