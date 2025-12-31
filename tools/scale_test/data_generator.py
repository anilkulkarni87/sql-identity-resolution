#!/usr/bin/env python3
"""
IDR Scale Test Data Generator - Multi-Source Version

Generates synthetic retail customer data across MULTIPLE source tables
simulating real-world retail data architecture:
- Web orders (e-commerce)
- Store POS (in-store purchases)
- Mobile app (app transactions)
- Call center (phone orders)
- Partner data (third-party affiliates)

Features:
- Deterministic: same seed → identical data on any platform
- Multi-source: realistic retail data architecture
- Configurable cluster distributions
- Parquet export for cross-platform compatibility

Usage:
    python tools/scale_test/data_generator.py --rows=20000000 --seed=42 --output=data/
"""

import argparse
import hashlib
import os
import random
import string
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Generator, Dict
import json

# Try to import optional dependencies
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    print("Warning: pyarrow not installed. Install with: pip install pyarrow")


# ============================================
# CONFIGURATION
# ============================================

@dataclass
class ClusterConfig:
    """Cluster distribution configuration."""
    # Cluster size distributions (size_min, size_max, percentage)
    distributions: List[Tuple[int, int, float]] = None
    
    # Identifier match rates
    email_match_rate: float = 0.55
    phone_match_rate: float = 0.25
    loyalty_match_rate: float = 0.10
    address_match_rate: float = 0.10
    
    # Chain patterns (transitive matching)
    chain_rate: float = 0.15
    
    def __post_init__(self):
        if self.distributions is None:
            self.distributions = [
                (1, 1, 0.35),      # 35% singletons
                (2, 2, 0.25),      # 25% pairs
                (3, 5, 0.20),      # 20% small
                (6, 15, 0.12),     # 12% medium
                (16, 50, 0.05),    # 5% large
                (51, 200, 0.02),   # 2% very large
                (201, 1000, 0.01), # 1% massive
            ]


@dataclass
class SourceConfig:
    """Source table configuration."""
    source_id: str
    source_name: str
    percentage: float  # % of total entities from this source
    has_loyalty: bool = True
    has_address: bool = True
    data_quality: float = 0.95  # % of clean data (vs typos/nulls)


# Retail-style source systems (like Nike/Lululemon architecture)
SOURCE_SYSTEMS = [
    SourceConfig("web", "Web E-commerce", 0.35, has_loyalty=True, has_address=True, data_quality=0.98),
    SourceConfig("store", "Store POS", 0.30, has_loyalty=True, has_address=False, data_quality=0.90),
    SourceConfig("mobile", "Mobile App", 0.20, has_loyalty=True, has_address=True, data_quality=0.95),
    SourceConfig("call_center", "Call Center", 0.10, has_loyalty=True, has_address=True, data_quality=0.85),
    SourceConfig("partner", "Partner/Affiliate", 0.05, has_loyalty=False, has_address=False, data_quality=0.80),
]


# Data pools
FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason",
    "Isabella", "William", "Mia", "James", "Charlotte", "Benjamin", "Amelia",
    "Lucas", "Harper", "Henry", "Evelyn", "Alexander", "Abigail", "Michael",
    "Emily", "Daniel", "Elizabeth", "Jacob", "Sofia", "Logan", "Avery", "Jackson",
    "Ella", "Sebastian", "Scarlett", "Aiden", "Grace", "Matthew", "Chloe", "Samuel",
    "Victoria", "David", "Riley", "Joseph", "Aria", "Carter", "Lily", "Owen",
    "Aurora", "Wyatt", "Zoey", "John", "Penelope", "Jack", "Layla", "Luke",
    "Nora", "Jayden", "Camila", "Dylan", "Hannah", "Grayson", "Lillian", "Levi",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
]

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "aol.com", "protonmail.com", "mail.com", "live.com", "msn.com",
]

STREET_TYPES = ["St", "Ave", "Blvd", "Dr", "Ln", "Rd", "Way", "Ct", "Pl", "Cir"]
STREET_NAMES = [
    "Main", "Oak", "Maple", "Cedar", "Pine", "Elm", "Washington", "Lake",
    "Hill", "Park", "Forest", "River", "Spring", "Valley", "Sunset", "Mountain",
]

CITIES = [
    ("New York", "NY", "10001"), ("Los Angeles", "CA", "90001"),
    ("Chicago", "IL", "60601"), ("Houston", "TX", "77001"),
    ("Phoenix", "AZ", "85001"), ("Philadelphia", "PA", "19101"),
    ("San Antonio", "TX", "78201"), ("San Diego", "CA", "92101"),
    ("Dallas", "TX", "75201"), ("San Jose", "CA", "95101"),
    ("Austin", "TX", "78701"), ("Jacksonville", "FL", "32099"),
    ("Fort Worth", "TX", "76101"), ("Columbus", "OH", "43085"),
    ("Charlotte", "NC", "28201"), ("San Francisco", "CA", "94102"),
    ("Indianapolis", "IN", "46201"), ("Seattle", "WA", "98101"),
    ("Denver", "CO", "80201"), ("Boston", "MA", "02101"),
]

LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum", "diamond"]


# ============================================
# DETERMINISTIC RANDOM
# ============================================

class DeterministicRandom:
    """Wrapper for deterministic random generation."""
    
    def __init__(self, seed: int):
        self.seed = seed
        self.rng = random.Random(seed)
    
    def choice(self, seq):
        return self.rng.choice(seq)
    
    def choices(self, seq, weights=None, k=1):
        return self.rng.choices(seq, weights=weights, k=k)
    
    def randint(self, a, b):
        return self.rng.randint(a, b)
    
    def random(self):
        return self.rng.random()
    
    def shuffle(self, seq):
        self.rng.shuffle(seq)
    
    def uuid_from_index(self, index: int, prefix: str = "") -> str:
        """Generate deterministic UUID from seed and index."""
        data = f"{self.seed}:{prefix}:{index}".encode()
        h = hashlib.md5(data).hexdigest()
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


# ============================================
# DATA GENERATOR
# ============================================

class RetailMultiSourceGenerator:
    """Generates synthetic retail customer data across multiple sources."""
    
    def __init__(self, seed: int = 42, config: ClusterConfig = None, sources: List[SourceConfig] = None):
        self.rng = DeterministicRandom(seed)
        self.config = config or ClusterConfig()
        self.sources = sources or SOURCE_SYSTEMS
        self.entity_counters = {s.source_id: 0 for s in self.sources}
        self.used_emails = set()
        self.used_phones = set()
        self.used_loyalty_ids = set()
    
    def _assign_source(self, cluster_size: int) -> List[SourceConfig]:
        """Assign source systems to entities in a cluster."""
        weights = [s.percentage for s in self.sources]
        return self.rng.choices(self.sources, weights=weights, k=cluster_size)
    
    def generate_email(self, first_name: str, last_name: str, unique: bool = True) -> str:
        patterns = [
            f"{first_name.lower()}.{last_name.lower()}",
            f"{first_name.lower()}{last_name.lower()}",
            f"{first_name[0].lower()}{last_name.lower()}",
            f"{first_name.lower()}{self.rng.randint(1, 999)}",
        ]
        base = self.rng.choice(patterns)
        domain = self.rng.choice(EMAIL_DOMAINS)
        email = f"{base}@{domain}"
        
        if unique:
            counter = 0
            while email in self.used_emails:
                counter += 1
                email = f"{base}{counter}@{domain}"
            self.used_emails.add(email)
        
        return email
    
    def generate_phone(self, unique: bool = True) -> str:
        area = self.rng.randint(200, 999)
        exchange = self.rng.randint(200, 999)
        subscriber = self.rng.randint(1000, 9999)
        phone = f"{area}{exchange}{subscriber}"
        
        if unique:
            while phone in self.used_phones:
                subscriber = self.rng.randint(1000, 9999)
                phone = f"{area}{exchange}{subscriber}"
            self.used_phones.add(phone)
        
        return phone
    
    def generate_loyalty_id(self, unique: bool = True) -> str:
        prefix = self.rng.choice(["LYL", "MBR", "VIP", "PRO"])
        number = self.rng.randint(100000000, 999999999)
        loyalty_id = f"{prefix}{number}"
        
        if unique:
            while loyalty_id in self.used_loyalty_ids:
                number = self.rng.randint(100000000, 999999999)
                loyalty_id = f"{prefix}{number}"
            self.used_loyalty_ids.add(loyalty_id)
        
        return loyalty_id
    
    def generate_address(self) -> dict:
        street_num = self.rng.randint(1, 9999)
        street_name = self.rng.choice(STREET_NAMES)
        street_type = self.rng.choice(STREET_TYPES)
        city, state, zip_base = self.rng.choice(CITIES)
        zip_code = str(int(zip_base) + self.rng.randint(0, 99))
        
        apt = ""
        if self.rng.random() < 0.30:
            apt = f" Apt {self.rng.randint(1, 999)}"
        
        return {
            "street": f"{street_num} {street_name} {street_type}{apt}",
            "city": city,
            "state": state,
            "zip": zip_code,
        }
    
    def apply_data_quality(self, value: str, quality: float) -> Optional[str]:
        """Apply data quality degradation (nulls, typos)."""
        if value is None:
            return None
        if self.rng.random() > quality:
            # 50% null, 50% typo
            if self.rng.random() < 0.5:
                return None
            else:
                # Introduce typo
                if len(value) > 2:
                    pos = self.rng.randint(1, len(value) - 1)
                    return value[:pos] + self.rng.choice(string.ascii_lowercase) + value[pos+1:]
        return value
    
    def generate_entity(self, 
                       source: SourceConfig,
                       shared_email: str = None,
                       shared_phone: str = None,
                       shared_loyalty: str = None,
                       shared_address: dict = None) -> dict:
        """Generate a single entity for a specific source."""
        self.entity_counters[source.source_id] += 1
        
        first_name = self.rng.choice(FIRST_NAMES)
        last_name = self.rng.choice(LAST_NAMES)
        
        # Base entity
        entity = {
            "entity_id": self.rng.uuid_from_index(self.entity_counters[source.source_id], source.source_id),
            "source_system": source.source_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": self.apply_data_quality(
                shared_email or self.generate_email(first_name, last_name),
                source.data_quality
            ),
            "phone": self.apply_data_quality(
                shared_phone or self.generate_phone(),
                source.data_quality
            ),
            "loyalty_id": None,
            "address_street": None,
            "address_city": None,
            "address_state": None,
            "address_zip": None,
            "loyalty_tier": None,
            "created_at": datetime.now() - timedelta(days=self.rng.randint(1, 1825)),
        }
        
        # Loyalty ID (if source supports it)
        if source.has_loyalty:
            if shared_loyalty:
                entity["loyalty_id"] = shared_loyalty
            elif self.rng.random() < 0.6:
                entity["loyalty_id"] = self.generate_loyalty_id()
            if entity["loyalty_id"]:
                entity["loyalty_tier"] = self.rng.choice(LOYALTY_TIERS)
        
        # Address (if source supports it)
        if source.has_address:
            if shared_address:
                entity["address_street"] = shared_address["street"]
                entity["address_city"] = shared_address["city"]
                entity["address_state"] = shared_address["state"]
                entity["address_zip"] = shared_address["zip"]
            else:
                addr = self.generate_address()
                entity["address_street"] = addr["street"]
                entity["address_city"] = addr["city"]
                entity["address_state"] = addr["state"]
                entity["address_zip"] = addr["zip"]
        
        return entity
    
    def generate_cluster(self, size: int) -> List[dict]:
        """Generate a cluster of related entities across sources."""
        # Assign sources to each entity in cluster
        source_assignments = self._assign_source(size)
        
        if size == 1:
            return [self.generate_entity(source_assignments[0])]
        
        entities = []
        
        # Anchor entity
        anchor_source = source_assignments[0]
        anchor = self.generate_entity(anchor_source)
        entities.append(anchor)
        
        # Determine shared identifiers
        config = self.config
        share_email = self.rng.random() < config.email_match_rate
        share_phone = self.rng.random() < config.phone_match_rate
        share_loyalty = self.rng.random() < config.loyalty_match_rate
        share_address = self.rng.random() < config.address_match_rate
        
        for i in range(1, size):
            source = source_assignments[i]
            
            if self.rng.random() < config.chain_rate and i > 1:
                prev = entities[-1]
                entity = self.generate_entity(
                    source,
                    shared_email=prev["email"] if share_email and self.rng.random() < 0.5 else None,
                    shared_phone=prev["phone"] if share_phone and self.rng.random() < 0.5 else None,
                    shared_loyalty=prev["loyalty_id"] if share_loyalty and source.has_loyalty else None,
                )
            else:
                entity = self.generate_entity(
                    source,
                    shared_email=anchor["email"] if share_email and self.rng.random() < 0.7 else None,
                    shared_phone=anchor["phone"] if share_phone and self.rng.random() < 0.5 else None,
                    shared_loyalty=anchor["loyalty_id"] if share_loyalty and source.has_loyalty else None,
                    shared_address={"street": anchor["address_street"], "city": anchor["address_city"],
                                   "state": anchor["address_state"], "zip": anchor["address_zip"]} 
                                  if share_address and source.has_address and anchor["address_street"] else None,
                )
            entities.append(entity)
        
        return entities
    
    def generate_dataset(self, total_rows: int) -> Dict[str, Generator]:
        """Generate complete dataset as dict of generators per source."""
        print(f"Generating {total_rows:,} rows across {len(self.sources)} sources...")
        
        # Calculate cluster distribution
        remaining = total_rows
        clusters_to_generate = []
        
        for size_min, size_max, percentage in self.config.distributions:
            target_entities = int(total_rows * percentage)
            while target_entities >= size_min:  # Fixed: only generate if we have enough entities
                size = self.rng.randint(size_min, min(size_max, target_entities))
                clusters_to_generate.append(size)
                target_entities -= size
                remaining -= size
        
        while remaining > 0:
            clusters_to_generate.append(1)
            remaining -= 1
        
        self.rng.shuffle(clusters_to_generate)
        
        print(f"  Total clusters: {len(clusters_to_generate):,}")
        
        # Group entities by source
        entities_by_source = {s.source_id: [] for s in self.sources}
        generated = 0
        
        for i, size in enumerate(clusters_to_generate):
            cluster = self.generate_cluster(size)
            for entity in cluster:
                entities_by_source[entity["source_system"]].append(entity)
                generated += 1
            
            if (i + 1) % 100000 == 0:
                print(f"  Progress: {generated:,} / {total_rows:,} ({100*generated/total_rows:.1f}%)")
        
        # Print distribution
        print(f"\n  Source distribution:")
        for source in self.sources:
            count = len(entities_by_source[source.source_id])
            pct = 100 * count / total_rows if total_rows > 0 else 0
            print(f"    {source.source_id}: {count:,} ({pct:.1f}%)")
        
        return entities_by_source


# ============================================
# EXPORT FUNCTIONS
# ============================================

def export_to_parquet(entities_by_source: Dict[str, List], output_dir: str):
    """Export each source to separate Parquet file."""
    if not HAS_PYARROW:
        raise ImportError("pyarrow required. Install with: pip install pyarrow")
    
    schema = pa.schema([
        ("entity_id", pa.string()),
        ("source_system", pa.string()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("email", pa.string()),
        ("phone", pa.string()),
        ("loyalty_id", pa.string()),
        ("address_street", pa.string()),
        ("address_city", pa.string()),
        ("address_state", pa.string()),
        ("address_zip", pa.string()),
        ("loyalty_tier", pa.string()),
        ("created_at", pa.timestamp("us")),
    ])
    
    os.makedirs(output_dir, exist_ok=True)
    
    files = {}
    for source_id, entities in entities_by_source.items():
        if not entities:
            continue
        
        filename = f"customers_{source_id}.parquet"
        filepath = os.path.join(output_dir, filename)
        
        table = pa.Table.from_pylist(entities, schema=schema)
        pq.write_table(table, filepath)
        
        size_mb = os.path.getsize(filepath) / 1e6
        print(f"  {filename}: {len(entities):,} rows ({size_mb:.1f} MB)")
        files[source_id] = filepath
    
    # Also create a combined file for convenience
    all_entities = []
    for entities in entities_by_source.values():
        all_entities.extend(entities)
    
    combined_path = os.path.join(output_dir, "customers_all.parquet")
    table = pa.Table.from_pylist(all_entities, schema=schema)
    pq.write_table(table, combined_path)
    size_mb = os.path.getsize(combined_path) / 1e6
    print(f"  customers_all.parquet: {len(all_entities):,} rows ({size_mb:.1f} MB)")
    
    return files


# ============================================
# CLI
# ============================================

def main():
    parser = argparse.ArgumentParser(description="Generate IDR scale test data (multi-source)")
    parser.add_argument("--rows", type=int, required=True, help="Total rows to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output", type=str, default="data/", help="Output directory")
    parser.add_argument("--format", choices=["parquet"], default="parquet", help="Output format")
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"IDR Scale Test Data Generator (Multi-Source)")
    print(f"{'='*60}")
    print(f"  Total Rows: {args.rows:,}")
    print(f"  Seed: {args.seed}")
    print(f"  Sources: {', '.join(s.source_id for s in SOURCE_SYSTEMS)}")
    print(f"{'='*60}\n")
    
    generator = RetailMultiSourceGenerator(seed=args.seed)
    entities_by_source = generator.generate_dataset(args.rows)
    
    print(f"\n{'='*60}")
    print("Exporting to Parquet...")
    print(f"{'='*60}")
    
    files = export_to_parquet(entities_by_source, args.output)
    
    # Save metadata
    metadata = {
        "rows": args.rows,
        "seed": args.seed,
        "generated_at": datetime.now().isoformat(),
        "sources": [
            {"source_id": s.source_id, "name": s.source_name, "percentage": s.percentage}
            for s in SOURCE_SYSTEMS
        ],
        "files": list(files.values()),
    }
    
    metadata_path = os.path.join(args.output, "metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n{'='*60}")
    print("Generation complete!")
    print(f"  Output: {args.output}")
    print(f"  Files: {len(files) + 1} (per-source + combined)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
