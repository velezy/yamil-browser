"""
Ollama Configuration

Environment settings and model profiles for optimized inference.
"""

import os

# Ollama server URL
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")

# Embedding configuration
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "768"))

# Multi-instance scaling
OLLAMA_INSTANCES = [
    url.strip()
    for url in os.getenv("OLLAMA_INSTANCES", OLLAMA_URL).split(",")
    if url.strip()
]
OLLAMA_MAX_QUEUE_DEPTH = int(os.getenv("OLLAMA_MAX_QUEUE_DEPTH", "50"))

# Ollama performance configuration
OLLAMA_CONFIG = {
    'OLLAMA_NUM_PARALLEL': int(os.getenv('OLLAMA_NUM_PARALLEL', '4')),
    'OLLAMA_MAX_LOADED_MODELS': int(os.getenv('OLLAMA_MAX_LOADED_MODELS', '3')),
    'OLLAMA_FLASH_ATTENTION': os.getenv('OLLAMA_FLASH_ATTENTION', '1'),
    'OLLAMA_KEEP_ALIVE': os.getenv('OLLAMA_KEEP_ALIVE', '10m'),
}

# Model profiles with performance characteristics
MODEL_PROFILES = {
    'gemma2:2b': {
        'avg_time': 1.5,
        'memory': 'very_low',
        'best_for': ['classification', 'simple_qa', 'extraction'],
        'max_tokens': 512,
        'context_window': 8192,
    },
    'gemma3:4b': {
        'avg_time': 4.0,
        'memory': 'medium',
        'best_for': ['summarization', 'tool_selection', 'code_gen'],
        'max_tokens': 2048,
        'context_window': 8192,
    },
    'llama3.2:3b': {
        'avg_time': 3.0,
        'memory': 'low',
        'best_for': ['chat', 'simple_qa', 'classification'],
        'max_tokens': 1024,
        'context_window': 8192,
    },
    'llama3.1:8b': {
        'avg_time': 8.0,
        'memory': 'high',
        'best_for': ['synthesis', 'reasoning', 'reflection', 'code_gen'],
        'max_tokens': 4096,
        'context_window': 8192,
    },
}

# Cache configuration
CACHE_CONFIG = {
    'max_size': int(os.getenv('OLLAMA_CACHE_MAX_SIZE', '1000')),
    'ttl_seconds': int(os.getenv('OLLAMA_CACHE_TTL', '1800')),  # 30 minutes
    'eviction_percent': 0.2,  # Remove 20% when full
}

# Priority models to keep warm
PRIORITY_MODELS = os.getenv('OLLAMA_PRIORITY_MODELS', 'gemma3:4b,llama3.2:3b').split(',')

# Model warm-up configuration
WARMUP_CONFIG = {
    'max_warm_models': int(os.getenv('OLLAMA_MAX_WARM_MODELS', '3')),
    'keep_alive_seconds': int(os.getenv('OLLAMA_KEEP_ALIVE_SECONDS', '3600')),  # 1 hour
    'stale_threshold_seconds': 600,  # 10 minutes
}

# Default model for fallback
DEFAULT_MODEL = os.getenv('OLLAMA_DEFAULT_MODEL', 'gemma3:4b')
