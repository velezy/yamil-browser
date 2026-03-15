"""
Model Router

Task-based model selection for optimal performance.
Routes requests to the most appropriate model based on task type.
"""

import logging
from typing import Optional, TYPE_CHECKING

from .config import MODEL_PROFILES, DEFAULT_MODEL

if TYPE_CHECKING:
    from .model_warmer import ModelWarmer

logger = logging.getLogger(__name__)


class ModelRouter:
    """
    Route requests to optimal model based on task type.

    Features:
    - Task-type based model selection
    - Prefer warm models to avoid cold starts
    - Fallback to default model
    - Performance-based sorting
    """

    def __init__(self, model_warmer: Optional['ModelWarmer'] = None):
        """
        Initialize router.

        Args:
            model_warmer: Optional ModelWarmer for warm model preference
        """
        self.warmer = model_warmer
        self.model_profiles = MODEL_PROFILES
        self.request_counts: dict[str, int] = {}

    def select_model(
        self,
        task_type: str,
        prefer_warm: bool = True,
        max_time: Optional[float] = None,
        min_tokens: Optional[int] = None,
    ) -> str:
        """
        Select optimal model for a task.

        Args:
            task_type: Type of task (e.g., 'classification', 'synthesis')
            prefer_warm: Prefer already-loaded models
            max_time: Maximum acceptable response time
            min_tokens: Minimum required output tokens

        Returns:
            Model name (e.g., 'gemma3:4b')
        """
        # Find models suited for this task
        candidates = []
        for name, profile in self.model_profiles.items():
            if task_type in profile.get('best_for', []):
                # Apply filters
                if max_time and profile['avg_time'] > max_time:
                    continue
                if min_tokens and profile['max_tokens'] < min_tokens:
                    continue
                candidates.append((name, profile))

        if not candidates:
            logger.warning(f"No model found for task '{task_type}', using default")
            return DEFAULT_MODEL

        # Prefer warm models if available
        if prefer_warm and self.warmer:
            warm_candidates = [
                (name, profile) for name, profile in candidates
                if self.warmer.is_warm(name)
            ]
            if warm_candidates:
                candidates = warm_candidates
                logger.debug(f"Using warm model candidates: {[c[0] for c in candidates]}")

        # Sort by speed (lower avg_time is better)
        candidates.sort(key=lambda x: x[1]['avg_time'])

        selected = candidates[0][0]
        self.request_counts[selected] = self.request_counts.get(selected, 0) + 1

        logger.info(f"Selected model '{selected}' for task '{task_type}'")
        return selected

    def select_for_complexity(
        self,
        complexity: str,
        prefer_warm: bool = True,
    ) -> str:
        """
        Select model based on task complexity.

        Args:
            complexity: 'simple', 'medium', or 'complex'
            prefer_warm: Prefer already-loaded models

        Returns:
            Model name
        """
        complexity_map = {
            'simple': ['simple_qa', 'classification', 'extraction'],
            'medium': ['summarization', 'tool_selection', 'code_gen'],
            'complex': ['synthesis', 'reasoning', 'reflection'],
        }

        task_types = complexity_map.get(complexity, complexity_map['medium'])

        # Find best model for any of these task types
        for task_type in task_types:
            candidates = [
                (name, profile) for name, profile in self.model_profiles.items()
                if task_type in profile.get('best_for', [])
            ]
            if candidates:
                # Sort by speed
                candidates.sort(key=lambda x: x[1]['avg_time'])

                # Check warm preference
                if prefer_warm and self.warmer:
                    for name, _ in candidates:
                        if self.warmer.is_warm(name):
                            return name

                return candidates[0][0]

        return DEFAULT_MODEL

    def get_fastest_model(
        self,
        min_tokens: int = 512,
        prefer_warm: bool = True,
    ) -> str:
        """
        Get fastest model that meets token requirements.

        Args:
            min_tokens: Minimum required output tokens
            prefer_warm: Prefer already-loaded models

        Returns:
            Model name
        """
        candidates = [
            (name, profile) for name, profile in self.model_profiles.items()
            if profile['max_tokens'] >= min_tokens
        ]

        if not candidates:
            return DEFAULT_MODEL

        # Sort by speed
        candidates.sort(key=lambda x: x[1]['avg_time'])

        if prefer_warm and self.warmer:
            for name, _ in candidates:
                if self.warmer.is_warm(name):
                    return name

        return candidates[0][0]

    def get_best_quality_model(
        self,
        max_time: Optional[float] = None,
        prefer_warm: bool = True,
    ) -> str:
        """
        Get highest quality model within time constraints.

        Quality is approximated by max_tokens (larger context = better reasoning).

        Args:
            max_time: Maximum acceptable response time
            prefer_warm: Prefer already-loaded models

        Returns:
            Model name
        """
        candidates = list(self.model_profiles.items())

        if max_time:
            candidates = [
                (name, profile) for name, profile in candidates
                if profile['avg_time'] <= max_time
            ]

        if not candidates:
            return DEFAULT_MODEL

        # Sort by max_tokens (higher = better quality proxy)
        candidates.sort(key=lambda x: x[1]['max_tokens'], reverse=True)

        if prefer_warm and self.warmer:
            for name, _ in candidates:
                if self.warmer.is_warm(name):
                    return name

        return candidates[0][0]

    def get_model_info(self, model_name: str) -> Optional[dict]:
        """Get profile information for a model"""
        return self.model_profiles.get(model_name)

    def list_models_for_task(self, task_type: str) -> list[str]:
        """List all models suitable for a task type"""
        return [
            name for name, profile in self.model_profiles.items()
            if task_type in profile.get('best_for', [])
        ]

    def get_stats(self) -> dict:
        """Get router statistics"""
        return {
            'request_counts': self.request_counts.copy(),
            'available_models': list(self.model_profiles.keys()),
            'default_model': DEFAULT_MODEL,
        }
