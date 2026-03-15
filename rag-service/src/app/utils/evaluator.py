"""
T.A.L.O.S. RAG Evaluation Metrics
Industry-standard evaluation for retrieval and generation quality

Metrics implemented:
- Precision@k, Recall@k, F1@k
- Mean Reciprocal Rank (MRR)
- Normalized Discounted Cumulative Gain (nDCG)
- Faithfulness Score (LLM-as-judge)
- Answer Relevancy
- Context Relevancy
- Hallucination Detection

Sources:
- Qdrant RAG Evaluation Guide
- RAGAS Framework
- LlamaIndex Evaluation
- Evidently AI
"""

import asyncio
import math
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
import json

logger = logging.getLogger(__name__)


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class RetrievalResult:
    """A single retrieval result"""
    chunk_id: int
    document_id: int
    content: str
    score: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EvaluationQuery:
    """A query with ground truth for evaluation"""
    query: str
    relevant_chunk_ids: List[int]  # Ground truth relevant chunks
    relevant_document_ids: List[int]  # Ground truth relevant documents
    expected_answer: Optional[str] = None  # For generation evaluation
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RetrievalMetrics:
    """Retrieval quality metrics"""
    precision_at_k: Dict[int, float] = field(default_factory=dict)  # k -> precision
    recall_at_k: Dict[int, float] = field(default_factory=dict)  # k -> recall
    f1_at_k: Dict[int, float] = field(default_factory=dict)  # k -> f1
    mrr: float = 0.0
    ndcg_at_k: Dict[int, float] = field(default_factory=dict)  # k -> ndcg
    hit_rate_at_k: Dict[int, float] = field(default_factory=dict)  # k -> hit rate

    def to_dict(self) -> Dict[str, Any]:
        return {
            "precision_at_k": self.precision_at_k,
            "recall_at_k": self.recall_at_k,
            "f1_at_k": self.f1_at_k,
            "mrr": self.mrr,
            "ndcg_at_k": self.ndcg_at_k,
            "hit_rate_at_k": self.hit_rate_at_k
        }


@dataclass
class GenerationMetrics:
    """Generation quality metrics"""
    faithfulness: float = 0.0  # 0-1, is answer grounded in context?
    answer_relevancy: float = 0.0  # 0-1, does answer address query?
    context_relevancy: float = 0.0  # 0-1, is retrieved context useful?
    hallucination_score: float = 0.0  # 0-1, lower is better

    def to_dict(self) -> Dict[str, Any]:
        return {
            "faithfulness": self.faithfulness,
            "answer_relevancy": self.answer_relevancy,
            "context_relevancy": self.context_relevancy,
            "hallucination_score": self.hallucination_score
        }


@dataclass
class EvaluationResult:
    """Complete evaluation result"""
    query: str
    retrieval_metrics: RetrievalMetrics
    generation_metrics: Optional[GenerationMetrics] = None
    latency_ms: float = 0.0
    retrieved_count: int = 0
    relevant_count: int = 0
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "retrieval_metrics": self.retrieval_metrics.to_dict(),
            "generation_metrics": self.generation_metrics.to_dict() if self.generation_metrics else None,
            "latency_ms": self.latency_ms,
            "retrieved_count": self.retrieved_count,
            "relevant_count": self.relevant_count,
            "timestamp": self.timestamp
        }


# =============================================================================
# RETRIEVAL METRICS
# =============================================================================

def precision_at_k(
    retrieved_ids: List[int],
    relevant_ids: List[int],
    k: int
) -> float:
    """
    Precision@k: What fraction of the top-k retrieved items are relevant?

    Target: >= 0.7 for Precision@5

    Args:
        retrieved_ids: List of retrieved chunk/document IDs in ranked order
        relevant_ids: List of ground truth relevant IDs
        k: Number of top results to consider

    Returns:
        Precision score between 0 and 1
    """
    if k <= 0:
        return 0.0

    retrieved_k = set(retrieved_ids[:k])
    relevant_set = set(relevant_ids)

    if not retrieved_k:
        return 0.0

    return len(retrieved_k & relevant_set) / k


def recall_at_k(
    retrieved_ids: List[int],
    relevant_ids: List[int],
    k: int
) -> float:
    """
    Recall@k: What fraction of relevant items appear in the top-k?

    Target: >= 0.8 for Recall@20

    Args:
        retrieved_ids: List of retrieved chunk/document IDs in ranked order
        relevant_ids: List of ground truth relevant IDs
        k: Number of top results to consider

    Returns:
        Recall score between 0 and 1
    """
    if not relevant_ids:
        return 0.0

    retrieved_k = set(retrieved_ids[:k])
    relevant_set = set(relevant_ids)

    return len(retrieved_k & relevant_set) / len(relevant_set)


def f1_at_k(
    retrieved_ids: List[int],
    relevant_ids: List[int],
    k: int
) -> float:
    """
    F1@k: Harmonic mean of Precision@k and Recall@k

    Args:
        retrieved_ids: List of retrieved chunk/document IDs in ranked order
        relevant_ids: List of ground truth relevant IDs
        k: Number of top results to consider

    Returns:
        F1 score between 0 and 1
    """
    p = precision_at_k(retrieved_ids, relevant_ids, k)
    r = recall_at_k(retrieved_ids, relevant_ids, k)

    if p + r == 0:
        return 0.0

    return 2 * (p * r) / (p + r)


def mean_reciprocal_rank(
    retrieved_ids: List[int],
    relevant_ids: List[int]
) -> float:
    """
    Mean Reciprocal Rank (MRR): How early does the first relevant item appear?

    Target: >= 0.6

    Args:
        retrieved_ids: List of retrieved chunk/document IDs in ranked order
        relevant_ids: List of ground truth relevant IDs

    Returns:
        MRR score between 0 and 1
    """
    relevant_set = set(relevant_ids)

    for i, doc_id in enumerate(retrieved_ids):
        if doc_id in relevant_set:
            return 1.0 / (i + 1)

    return 0.0


def hit_rate_at_k(
    retrieved_ids: List[int],
    relevant_ids: List[int],
    k: int
) -> float:
    """
    Hit Rate@k: Is there at least one relevant item in top-k?

    Args:
        retrieved_ids: List of retrieved chunk/document IDs in ranked order
        relevant_ids: List of ground truth relevant IDs
        k: Number of top results to consider

    Returns:
        1.0 if hit, 0.0 otherwise
    """
    retrieved_k = set(retrieved_ids[:k])
    relevant_set = set(relevant_ids)

    return 1.0 if (retrieved_k & relevant_set) else 0.0


def dcg_at_k(
    retrieved_ids: List[int],
    relevant_ids: List[int],
    k: int,
    relevance_scores: Optional[Dict[int, float]] = None
) -> float:
    """
    Discounted Cumulative Gain at k

    Args:
        retrieved_ids: List of retrieved chunk/document IDs in ranked order
        relevant_ids: List of ground truth relevant IDs
        k: Number of top results to consider
        relevance_scores: Optional dict mapping ID to graded relevance (default: binary)

    Returns:
        DCG score
    """
    relevant_set = set(relevant_ids)
    dcg = 0.0

    for i, doc_id in enumerate(retrieved_ids[:k]):
        if doc_id in relevant_set:
            # Use provided relevance score or binary (1 for relevant)
            rel = relevance_scores.get(doc_id, 1.0) if relevance_scores else 1.0
            # DCG formula: rel / log2(i + 2)
            dcg += rel / math.log2(i + 2)

    return dcg


def ndcg_at_k(
    retrieved_ids: List[int],
    relevant_ids: List[int],
    k: int,
    relevance_scores: Optional[Dict[int, float]] = None
) -> float:
    """
    Normalized Discounted Cumulative Gain at k

    NDCG = DCG / IDCG (Ideal DCG)

    Args:
        retrieved_ids: List of retrieved chunk/document IDs in ranked order
        relevant_ids: List of ground truth relevant IDs
        k: Number of top results to consider
        relevance_scores: Optional dict mapping ID to graded relevance

    Returns:
        NDCG score between 0 and 1
    """
    dcg = dcg_at_k(retrieved_ids, relevant_ids, k, relevance_scores)

    # Ideal DCG: all relevant items ranked first
    ideal_retrieved = relevant_ids[:k]
    idcg = dcg_at_k(ideal_retrieved, relevant_ids, k, relevance_scores)

    if idcg == 0:
        return 0.0

    return dcg / idcg


def compute_retrieval_metrics(
    retrieved_ids: List[int],
    relevant_ids: List[int],
    k_values: List[int] = [1, 3, 5, 10, 20]
) -> RetrievalMetrics:
    """
    Compute all retrieval metrics for a single query

    Args:
        retrieved_ids: List of retrieved chunk/document IDs in ranked order
        relevant_ids: List of ground truth relevant IDs
        k_values: List of k values to compute metrics for

    Returns:
        RetrievalMetrics object with all computed metrics
    """
    metrics = RetrievalMetrics()

    for k in k_values:
        metrics.precision_at_k[k] = precision_at_k(retrieved_ids, relevant_ids, k)
        metrics.recall_at_k[k] = recall_at_k(retrieved_ids, relevant_ids, k)
        metrics.f1_at_k[k] = f1_at_k(retrieved_ids, relevant_ids, k)
        metrics.ndcg_at_k[k] = ndcg_at_k(retrieved_ids, relevant_ids, k)
        metrics.hit_rate_at_k[k] = hit_rate_at_k(retrieved_ids, relevant_ids, k)

    metrics.mrr = mean_reciprocal_rank(retrieved_ids, relevant_ids)

    return metrics


# =============================================================================
# GENERATION METRICS (LLM-as-Judge)
# =============================================================================

FAITHFULNESS_PROMPT = """You are evaluating the faithfulness of an AI-generated answer.

Context (retrieved information):
{context}

Question: {question}

Answer: {answer}

Task: Determine if the answer is fully supported by the context.
- Score 1.0: Every claim in the answer is directly supported by the context
- Score 0.7-0.9: Most claims are supported, minor unsupported details
- Score 0.4-0.6: Some claims supported, some not in context
- Score 0.1-0.3: Mostly unsupported claims
- Score 0.0: Answer contradicts or is completely unrelated to context

Output ONLY a number between 0 and 1:"""


ANSWER_RELEVANCY_PROMPT = """You are evaluating if an answer addresses the question.

Question: {question}

Answer: {answer}

Task: Determine how well the answer addresses the question.
- Score 1.0: Directly and completely answers the question
- Score 0.7-0.9: Mostly answers the question with minor gaps
- Score 0.4-0.6: Partially answers the question
- Score 0.1-0.3: Barely related to the question
- Score 0.0: Does not address the question at all

Output ONLY a number between 0 and 1:"""


CONTEXT_RELEVANCY_PROMPT = """You are evaluating the relevancy of retrieved context.

Question: {question}

Retrieved Context:
{context}

Task: Determine how relevant the context is to answering the question.
- Score 1.0: Context contains all information needed to answer
- Score 0.7-0.9: Context is highly relevant with minor gaps
- Score 0.4-0.6: Context is partially relevant
- Score 0.1-0.3: Context barely relates to the question
- Score 0.0: Context is completely irrelevant

Output ONLY a number between 0 and 1:"""


HALLUCINATION_PROMPT = """You are detecting hallucinations in an AI-generated answer.

Context (the ONLY source of truth):
{context}

Answer to evaluate:
{answer}

Task: Identify claims in the answer that are NOT supported by the context.
Count how many claims are hallucinated (made up, not in context).

Output format:
HALLUCINATED_CLAIMS: [number]
TOTAL_CLAIMS: [number]
HALLUCINATION_RATE: [decimal between 0 and 1]

Only output the three lines above, nothing else."""


async def evaluate_faithfulness(
    question: str,
    answer: str,
    context: str,
    llm_client: Any
) -> float:
    """
    Evaluate faithfulness using LLM-as-judge.

    Target: >= 0.9 (90% of claims supported by context)

    Args:
        question: The original question
        answer: The generated answer
        context: The retrieved context
        llm_client: LLM client with generate() method

    Returns:
        Faithfulness score between 0 and 1
    """
    prompt = FAITHFULNESS_PROMPT.format(
        context=context,
        question=question,
        answer=answer
    )

    try:
        response = await llm_client.generate(prompt)
        score = float(response.strip())
        return max(0.0, min(1.0, score))
    except Exception as e:
        logger.warning(f"Faithfulness evaluation failed: {e}")
        return 0.0


async def evaluate_answer_relevancy(
    question: str,
    answer: str,
    llm_client: Any
) -> float:
    """
    Evaluate answer relevancy using LLM-as-judge.

    Target: >= 0.8

    Args:
        question: The original question
        answer: The generated answer
        llm_client: LLM client with generate() method

    Returns:
        Answer relevancy score between 0 and 1
    """
    prompt = ANSWER_RELEVANCY_PROMPT.format(
        question=question,
        answer=answer
    )

    try:
        response = await llm_client.generate(prompt)
        score = float(response.strip())
        return max(0.0, min(1.0, score))
    except Exception as e:
        logger.warning(f"Answer relevancy evaluation failed: {e}")
        return 0.0


async def evaluate_context_relevancy(
    question: str,
    context: str,
    llm_client: Any
) -> float:
    """
    Evaluate context relevancy using LLM-as-judge.

    Args:
        question: The original question
        context: The retrieved context
        llm_client: LLM client with generate() method

    Returns:
        Context relevancy score between 0 and 1
    """
    prompt = CONTEXT_RELEVANCY_PROMPT.format(
        question=question,
        context=context
    )

    try:
        response = await llm_client.generate(prompt)
        score = float(response.strip())
        return max(0.0, min(1.0, score))
    except Exception as e:
        logger.warning(f"Context relevancy evaluation failed: {e}")
        return 0.0


async def evaluate_hallucination(
    answer: str,
    context: str,
    llm_client: Any
) -> float:
    """
    Detect hallucinations using LLM-as-judge.

    Target: < 0.05 (5% hallucination rate)

    Args:
        answer: The generated answer
        context: The retrieved context
        llm_client: LLM client with generate() method

    Returns:
        Hallucination rate between 0 and 1 (lower is better)
    """
    prompt = HALLUCINATION_PROMPT.format(
        context=context,
        answer=answer
    )

    try:
        response = await llm_client.generate(prompt)
        lines = response.strip().split('\n')

        for line in lines:
            if 'HALLUCINATION_RATE' in line:
                rate = float(line.split(':')[1].strip())
                return max(0.0, min(1.0, rate))

        return 0.0
    except Exception as e:
        logger.warning(f"Hallucination evaluation failed: {e}")
        return 0.0


async def compute_generation_metrics(
    question: str,
    answer: str,
    context: str,
    llm_client: Any
) -> GenerationMetrics:
    """
    Compute all generation metrics for a single query-answer pair.

    Args:
        question: The original question
        answer: The generated answer
        context: The retrieved context
        llm_client: LLM client with generate() method

    Returns:
        GenerationMetrics object with all computed metrics
    """
    # Run evaluations in parallel
    faithfulness_task = evaluate_faithfulness(question, answer, context, llm_client)
    relevancy_task = evaluate_answer_relevancy(question, answer, llm_client)
    context_task = evaluate_context_relevancy(question, context, llm_client)
    hallucination_task = evaluate_hallucination(answer, context, llm_client)

    faithfulness, relevancy, context_rel, hallucination = await asyncio.gather(
        faithfulness_task,
        relevancy_task,
        context_task,
        hallucination_task
    )

    return GenerationMetrics(
        faithfulness=faithfulness,
        answer_relevancy=relevancy,
        context_relevancy=context_rel,
        hallucination_score=hallucination
    )


# =============================================================================
# AGGREGATE EVALUATION
# =============================================================================

@dataclass
class AggregateMetrics:
    """Aggregated metrics across multiple queries"""
    num_queries: int = 0
    avg_precision_at_k: Dict[int, float] = field(default_factory=dict)
    avg_recall_at_k: Dict[int, float] = field(default_factory=dict)
    avg_f1_at_k: Dict[int, float] = field(default_factory=dict)
    avg_mrr: float = 0.0
    avg_ndcg_at_k: Dict[int, float] = field(default_factory=dict)
    avg_hit_rate_at_k: Dict[int, float] = field(default_factory=dict)
    avg_faithfulness: float = 0.0
    avg_answer_relevancy: float = 0.0
    avg_context_relevancy: float = 0.0
    avg_hallucination: float = 0.0
    avg_latency_ms: float = 0.0

    # Target compliance
    precision_5_target: float = 0.7
    recall_20_target: float = 0.8
    mrr_target: float = 0.6
    faithfulness_target: float = 0.9
    hallucination_target: float = 0.05

    def to_dict(self) -> Dict[str, Any]:
        return {
            "num_queries": self.num_queries,
            "retrieval": {
                "precision_at_k": self.avg_precision_at_k,
                "recall_at_k": self.avg_recall_at_k,
                "f1_at_k": self.avg_f1_at_k,
                "mrr": self.avg_mrr,
                "ndcg_at_k": self.avg_ndcg_at_k,
                "hit_rate_at_k": self.avg_hit_rate_at_k
            },
            "generation": {
                "faithfulness": self.avg_faithfulness,
                "answer_relevancy": self.avg_answer_relevancy,
                "context_relevancy": self.avg_context_relevancy,
                "hallucination_score": self.avg_hallucination
            },
            "latency_ms": self.avg_latency_ms,
            "target_compliance": {
                "precision_5": {
                    "value": self.avg_precision_at_k.get(5, 0),
                    "target": self.precision_5_target,
                    "passed": self.avg_precision_at_k.get(5, 0) >= self.precision_5_target
                },
                "recall_20": {
                    "value": self.avg_recall_at_k.get(20, 0),
                    "target": self.recall_20_target,
                    "passed": self.avg_recall_at_k.get(20, 0) >= self.recall_20_target
                },
                "mrr": {
                    "value": self.avg_mrr,
                    "target": self.mrr_target,
                    "passed": self.avg_mrr >= self.mrr_target
                },
                "faithfulness": {
                    "value": self.avg_faithfulness,
                    "target": self.faithfulness_target,
                    "passed": self.avg_faithfulness >= self.faithfulness_target
                },
                "hallucination": {
                    "value": self.avg_hallucination,
                    "target": self.hallucination_target,
                    "passed": self.avg_hallucination <= self.hallucination_target
                }
            }
        }


def aggregate_metrics(results: List[EvaluationResult]) -> AggregateMetrics:
    """
    Aggregate metrics across multiple evaluation results.

    Args:
        results: List of EvaluationResult objects

    Returns:
        AggregateMetrics with averaged scores
    """
    if not results:
        return AggregateMetrics()

    n = len(results)
    agg = AggregateMetrics(num_queries=n)

    # Collect all k values
    k_values = set()
    for r in results:
        k_values.update(r.retrieval_metrics.precision_at_k.keys())

    # Initialize k-based metrics
    for k in k_values:
        agg.avg_precision_at_k[k] = 0.0
        agg.avg_recall_at_k[k] = 0.0
        agg.avg_f1_at_k[k] = 0.0
        agg.avg_ndcg_at_k[k] = 0.0
        agg.avg_hit_rate_at_k[k] = 0.0

    # Sum metrics
    for r in results:
        agg.avg_mrr += r.retrieval_metrics.mrr
        agg.avg_latency_ms += r.latency_ms

        for k in k_values:
            agg.avg_precision_at_k[k] += r.retrieval_metrics.precision_at_k.get(k, 0)
            agg.avg_recall_at_k[k] += r.retrieval_metrics.recall_at_k.get(k, 0)
            agg.avg_f1_at_k[k] += r.retrieval_metrics.f1_at_k.get(k, 0)
            agg.avg_ndcg_at_k[k] += r.retrieval_metrics.ndcg_at_k.get(k, 0)
            agg.avg_hit_rate_at_k[k] += r.retrieval_metrics.hit_rate_at_k.get(k, 0)

        if r.generation_metrics:
            agg.avg_faithfulness += r.generation_metrics.faithfulness
            agg.avg_answer_relevancy += r.generation_metrics.answer_relevancy
            agg.avg_context_relevancy += r.generation_metrics.context_relevancy
            agg.avg_hallucination += r.generation_metrics.hallucination_score

    # Average metrics
    agg.avg_mrr /= n
    agg.avg_latency_ms /= n

    for k in k_values:
        agg.avg_precision_at_k[k] /= n
        agg.avg_recall_at_k[k] /= n
        agg.avg_f1_at_k[k] /= n
        agg.avg_ndcg_at_k[k] /= n
        agg.avg_hit_rate_at_k[k] /= n

    # Count results with generation metrics
    gen_count = sum(1 for r in results if r.generation_metrics)
    if gen_count > 0:
        agg.avg_faithfulness /= gen_count
        agg.avg_answer_relevancy /= gen_count
        agg.avg_context_relevancy /= gen_count
        agg.avg_hallucination /= gen_count

    return agg


# =============================================================================
# EVALUATION RUNNER
# =============================================================================

class RAGEvaluator:
    """
    RAG Evaluation runner for systematic quality assessment.

    Usage:
        evaluator = RAGEvaluator(search_fn, llm_client)
        results = await evaluator.evaluate_dataset(test_queries)
        summary = evaluator.get_summary(results)
    """

    def __init__(
        self,
        search_fn: Any,  # async function(query) -> List[RetrievalResult]
        llm_client: Any = None,  # Optional LLM for generation metrics
        k_values: List[int] = [1, 3, 5, 10, 20]
    ):
        self.search_fn = search_fn
        self.llm_client = llm_client
        self.k_values = k_values

    async def evaluate_query(
        self,
        query: EvaluationQuery,
        include_generation: bool = False,
        generated_answer: Optional[str] = None
    ) -> EvaluationResult:
        """
        Evaluate a single query.

        Args:
            query: EvaluationQuery with ground truth
            include_generation: Whether to compute generation metrics
            generated_answer: Optional pre-generated answer

        Returns:
            EvaluationResult with all metrics
        """
        import time
        start_time = time.time()

        # Run retrieval
        results = await self.search_fn(query.query)

        latency_ms = (time.time() - start_time) * 1000

        # Extract IDs
        retrieved_ids = [r.chunk_id for r in results]

        # Compute retrieval metrics
        retrieval_metrics = compute_retrieval_metrics(
            retrieved_ids,
            query.relevant_chunk_ids,
            self.k_values
        )

        # Compute generation metrics if requested
        generation_metrics = None
        if include_generation and self.llm_client and generated_answer:
            context = "\n\n".join([r.content for r in results[:5]])
            generation_metrics = await compute_generation_metrics(
                query.query,
                generated_answer,
                context,
                self.llm_client
            )

        return EvaluationResult(
            query=query.query,
            retrieval_metrics=retrieval_metrics,
            generation_metrics=generation_metrics,
            latency_ms=latency_ms,
            retrieved_count=len(results),
            relevant_count=len(query.relevant_chunk_ids)
        )

    async def evaluate_dataset(
        self,
        queries: List[EvaluationQuery],
        include_generation: bool = False,
        answers: Optional[List[str]] = None
    ) -> List[EvaluationResult]:
        """
        Evaluate a dataset of queries.

        Args:
            queries: List of EvaluationQuery objects
            include_generation: Whether to compute generation metrics
            answers: Optional list of generated answers (parallel to queries)

        Returns:
            List of EvaluationResult objects
        """
        results = []

        for i, query in enumerate(queries):
            answer = answers[i] if answers and i < len(answers) else None
            result = await self.evaluate_query(query, include_generation, answer)
            results.append(result)

            logger.info(
                f"Evaluated query {i+1}/{len(queries)}: "
                f"P@5={result.retrieval_metrics.precision_at_k.get(5, 0):.3f}, "
                f"MRR={result.retrieval_metrics.mrr:.3f}"
            )

        return results

    def get_summary(self, results: List[EvaluationResult]) -> AggregateMetrics:
        """Get aggregated summary of evaluation results."""
        return aggregate_metrics(results)


# =============================================================================
# RAGAS FRAMEWORK INTEGRATION
# =============================================================================

RAGAS_AVAILABLE = False

try:
    from ragas import evaluate as ragas_evaluate
    from ragas.metrics import (
        faithfulness,
        answer_relevancy,
        context_precision,
        context_recall,
        context_relevancy,
        answer_similarity,
        answer_correctness
    )
    from datasets import Dataset
    RAGAS_AVAILABLE = True
    logger.info("RAGAS framework loaded successfully")
except ImportError:
    logger.warning("RAGAS not installed. Run: pip install ragas datasets langchain")


@dataclass
class RAGASMetrics:
    """RAGAS framework evaluation metrics"""
    faithfulness: float = 0.0
    answer_relevancy: float = 0.0
    context_precision: float = 0.0
    context_recall: float = 0.0
    context_relevancy: float = 0.0
    answer_similarity: float = 0.0
    answer_correctness: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "faithfulness": self.faithfulness,
            "answer_relevancy": self.answer_relevancy,
            "context_precision": self.context_precision,
            "context_recall": self.context_recall,
            "context_relevancy": self.context_relevancy,
            "answer_similarity": self.answer_similarity,
            "answer_correctness": self.answer_correctness
        }


@dataclass
class RAGASEvaluationResult:
    """Complete RAGAS evaluation result"""
    metrics: RAGASMetrics
    num_samples: int
    evaluation_time_seconds: float
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    raw_scores: Optional[Dict[str, List[float]]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metrics": self.metrics.to_dict(),
            "num_samples": self.num_samples,
            "evaluation_time_seconds": self.evaluation_time_seconds,
            "timestamp": self.timestamp,
            "raw_scores": self.raw_scores
        }


class RAGASEvaluator:
    """
    RAGAS Framework Integration for RAG Evaluation.

    RAGAS provides state-of-the-art metrics for evaluating RAG systems:
    - Faithfulness: Are generated answers grounded in retrieved context?
    - Answer Relevancy: Does the answer address the question?
    - Context Precision: How relevant is the retrieved context?
    - Context Recall: Does context cover the ground truth?
    - Answer Similarity: Semantic similarity to ground truth
    - Answer Correctness: Factual correctness of the answer

    Reference: https://docs.ragas.io/
    """

    def __init__(
        self,
        llm: Any = None,
        embeddings: Any = None,
        metrics: Optional[List[str]] = None
    ):
        """
        Initialize RAGAS evaluator.

        Args:
            llm: LangChain-compatible LLM for evaluation
            embeddings: LangChain-compatible embeddings model
            metrics: List of metrics to compute. Default: all available
        """
        if not RAGAS_AVAILABLE:
            raise RuntimeError(
                "RAGAS not available. Install with: pip install ragas datasets langchain"
            )

        self.llm = llm
        self.embeddings = embeddings

        # Configure metrics
        self.available_metrics = {
            "faithfulness": faithfulness,
            "answer_relevancy": answer_relevancy,
            "context_precision": context_precision,
            "context_recall": context_recall,
            "context_relevancy": context_relevancy,
            "answer_similarity": answer_similarity,
            "answer_correctness": answer_correctness
        }

        if metrics:
            self.metrics_to_use = [
                self.available_metrics[m] for m in metrics
                if m in self.available_metrics
            ]
        else:
            # Default: core metrics that don't require ground truth answers
            self.metrics_to_use = [
                faithfulness,
                answer_relevancy,
                context_precision,
                context_relevancy
            ]

    def prepare_dataset(
        self,
        questions: List[str],
        answers: List[str],
        contexts: List[List[str]],
        ground_truths: Optional[List[str]] = None
    ) -> "Dataset":
        """
        Prepare dataset for RAGAS evaluation.

        Args:
            questions: List of questions/queries
            answers: List of generated answers
            contexts: List of retrieved context lists (one list per question)
            ground_truths: Optional list of ground truth answers

        Returns:
            HuggingFace Dataset ready for RAGAS evaluation
        """
        data = {
            "question": questions,
            "answer": answers,
            "contexts": contexts
        }

        if ground_truths:
            data["ground_truth"] = ground_truths

        return Dataset.from_dict(data)

    async def evaluate(
        self,
        questions: List[str],
        answers: List[str],
        contexts: List[List[str]],
        ground_truths: Optional[List[str]] = None
    ) -> RAGASEvaluationResult:
        """
        Run RAGAS evaluation on a dataset.

        Args:
            questions: List of questions/queries
            answers: List of generated answers
            contexts: List of retrieved context lists
            ground_truths: Optional ground truth answers

        Returns:
            RAGASEvaluationResult with all metrics
        """
        import time
        start_time = time.time()

        # Prepare dataset
        dataset = self.prepare_dataset(questions, answers, contexts, ground_truths)

        # Determine which metrics need ground truth
        metrics_for_eval = self.metrics_to_use.copy()
        if not ground_truths:
            # Remove metrics that require ground truth
            metrics_for_eval = [
                m for m in metrics_for_eval
                if m not in [answer_similarity, answer_correctness, context_recall]
            ]

        # Run evaluation
        try:
            result = ragas_evaluate(
                dataset,
                metrics=metrics_for_eval,
                llm=self.llm,
                embeddings=self.embeddings
            )

            evaluation_time = time.time() - start_time

            # Extract metrics
            metrics = RAGASMetrics(
                faithfulness=result.get("faithfulness", 0.0),
                answer_relevancy=result.get("answer_relevancy", 0.0),
                context_precision=result.get("context_precision", 0.0),
                context_recall=result.get("context_recall", 0.0),
                context_relevancy=result.get("context_relevancy", 0.0),
                answer_similarity=result.get("answer_similarity", 0.0),
                answer_correctness=result.get("answer_correctness", 0.0)
            )

            # Get per-sample scores
            raw_scores = {}
            for metric_name in result.keys():
                if metric_name in self.available_metrics:
                    raw_scores[metric_name] = result[metric_name]

            return RAGASEvaluationResult(
                metrics=metrics,
                num_samples=len(questions),
                evaluation_time_seconds=evaluation_time,
                raw_scores=raw_scores
            )

        except Exception as e:
            logger.error(f"RAGAS evaluation failed: {e}")
            raise

    async def evaluate_single(
        self,
        question: str,
        answer: str,
        contexts: List[str],
        ground_truth: Optional[str] = None
    ) -> RAGASMetrics:
        """
        Evaluate a single question-answer pair.

        Args:
            question: The question/query
            answer: The generated answer
            contexts: Retrieved context chunks
            ground_truth: Optional ground truth answer

        Returns:
            RAGASMetrics for this single sample
        """
        result = await self.evaluate(
            questions=[question],
            answers=[answer],
            contexts=[contexts],
            ground_truths=[ground_truth] if ground_truth else None
        )
        return result.metrics

    def get_metric_descriptions(self) -> Dict[str, str]:
        """Get descriptions of all RAGAS metrics"""
        return {
            "faithfulness": (
                "Measures if the generated answer is grounded in the retrieved context. "
                "Target: >= 0.9. A high score means the answer doesn't contain hallucinations."
            ),
            "answer_relevancy": (
                "Measures if the answer is relevant to the question. "
                "Target: >= 0.8. Checks if the response addresses what was asked."
            ),
            "context_precision": (
                "Measures if the retrieved context is relevant to the question. "
                "Target: >= 0.7. Higher means better retrieval quality."
            ),
            "context_recall": (
                "Measures if the context contains all information needed to answer. "
                "Target: >= 0.8. Requires ground truth answer."
            ),
            "context_relevancy": (
                "Measures the signal-to-noise ratio of the retrieved context. "
                "Higher means less irrelevant information in context."
            ),
            "answer_similarity": (
                "Semantic similarity between generated and ground truth answers. "
                "Requires ground truth. Target: >= 0.8."
            ),
            "answer_correctness": (
                "Factual correctness of the answer compared to ground truth. "
                "Requires ground truth. Target: >= 0.85."
            )
        }


def is_ragas_available() -> bool:
    """Check if RAGAS framework is available"""
    return RAGAS_AVAILABLE


async def get_ragas_evaluator(
    llm: Any = None,
    embeddings: Any = None,
    metrics: Optional[List[str]] = None
) -> Optional[RAGASEvaluator]:
    """
    Get or create RAGAS evaluator.

    Args:
        llm: LangChain-compatible LLM
        embeddings: LangChain-compatible embeddings
        metrics: List of metrics to use

    Returns:
        RAGASEvaluator instance or None if not available
    """
    if not RAGAS_AVAILABLE:
        logger.warning("RAGAS not available")
        return None

    return RAGASEvaluator(llm=llm, embeddings=embeddings, metrics=metrics)


# =============================================================================
# WEEKLY EVALUATION REPORT
# =============================================================================

@dataclass
class WeeklyEvaluationReport:
    """Weekly RAG evaluation report"""
    report_id: str
    week_start: str
    week_end: str
    total_queries_evaluated: int

    # Custom metrics summary
    custom_metrics: Dict[str, Any]

    # RAGAS metrics summary (if available)
    ragas_metrics: Optional[Dict[str, Any]]

    # Target compliance
    targets_met: Dict[str, bool]
    overall_score: float

    # Trend analysis
    trend_vs_previous_week: Optional[Dict[str, float]]

    # Recommendations
    recommendations: List[str]

    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_id": self.report_id,
            "week_start": self.week_start,
            "week_end": self.week_end,
            "total_queries_evaluated": self.total_queries_evaluated,
            "custom_metrics": self.custom_metrics,
            "ragas_metrics": self.ragas_metrics,
            "targets_met": self.targets_met,
            "overall_score": self.overall_score,
            "trend_vs_previous_week": self.trend_vs_previous_week,
            "recommendations": self.recommendations,
            "timestamp": self.timestamp
        }

    def to_markdown(self) -> str:
        """Generate markdown report"""
        lines = [
            f"# Weekly RAG Evaluation Report",
            f"",
            f"**Report ID:** {self.report_id}",
            f"**Period:** {self.week_start} to {self.week_end}",
            f"**Generated:** {self.timestamp}",
            f"",
            f"## Summary",
            f"",
            f"- **Total Queries Evaluated:** {self.total_queries_evaluated}",
            f"- **Overall Score:** {self.overall_score:.2%}",
            f"",
            f"## Target Compliance",
            f"",
        ]

        for target, met in self.targets_met.items():
            status = "PASS" if met else "FAIL"
            lines.append(f"- **{target}:** {status}")

        lines.extend([
            f"",
            f"## Retrieval Metrics (Custom)",
            f"",
        ])

        if "retrieval" in self.custom_metrics:
            for metric, value in self.custom_metrics["retrieval"].items():
                if isinstance(value, dict):
                    lines.append(f"### {metric}")
                    for k, v in value.items():
                        lines.append(f"  - k={k}: {v:.3f}")
                else:
                    lines.append(f"- **{metric}:** {value:.3f}")

        if self.ragas_metrics:
            lines.extend([
                f"",
                f"## RAGAS Metrics",
                f"",
            ])
            for metric, value in self.ragas_metrics.items():
                if isinstance(value, (int, float)):
                    lines.append(f"- **{metric}:** {value:.3f}")

        if self.trend_vs_previous_week:
            lines.extend([
                f"",
                f"## Trend vs Previous Week",
                f"",
            ])
            for metric, change in self.trend_vs_previous_week.items():
                direction = "improved" if change > 0 else "declined" if change < 0 else "unchanged"
                lines.append(f"- **{metric}:** {direction} ({change:+.2%})")

        if self.recommendations:
            lines.extend([
                f"",
                f"## Recommendations",
                f"",
            ])
            for i, rec in enumerate(self.recommendations, 1):
                lines.append(f"{i}. {rec}")

        return "\n".join(lines)


class WeeklyReportGenerator:
    """Generate weekly RAG evaluation reports"""

    def __init__(
        self,
        evaluator: RAGEvaluator,
        ragas_evaluator: Optional[RAGASEvaluator] = None,
        report_storage_path: str = "/tmp/rag_reports"
    ):
        self.evaluator = evaluator
        self.ragas_evaluator = ragas_evaluator
        self.report_storage_path = report_storage_path
        self._previous_report: Optional[WeeklyEvaluationReport] = None

    async def generate_report(
        self,
        test_queries: List[EvaluationQuery],
        answers: Optional[List[str]] = None,
        contexts: Optional[List[List[str]]] = None
    ) -> WeeklyEvaluationReport:
        """
        Generate weekly evaluation report.

        Args:
            test_queries: List of test queries with ground truth
            answers: Optional list of generated answers (for RAGAS)
            contexts: Optional list of context lists (for RAGAS)

        Returns:
            WeeklyEvaluationReport
        """
        import uuid
        from datetime import datetime, timedelta

        now = datetime.utcnow()
        week_start = (now - timedelta(days=7)).isoformat()
        week_end = now.isoformat()
        report_id = f"weekly_{now.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"

        # Run custom evaluation
        custom_results = await self.evaluator.evaluate_dataset(test_queries)
        custom_summary = self.evaluator.get_summary(custom_results)

        # Run RAGAS evaluation if available
        ragas_metrics = None
        if self.ragas_evaluator and answers and contexts:
            try:
                questions = [q.query for q in test_queries]
                ground_truths = [q.expected_answer for q in test_queries if q.expected_answer]

                ragas_result = await self.ragas_evaluator.evaluate(
                    questions=questions,
                    answers=answers,
                    contexts=contexts,
                    ground_truths=ground_truths if len(ground_truths) == len(questions) else None
                )
                ragas_metrics = ragas_result.metrics.to_dict()
            except Exception as e:
                logger.warning(f"RAGAS evaluation failed: {e}")

        # Calculate target compliance
        targets_met = {
            "precision_5": custom_summary.avg_precision_at_k.get(5, 0) >= 0.7,
            "recall_20": custom_summary.avg_recall_at_k.get(20, 0) >= 0.8,
            "mrr": custom_summary.avg_mrr >= 0.6,
            "faithfulness": custom_summary.avg_faithfulness >= 0.9,
            "hallucination": custom_summary.avg_hallucination <= 0.05
        }

        # Calculate overall score (weighted average)
        weights = {"precision_5": 0.2, "recall_20": 0.2, "mrr": 0.2, "faithfulness": 0.3, "hallucination": 0.1}
        overall_score = sum(1.0 if met else 0.0 for met in targets_met.values()) / len(targets_met)

        # Calculate trends
        trend = None
        if self._previous_report:
            trend = self._calculate_trend(custom_summary, self._previous_report)

        # Generate recommendations
        recommendations = self._generate_recommendations(custom_summary, targets_met, ragas_metrics)

        report = WeeklyEvaluationReport(
            report_id=report_id,
            week_start=week_start,
            week_end=week_end,
            total_queries_evaluated=len(test_queries),
            custom_metrics=custom_summary.to_dict(),
            ragas_metrics=ragas_metrics,
            targets_met=targets_met,
            overall_score=overall_score,
            trend_vs_previous_week=trend,
            recommendations=recommendations
        )

        # Store for trend analysis
        self._previous_report = report

        # Save report
        await self._save_report(report)

        return report

    def _calculate_trend(
        self,
        current: AggregateMetrics,
        previous: WeeklyEvaluationReport
    ) -> Dict[str, float]:
        """Calculate metric trends vs previous week"""
        prev_metrics = previous.custom_metrics.get("retrieval", {})
        trend = {}

        if "precision_at_k" in prev_metrics:
            prev_p5 = prev_metrics["precision_at_k"].get("5", prev_metrics["precision_at_k"].get(5, 0))
            curr_p5 = current.avg_precision_at_k.get(5, 0)
            trend["precision_5"] = curr_p5 - prev_p5

        if "mrr" in prev_metrics:
            trend["mrr"] = current.avg_mrr - prev_metrics["mrr"]

        return trend

    def _generate_recommendations(
        self,
        summary: AggregateMetrics,
        targets_met: Dict[str, bool],
        ragas_metrics: Optional[Dict[str, Any]]
    ) -> List[str]:
        """Generate actionable recommendations based on metrics"""
        recommendations = []

        if not targets_met.get("precision_5", True):
            recommendations.append(
                "Precision@5 is below target (0.7). Consider: "
                "1) Improving embedding model quality, "
                "2) Tuning similarity thresholds, "
                "3) Adding more diverse training data."
            )

        if not targets_met.get("recall_20", True):
            recommendations.append(
                "Recall@20 is below target (0.8). Consider: "
                "1) Expanding chunk overlap, "
                "2) Using hybrid search with keyword boosting, "
                "3) Implementing query expansion."
            )

        if not targets_met.get("mrr", True):
            recommendations.append(
                "MRR is below target (0.6). The first relevant result appears too late. Consider: "
                "1) Implementing re-ranking, "
                "2) Fine-tuning retrieval on domain data."
            )

        if not targets_met.get("faithfulness", True):
            recommendations.append(
                "Faithfulness is below target (0.9). Generated answers contain unsupported claims. Consider: "
                "1) Adding explicit citation requirements, "
                "2) Using smaller context windows, "
                "3) Implementing answer verification."
            )

        if not targets_met.get("hallucination", True):
            recommendations.append(
                "Hallucination rate exceeds target (5%). Consider: "
                "1) Adding hallucination detection post-processing, "
                "2) Using more conservative generation parameters, "
                "3) Implementing fact-checking pipelines."
            )

        if ragas_metrics:
            if ragas_metrics.get("context_precision", 1.0) < 0.7:
                recommendations.append(
                    "RAGAS context_precision is low. Retrieved context contains irrelevant information. "
                    "Consider improving retrieval filtering or reducing chunk size."
                )

        if not recommendations:
            recommendations.append(
                "All targets met! Consider raising target thresholds for continuous improvement."
            )

        return recommendations

    async def _save_report(self, report: WeeklyEvaluationReport) -> str:
        """Save report to storage"""
        import os
        import aiofiles

        os.makedirs(self.report_storage_path, exist_ok=True)

        # Save JSON
        json_path = os.path.join(self.report_storage_path, f"{report.report_id}.json")
        async with aiofiles.open(json_path, "w") as f:
            await f.write(json.dumps(report.to_dict(), indent=2))

        # Save Markdown
        md_path = os.path.join(self.report_storage_path, f"{report.report_id}.md")
        async with aiofiles.open(md_path, "w") as f:
            await f.write(report.to_markdown())

        logger.info(f"Saved weekly report: {json_path}")
        return json_path


# =============================================================================
# SAMPLE TEST DATASET
# =============================================================================

def create_sample_test_dataset() -> List[EvaluationQuery]:
    """
    Create a sample test dataset for evaluation.

    In production, this should be replaced with a curated dataset
    of 50+ query-answer pairs with ground truth.
    """
    return [
        EvaluationQuery(
            query="What is machine learning?",
            relevant_chunk_ids=[1, 2, 3],
            relevant_document_ids=[1],
            expected_answer="Machine learning is a subset of AI..."
        ),
        EvaluationQuery(
            query="How does RAG work?",
            relevant_chunk_ids=[10, 11, 12],
            relevant_document_ids=[2],
            expected_answer="RAG combines retrieval with generation..."
        ),
        # Add more queries...
    ]


# =============================================================================
# CUTTING EDGE: MULTI-SOURCE TRIANGULATION & CONFIDENCE CALIBRATION
# =============================================================================

@dataclass
class SourceEvidence:
    """Evidence from a single source."""
    source_id: str
    source_type: str  # "document", "web", "database", "api"
    content: str
    confidence: float  # Source reliability score
    timestamp: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "source_type": self.source_type,
            "content": self.content[:200] + "..." if len(self.content) > 200 else self.content,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "metadata": self.metadata
        }


@dataclass
class ClaimVerification:
    """Verification result for a single claim."""
    claim: str
    supporting_sources: List[str]  # Source IDs that support
    contradicting_sources: List[str]  # Source IDs that contradict
    neutral_sources: List[str]  # Source IDs with no opinion
    agreement_score: float  # 0-1, how much sources agree
    confidence: float  # Calibrated confidence
    verdict: str  # "verified", "contradicted", "uncertain", "unsupported"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "claim": self.claim,
            "supporting_sources": self.supporting_sources,
            "contradicting_sources": self.contradicting_sources,
            "neutral_sources": self.neutral_sources,
            "agreement_score": self.agreement_score,
            "confidence": self.confidence,
            "verdict": self.verdict
        }


@dataclass
class TriangulationResult:
    """Complete multi-source triangulation result."""
    answer: str
    claims: List[ClaimVerification]
    overall_confidence: float  # Calibrated overall confidence
    source_agreement: float  # How much sources agree overall
    verification_coverage: float  # % of claims verified
    recommended_action: str  # "accept", "flag_for_review", "reject"
    calibration_applied: bool
    raw_confidence: float  # Before calibration
    calibration_adjustment: float  # How much calibration changed confidence

    def to_dict(self) -> Dict[str, Any]:
        return {
            "answer": self.answer[:300] + "..." if len(self.answer) > 300 else self.answer,
            "claims": [c.to_dict() for c in self.claims],
            "overall_confidence": self.overall_confidence,
            "source_agreement": self.source_agreement,
            "verification_coverage": self.verification_coverage,
            "recommended_action": self.recommended_action,
            "calibration_applied": self.calibration_applied,
            "raw_confidence": self.raw_confidence,
            "calibration_adjustment": self.calibration_adjustment
        }


@dataclass
class CalibrationDataPoint:
    """A data point for confidence calibration."""
    predicted_confidence: float
    actual_correctness: float  # 0 or 1 (binary) or 0-1 (graded)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    query_type: str = "general"


@dataclass
class CalibrationBucket:
    """A bucket for calibration binning."""
    lower_bound: float
    upper_bound: float
    total_predictions: int = 0
    correct_predictions: float = 0.0  # Sum of actual correctness

    @property
    def accuracy(self) -> float:
        """Actual accuracy in this bucket."""
        return self.correct_predictions / self.total_predictions if self.total_predictions > 0 else 0.5

    @property
    def midpoint(self) -> float:
        """Midpoint of the bucket (expected confidence)."""
        return (self.lower_bound + self.upper_bound) / 2

    @property
    def calibration_error(self) -> float:
        """Difference between predicted and actual."""
        return abs(self.midpoint - self.accuracy)


class ConfidenceCalibrator:
    """
    Calibrates confidence scores to match actual accuracy.

    Uses isotonic regression and binning to ensure that when the system
    says it's 80% confident, it's actually correct ~80% of the time.

    Based on: Platt scaling, temperature scaling, and histogram binning.
    """

    def __init__(
        self,
        num_buckets: int = 10,
        min_samples_per_bucket: int = 20,
        temperature: float = 1.0,
        enable_platt_scaling: bool = True
    ):
        self.num_buckets = num_buckets
        self.min_samples_per_bucket = min_samples_per_bucket
        self.temperature = temperature
        self.enable_platt_scaling = enable_platt_scaling

        # Initialize buckets
        bucket_size = 1.0 / num_buckets
        self._buckets = [
            CalibrationBucket(
                lower_bound=i * bucket_size,
                upper_bound=(i + 1) * bucket_size
            )
            for i in range(num_buckets)
        ]

        # Platt scaling parameters (logistic regression)
        self._platt_a = 0.0  # Slope
        self._platt_b = 0.0  # Intercept
        self._platt_fitted = False

        # History for recalibration
        self._history: List[CalibrationDataPoint] = []
        self._max_history = 10000

        # Query-type specific calibration
        self._query_type_calibrators: Dict[str, 'ConfidenceCalibrator'] = {}

    def record_outcome(
        self,
        predicted_confidence: float,
        actual_correctness: float,
        query_type: str = "general"
    ):
        """
        Record a prediction outcome for calibration.

        Args:
            predicted_confidence: The confidence score that was predicted (0-1)
            actual_correctness: Whether the prediction was correct (0-1)
            query_type: Type of query for type-specific calibration
        """
        # Clamp values
        predicted_confidence = max(0.0, min(1.0, predicted_confidence))
        actual_correctness = max(0.0, min(1.0, actual_correctness))

        # Record in history
        self._history.append(CalibrationDataPoint(
            predicted_confidence=predicted_confidence,
            actual_correctness=actual_correctness,
            query_type=query_type
        ))

        # Trim history if needed
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history // 2:]

        # Update appropriate bucket
        bucket_idx = min(
            int(predicted_confidence * self.num_buckets),
            self.num_buckets - 1
        )
        self._buckets[bucket_idx].total_predictions += 1
        self._buckets[bucket_idx].correct_predictions += actual_correctness

        # Invalidate Platt scaling
        self._platt_fitted = False

    def calibrate(self, raw_confidence: float, query_type: str = "general") -> float:
        """
        Calibrate a raw confidence score.

        Args:
            raw_confidence: Uncalibrated confidence score (0-1)
            query_type: Type of query for type-specific calibration

        Returns:
            Calibrated confidence score (0-1)
        """
        raw_confidence = max(0.0, min(1.0, raw_confidence))

        # Check for query-type specific calibrator
        if query_type in self._query_type_calibrators:
            type_calibrator = self._query_type_calibrators[query_type]
            if type_calibrator._has_sufficient_data():
                return type_calibrator.calibrate(raw_confidence, "general")

        # Apply temperature scaling first
        if self.temperature != 1.0:
            # Convert to logit, scale, convert back
            eps = 1e-7
            logit = math.log((raw_confidence + eps) / (1 - raw_confidence + eps))
            scaled_logit = logit / self.temperature
            raw_confidence = 1 / (1 + math.exp(-scaled_logit))

        # Try Platt scaling if enabled and fitted
        if self.enable_platt_scaling and self._platt_fitted:
            return self._apply_platt_scaling(raw_confidence)

        # Fall back to histogram binning
        return self._apply_histogram_binning(raw_confidence)

    def _apply_platt_scaling(self, confidence: float) -> float:
        """Apply Platt scaling (logistic regression on logits)."""
        eps = 1e-7
        logit = math.log((confidence + eps) / (1 - confidence + eps))
        scaled_logit = self._platt_a * logit + self._platt_b
        return 1 / (1 + math.exp(-scaled_logit))

    def _apply_histogram_binning(self, confidence: float) -> float:
        """Apply histogram binning calibration."""
        bucket_idx = min(
            int(confidence * self.num_buckets),
            self.num_buckets - 1
        )
        bucket = self._buckets[bucket_idx]

        # If bucket has enough samples, use its accuracy
        if bucket.total_predictions >= self.min_samples_per_bucket:
            return bucket.accuracy

        # Otherwise, interpolate with nearby buckets
        return self._interpolate_calibration(confidence)

    def _interpolate_calibration(self, confidence: float) -> float:
        """Interpolate calibration from nearby buckets."""
        # Find buckets with sufficient data
        valid_buckets = [
            (b.midpoint, b.accuracy)
            for b in self._buckets
            if b.total_predictions >= self.min_samples_per_bucket
        ]

        if not valid_buckets:
            # No calibration data, return unchanged
            return confidence

        if len(valid_buckets) == 1:
            # Only one bucket, use its accuracy
            return valid_buckets[0][1]

        # Linear interpolation between nearest valid buckets
        valid_buckets.sort(key=lambda x: x[0])

        # Find surrounding buckets
        lower_bucket = None
        upper_bucket = None

        for midpoint, accuracy in valid_buckets:
            if midpoint <= confidence:
                lower_bucket = (midpoint, accuracy)
            else:
                upper_bucket = (midpoint, accuracy)
                break

        if lower_bucket is None:
            return valid_buckets[0][1]
        if upper_bucket is None:
            return valid_buckets[-1][1]

        # Interpolate
        t = (confidence - lower_bucket[0]) / (upper_bucket[0] - lower_bucket[0])
        return lower_bucket[1] + t * (upper_bucket[1] - lower_bucket[1])

    def fit_platt_scaling(self):
        """Fit Platt scaling parameters using collected data."""
        if len(self._history) < 100:
            logger.debug("Insufficient data for Platt scaling")
            return

        # Simple gradient descent for logistic regression
        # Minimize: sum((sigmoid(a*logit + b) - y)^2)

        a, b = 1.0, 0.0
        learning_rate = 0.1

        for _ in range(100):
            grad_a, grad_b = 0.0, 0.0

            for dp in self._history[-1000:]:  # Use recent data
                eps = 1e-7
                logit = math.log((dp.predicted_confidence + eps) / (1 - dp.predicted_confidence + eps))
                pred = 1 / (1 + math.exp(-(a * logit + b)))
                error = pred - dp.actual_correctness

                grad_a += error * logit
                grad_b += error

            a -= learning_rate * grad_a / len(self._history)
            b -= learning_rate * grad_b / len(self._history)

        self._platt_a = a
        self._platt_b = b
        self._platt_fitted = True

        logger.info(f"Platt scaling fitted: a={a:.4f}, b={b:.4f}")

    def _has_sufficient_data(self) -> bool:
        """Check if calibrator has enough data."""
        total_samples = sum(b.total_predictions for b in self._buckets)
        return total_samples >= self.min_samples_per_bucket * 3

    def get_expected_calibration_error(self) -> float:
        """
        Calculate Expected Calibration Error (ECE).

        ECE = sum(|accuracy - confidence| * bucket_size) for each bucket
        Lower is better. Target: < 0.05
        """
        total_samples = sum(b.total_predictions for b in self._buckets)
        if total_samples == 0:
            return 1.0

        ece = 0.0
        for bucket in self._buckets:
            if bucket.total_predictions > 0:
                weight = bucket.total_predictions / total_samples
                ece += weight * bucket.calibration_error

        return ece

    def get_calibration_stats(self) -> Dict[str, Any]:
        """Get calibration statistics."""
        buckets_with_data = [
            {
                "range": f"{b.lower_bound:.1f}-{b.upper_bound:.1f}",
                "samples": b.total_predictions,
                "expected": b.midpoint,
                "actual": round(b.accuracy, 3),
                "error": round(b.calibration_error, 3)
            }
            for b in self._buckets
            if b.total_predictions > 0
        ]

        return {
            "total_samples": len(self._history),
            "expected_calibration_error": round(self.get_expected_calibration_error(), 4),
            "platt_fitted": self._platt_fitted,
            "platt_params": {"a": self._platt_a, "b": self._platt_b} if self._platt_fitted else None,
            "temperature": self.temperature,
            "buckets": buckets_with_data
        }


class MultiSourceTriangulator:
    """
    Verifies claims by cross-referencing multiple sources.

    Implements multi-source triangulation for fact-checking:
    1. Extract claims from the answer
    2. Search each claim across multiple sources
    3. Determine agreement/disagreement
    4. Calculate calibrated confidence

    Based on: Fact-checking research, Wikipedia verification guidelines,
    and news verification protocols.
    """

    def __init__(
        self,
        llm_client: Any = None,
        calibrator: Optional[ConfidenceCalibrator] = None,
        min_sources_for_verification: int = 2,
        agreement_threshold: float = 0.7,
        source_weights: Optional[Dict[str, float]] = None
    ):
        self.llm_client = llm_client
        self.calibrator = calibrator or ConfidenceCalibrator()
        self.min_sources = min_sources_for_verification
        self.agreement_threshold = agreement_threshold

        # Source reliability weights
        self.source_weights = source_weights or {
            "document": 1.0,  # Internal documents
            "database": 0.95,  # Structured data
            "api": 0.9,  # External APIs
            "web": 0.7,  # Web sources
            "generated": 0.5  # AI-generated content
        }

        # Claim extraction prompt
        self._claim_extraction_prompt = """Extract all factual claims from this text.
A claim is a statement that can be verified as true or false.

Text: {text}

List each claim on a separate line, prefixed with "CLAIM: ".
Only include factual claims, not opinions or questions.
Be specific and extract atomic claims (one fact per claim).

Example format:
CLAIM: Python was created in 1991
CLAIM: The capital of France is Paris
CLAIM: Machine learning requires training data"""

        # Source verification prompt
        self._verification_prompt = """Does the following source support, contradict, or have no information about the claim?

CLAIM: {claim}

SOURCE CONTENT:
{source_content}

Answer with exactly one of:
SUPPORTS - The source confirms this claim is true
CONTRADICTS - The source says this claim is false
NEUTRAL - The source has no relevant information about this claim

Also rate your confidence (0.0-1.0) in this assessment.

Format:
VERDICT: [SUPPORTS/CONTRADICTS/NEUTRAL]
CONFIDENCE: [0.0-1.0]
REASONING: [brief explanation]"""

    async def extract_claims(self, text: str) -> List[str]:
        """Extract verifiable claims from text."""
        if not self.llm_client:
            # Simple fallback: split by sentences
            sentences = text.replace('!', '.').replace('?', '.').split('.')
            return [s.strip() for s in sentences if len(s.strip()) > 20][:10]

        prompt = self._claim_extraction_prompt.format(text=text[:2000])

        try:
            response = await self.llm_client.generate(prompt)
            claims = []

            for line in response.split('\n'):
                if line.strip().startswith('CLAIM:'):
                    claim = line.replace('CLAIM:', '').strip()
                    if claim:
                        claims.append(claim)

            return claims[:15]  # Limit to 15 claims

        except Exception as e:
            logger.warning(f"Claim extraction failed: {e}")
            return []

    async def verify_claim_against_source(
        self,
        claim: str,
        source: SourceEvidence
    ) -> Tuple[str, float]:
        """
        Verify a claim against a single source.

        Returns: (verdict, confidence)
        """
        if not self.llm_client:
            # Simple keyword matching fallback
            claim_words = set(claim.lower().split())
            source_words = set(source.content.lower().split())
            overlap = len(claim_words & source_words) / len(claim_words) if claim_words else 0

            if overlap > 0.5:
                return ("SUPPORTS", 0.6)
            return ("NEUTRAL", 0.5)

        prompt = self._verification_prompt.format(
            claim=claim,
            source_content=source.content[:1500]
        )

        try:
            response = await self.llm_client.generate(prompt)

            verdict = "NEUTRAL"
            confidence = 0.5

            for line in response.split('\n'):
                line = line.strip().upper()
                if line.startswith('VERDICT:'):
                    v = line.replace('VERDICT:', '').strip()
                    if v in ('SUPPORTS', 'CONTRADICTS', 'NEUTRAL'):
                        verdict = v
                elif line.startswith('CONFIDENCE:'):
                    try:
                        confidence = float(line.replace('CONFIDENCE:', '').strip())
                        confidence = max(0.0, min(1.0, confidence))
                    except ValueError:
                        pass

            # Weight by source reliability
            source_weight = self.source_weights.get(source.source_type, 0.7)
            confidence *= source_weight

            return (verdict, confidence)

        except Exception as e:
            logger.warning(f"Source verification failed: {e}")
            return ("NEUTRAL", 0.3)

    async def verify_claim(
        self,
        claim: str,
        sources: List[SourceEvidence]
    ) -> ClaimVerification:
        """Verify a claim against multiple sources."""
        supporting = []
        contradicting = []
        neutral = []

        confidences = []

        for source in sources:
            verdict, confidence = await self.verify_claim_against_source(claim, source)

            if verdict == "SUPPORTS":
                supporting.append(source.source_id)
                confidences.append(confidence)
            elif verdict == "CONTRADICTS":
                contradicting.append(source.source_id)
                confidences.append(confidence)
            else:
                neutral.append(source.source_id)

        # Calculate agreement score
        total_opinions = len(supporting) + len(contradicting)
        if total_opinions == 0:
            agreement_score = 0.0
            raw_confidence = 0.3
            verdict = "unsupported"
        else:
            # Agreement is high when sources mostly agree
            if len(supporting) > len(contradicting):
                agreement_score = len(supporting) / total_opinions
                raw_confidence = sum(confidences) / len(confidences) if confidences else 0.5
                verdict = "verified" if agreement_score >= self.agreement_threshold else "uncertain"
            else:
                agreement_score = len(contradicting) / total_opinions
                raw_confidence = sum(confidences) / len(confidences) if confidences else 0.5
                verdict = "contradicted" if agreement_score >= self.agreement_threshold else "uncertain"

        # Apply confidence calibration
        calibrated_confidence = self.calibrator.calibrate(raw_confidence)

        return ClaimVerification(
            claim=claim,
            supporting_sources=supporting,
            contradicting_sources=contradicting,
            neutral_sources=neutral,
            agreement_score=agreement_score,
            confidence=calibrated_confidence,
            verdict=verdict
        )

    async def triangulate(
        self,
        answer: str,
        sources: List[SourceEvidence],
        query_type: str = "general"
    ) -> TriangulationResult:
        """
        Perform full multi-source triangulation on an answer.

        Args:
            answer: The answer to verify
            sources: List of sources to cross-reference
            query_type: Type of query for calibration

        Returns:
            TriangulationResult with verification details
        """
        # Step 1: Extract claims
        claims = await self.extract_claims(answer)

        if not claims:
            return TriangulationResult(
                answer=answer,
                claims=[],
                overall_confidence=0.5,
                source_agreement=0.0,
                verification_coverage=0.0,
                recommended_action="flag_for_review",
                calibration_applied=False,
                raw_confidence=0.5,
                calibration_adjustment=0.0
            )

        # Step 2: Verify each claim
        verifications = []
        for claim in claims:
            verification = await self.verify_claim(claim, sources)
            verifications.append(verification)

        # Step 3: Calculate aggregate metrics
        verified_count = sum(1 for v in verifications if v.verdict == "verified")
        contradicted_count = sum(1 for v in verifications if v.verdict == "contradicted")

        verification_coverage = verified_count / len(verifications)

        # Overall agreement
        all_agreements = [v.agreement_score for v in verifications if v.agreement_score > 0]
        source_agreement = sum(all_agreements) / len(all_agreements) if all_agreements else 0.0

        # Raw confidence (weighted by claim verdicts)
        raw_confidence = 0.5
        if verifications:
            confidence_sum = 0.0
            weight_sum = 0.0

            for v in verifications:
                if v.verdict == "verified":
                    confidence_sum += v.confidence * 1.0
                    weight_sum += 1.0
                elif v.verdict == "contradicted":
                    confidence_sum += (1 - v.confidence) * 1.0
                    weight_sum += 1.0
                elif v.verdict == "uncertain":
                    confidence_sum += 0.5 * 0.5
                    weight_sum += 0.5

            if weight_sum > 0:
                raw_confidence = confidence_sum / weight_sum

        # Apply calibration
        calibrated_confidence = self.calibrator.calibrate(raw_confidence, query_type)
        calibration_adjustment = calibrated_confidence - raw_confidence

        # Determine recommended action
        if contradicted_count > verified_count:
            recommended_action = "reject"
        elif calibrated_confidence >= 0.8 and verification_coverage >= 0.7:
            recommended_action = "accept"
        elif calibrated_confidence < 0.4 or verification_coverage < 0.3:
            recommended_action = "reject"
        else:
            recommended_action = "flag_for_review"

        return TriangulationResult(
            answer=answer,
            claims=verifications,
            overall_confidence=calibrated_confidence,
            source_agreement=source_agreement,
            verification_coverage=verification_coverage,
            recommended_action=recommended_action,
            calibration_applied=True,
            raw_confidence=raw_confidence,
            calibration_adjustment=calibration_adjustment
        )

    def record_verification_outcome(
        self,
        predicted_confidence: float,
        actual_correctness: float,
        query_type: str = "general"
    ):
        """Record outcome for calibration improvement."""
        self.calibrator.record_outcome(
            predicted_confidence=predicted_confidence,
            actual_correctness=actual_correctness,
            query_type=query_type
        )


class LRMStyleVerifier:
    """
    LRM-Style (Language Model Reasoning) Verification System.

    Combines multi-source triangulation with confidence calibration
    for cutting-edge fact verification.

    Features:
    - Multi-source triangulation
    - Confidence calibration (Platt scaling + histogram binning)
    - Query-type specific calibration
    - Source reliability weighting
    - Verification coverage tracking
    """

    def __init__(
        self,
        llm_client: Any = None,
        num_calibration_buckets: int = 10,
        source_weights: Optional[Dict[str, float]] = None
    ):
        self.llm_client = llm_client

        # Initialize calibrator
        self.calibrator = ConfidenceCalibrator(
            num_buckets=num_calibration_buckets,
            enable_platt_scaling=True
        )

        # Initialize triangulator
        self.triangulator = MultiSourceTriangulator(
            llm_client=llm_client,
            calibrator=self.calibrator,
            source_weights=source_weights
        )

        # Verification history for metrics
        self._verification_history: List[Dict[str, Any]] = []
        self._max_history = 1000

    async def verify_answer(
        self,
        question: str,
        answer: str,
        sources: List[SourceEvidence],
        query_type: str = "general"
    ) -> TriangulationResult:
        """
        Verify an answer using multi-source triangulation.

        Args:
            question: The original question
            answer: The generated answer to verify
            sources: List of sources for cross-referencing
            query_type: Type of query for calibration

        Returns:
            TriangulationResult with verification details
        """
        result = await self.triangulator.triangulate(
            answer=answer,
            sources=sources,
            query_type=query_type
        )

        # Record in history
        self._verification_history.append({
            "timestamp": datetime.utcnow().isoformat(),
            "query_type": query_type,
            "num_claims": len(result.claims),
            "overall_confidence": result.overall_confidence,
            "source_agreement": result.source_agreement,
            "verification_coverage": result.verification_coverage,
            "recommended_action": result.recommended_action,
            "calibration_adjustment": result.calibration_adjustment
        })

        if len(self._verification_history) > self._max_history:
            self._verification_history = self._verification_history[-self._max_history // 2:]

        return result

    async def quick_verify(
        self,
        answer: str,
        context_chunks: List[str],
        query_type: str = "general"
    ) -> Dict[str, Any]:
        """
        Quick verification using context as sources.

        Args:
            answer: The answer to verify
            context_chunks: RAG context chunks as sources
            query_type: Type of query

        Returns:
            Simplified verification result
        """
        # Convert context to sources
        sources = [
            SourceEvidence(
                source_id=f"context_{i}",
                source_type="document",
                content=chunk,
                confidence=0.9
            )
            for i, chunk in enumerate(context_chunks)
        ]

        result = await self.triangulator.triangulate(answer, sources, query_type)

        return {
            "confidence": result.overall_confidence,
            "source_agreement": result.source_agreement,
            "verification_coverage": result.verification_coverage,
            "action": result.recommended_action,
            "verified_claims": sum(1 for c in result.claims if c.verdict == "verified"),
            "total_claims": len(result.claims),
            "calibrated": result.calibration_applied
        }

    def record_feedback(
        self,
        predicted_confidence: float,
        was_correct: bool,
        query_type: str = "general"
    ):
        """
        Record user feedback for calibration improvement.

        Args:
            predicted_confidence: The confidence score that was shown
            was_correct: Whether the answer was actually correct
            query_type: Type of query
        """
        self.calibrator.record_outcome(
            predicted_confidence=predicted_confidence,
            actual_correctness=1.0 if was_correct else 0.0,
            query_type=query_type
        )

        # Refit Platt scaling periodically
        if len(self.calibrator._history) % 100 == 0:
            self.calibrator.fit_platt_scaling()

    def get_calibration_report(self) -> Dict[str, Any]:
        """Get calibration quality report."""
        return {
            "calibrator_stats": self.calibrator.get_calibration_stats(),
            "verification_history_count": len(self._verification_history),
            "recent_accuracy": self._calculate_recent_accuracy()
        }

    def _calculate_recent_accuracy(self) -> Optional[float]:
        """Calculate accuracy from recent verifications."""
        recent = self._verification_history[-100:]
        if not recent:
            return None

        # Use recommended_action as proxy for correctness
        accept_count = sum(1 for v in recent if v["recommended_action"] == "accept")
        return accept_count / len(recent)

    def get_verification_stats(self) -> Dict[str, Any]:
        """Get verification statistics."""
        if not self._verification_history:
            return {"message": "No verifications performed yet"}

        recent = self._verification_history[-100:]

        # Action distribution
        actions = {}
        for v in recent:
            action = v["recommended_action"]
            actions[action] = actions.get(action, 0) + 1

        # Average metrics
        avg_confidence = sum(v["overall_confidence"] for v in recent) / len(recent)
        avg_agreement = sum(v["source_agreement"] for v in recent) / len(recent)
        avg_coverage = sum(v["verification_coverage"] for v in recent) / len(recent)
        avg_calibration_adj = sum(abs(v["calibration_adjustment"]) for v in recent) / len(recent)

        return {
            "total_verifications": len(self._verification_history),
            "recent_count": len(recent),
            "action_distribution": actions,
            "avg_confidence": round(avg_confidence, 3),
            "avg_source_agreement": round(avg_agreement, 3),
            "avg_verification_coverage": round(avg_coverage, 3),
            "avg_calibration_adjustment": round(avg_calibration_adj, 4),
            "expected_calibration_error": round(
                self.calibrator.get_expected_calibration_error(), 4
            )
        }


# Factory functions for cutting-edge verification
_lrm_verifier: Optional[LRMStyleVerifier] = None
_confidence_calibrator: Optional[ConfidenceCalibrator] = None


def get_lrm_verifier(llm_client: Any = None) -> LRMStyleVerifier:
    """Get or create LRM-Style Verifier instance."""
    global _lrm_verifier
    if _lrm_verifier is None:
        _lrm_verifier = LRMStyleVerifier(llm_client=llm_client)
        logger.info("LRM-Style Verifier initialized (Cutting Edge)")
    return _lrm_verifier


def get_confidence_calibrator() -> ConfidenceCalibrator:
    """Get or create Confidence Calibrator instance."""
    global _confidence_calibrator
    if _confidence_calibrator is None:
        _confidence_calibrator = ConfidenceCalibrator()
        logger.info("Confidence Calibrator initialized")
    return _confidence_calibrator


async def verify_with_triangulation(
    answer: str,
    context_chunks: List[str],
    llm_client: Any = None,
    query_type: str = "general"
) -> Dict[str, Any]:
    """
    Convenience function for quick answer verification.

    Args:
        answer: The answer to verify
        context_chunks: RAG context chunks
        llm_client: Optional LLM client
        query_type: Type of query

    Returns:
        Verification result dict
    """
    verifier = get_lrm_verifier(llm_client)
    return await verifier.quick_verify(answer, context_chunks, query_type)
