# Coursera coursework

Completed practice labs and supporting work from two DeepLearning.AI /
Coursera tracks: the **Machine Learning Specialization** and IBM's data
engineering material.

## Machine Learning Specialization

### Course 2 — Advanced Learning Algorithms

| Notebook | Topic |
|---|---|
| `C2_W1_Assignment.ipynb` | Neural networks for handwritten digit recognition (binary) |
| `C2_W2_Assignment.ipynb` | Neural networks for handwritten digit recognition (multiclass) |
| `C2_W3_Assignment.ipynb` | Advice for applying machine learning — bias, variance, diagnostics |
| `C2_W4_Decision_Tree_with_Markdown.ipynb` | Decision trees |

### Course 3 — Unsupervised Learning, Recommenders, Reinforcement Learning

| Notebook | Topic |
|---|---|
| `C3_W1_Anomaly_Detection.ipynb` | Gaussian anomaly detection |
| `C3_W2_Collaborative_RecSys_Assignment.ipynb` | Collaborative filtering recommender |
| `C3_W2_RecSysNN_Assignment.ipynb` | Deep learning for content-based filtering |

## SQL

`SQL/` holds analysis of three Chicago public datasets — census, crime,
and public schools — against a SQLite database (`FinalDB.db`), written up
in `Chicago_stats.ipynb`.

It also contains three Airflow pieces:

- `production_dag.py` — a production-shaped DAG definition
- `sales_analytics_dag.py` — scheduled sales pipeline with SLAs and retries
- `retry_strategies.py` — exponential backoff with jitter, and notes on why
  naive fixed-interval retries cause thundering-herd problems

## Airflow

`airflow/` documents a DAG build end to end in screenshots — definition,
arguments, task wiring, submission, unpausing, and the resulting runs.

## Related

The Airflow work here grew into a standalone project:
[e-commerce-data-pipeline](https://github.com/leandrovelazquez/e-commerce-data-pipeline).
